import re
import scrapy
import pandas as pd
from ..utils import save_df
from inline_requests import inline_requests


class MtcSpider(scrapy.Spider):
    name = "mtc"

    # Unique institution ID
    institution_id = 258452477655934932

    # URLs
    course_url = "https://www.mtc.edu/my-mtc/class-schedules.html"
    directory_url = "https://www.mtc.edu/directory/index.html"
    calendar_url = "http://www.mtc.edu/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Store scraped data
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT - CONTROL SCRAPE MODE
    def start_requests(self):
        """
        SCRAPE_MODE can be:
        course, directory, calendar, course_directory, all (default)
        """

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        # Run only course scraping
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        # Run only directory scraping
        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        # Run only calendar scraping
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Run course + directory
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        # Default → run all
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE SCRAPER
    @inline_requests
    def parse_course(self, response):
        """
        Extract course schedule pages, paginate, and scrape section details
        """

        # Get all schedule links
        schedule_urls = response.xpath('//a[contains(@title,"Schedule")]/@href').getall()

        # Skip first link (duplicate / irrelevant)
        for schedule_url in schedule_urls[1:]:
            schedule_response = yield scrapy.Request(schedule_url)

            # Increase results per page
            base_url = schedule_response.url.replace("num=10", "num=100")
            per_page = 100

            # Extract total record count
            header_text = schedule_response.xpath('//div[@id="sectionSearchHeader"]/h2/text()').get("")
            total_records = int(re.search(r'of (\d+)', header_text).group(1))

            # Pagination loop
            for start in range(0, total_records, per_page):
                paginated_url = f"{base_url}&start={start}"
                page_response = yield scrapy.Request(paginated_url)

                # Get section detail links
                section_links = page_response.xpath(
                    '//a[contains(@href,"sectiondetailsdialog.aspx")]/@href'
                ).getall()

                # Loop each section page
                for link in section_links:
                    detail_response = yield scrapy.Request(link)

                    # ---------- COURSE HEADER ----------
                    header = detail_response.xpath('//span[@class="leveloneheader"]/text()').get("").strip()
                    header = re.sub(r"\s+", " ", header)

                    if " - " in header:
                        code_part, class_name = header.split(" - ", 1)
                    else:
                        code_part, class_name = header, ""

                    code_parts = code_part.split("/")
                    class_code = code_parts[0]
                    section = code_parts[-1] if len(code_parts) > 1 else ""

                    full_course_name = f"{class_code} - {class_name}"

                    # ---------- DESCRIPTION ----------
                    desc = detail_response.xpath('//span[@class="leveltwoheader"]/following-sibling::text()').getall()
                    desc = " ".join(desc)
                    desc = re.sub(r'^\s*\|\s*\d+(\.\d+)?\s*', '', desc)
                    desc = re.sub(r"\s+", " ", desc).strip()

                    # ---------- INSTRUCTOR ----------
                    instructor = detail_response.xpath(
                        '//span[@id="ctl00_mainContent_ucSectionDetail_lblInstructors"]/../../td[2]//text()'
                    ).getall()
                    instructor = " | ".join(i.strip() for i in instructor if i.strip())
                    instructor = re.sub(r"\s+", " ", instructor)

                    # ---------- COURSE DATES ----------
                    start_date = detail_response.xpath(
                        '//span[@id="ctl00_mainContent_ucSectionDetail_lblDuration"]/../../td[2]/text()'
                    ).get("").strip()

                    # ---------- LOCATION CLEANING ----------
                    raw_locations = detail_response.xpath(
                        '//span[@id="ctl00_mainContent_ucSectionDetail_lblSchedule"]/../../td[2]//text()'
                    ).getall()

                    raw_locations = [l.strip() for l in raw_locations if l.strip()]
                    clean_locations = []

                    for loc in raw_locations:
                        loc = re.sub(r'^(?:Online|Arranged|Mon|Tue|Wed|Thu|Fri|Sat|Sun)[^;]*;\s*', '', loc, flags=re.I)
                        loc = re.sub(r"\s+", " ", loc).strip()
                        loc = re.sub(r",\s*,", ", ", loc)

                        if loc and len(loc) > 5:
                            clean_locations.append(loc)

                    final_location = " | ".join(dict.fromkeys(clean_locations))

                    # ---------- SEATS ----------
                    seats_text = detail_response.xpath(
                        '//span[@id="ctl00_mainContent_ucSectionDetail_lblClass"]/../../td[2]/text()'
                    ).getall()

                    seats_text = " ".join(s.strip() for s in seats_text if s.strip())
                    nums = re.findall(r"\d+", seats_text)
                    seats_clean = f"{nums[1]}/{nums[0]}" if len(nums) >= 2 else ""

                    # ---------- STORE DATA ----------
                    self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": detail_response.url,
                        "Course Name": full_course_name,
                        "Course Description": desc,
                        "Class Number": class_code,
                        "Section": section,
                        "Instructor": instructor,
                        "Enrollment": seats_clean,
                        "Course Dates": start_date,
                        "Location": final_location,
                        "Textbook/Course Materials": "",
                    })

        # Save course CSV
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    @inline_requests
    def parse_directory(self, response):
        """
        Scrape faculty/staff profiles
        """

        employees = response.css("div.col-xl-12.panel-module.employee")

        for emp in employees:
            profile_href = emp.css("div.name a::attr(href)").get("")
            profile_url = f"https://www.mtc.edu{profile_href}"

            profile_response = yield scrapy.Request(profile_url, dont_filter=True)

            name = profile_response.xpath('//div[@class="program-director"]/a/text()').get("").strip()
            title = profile_response.xpath('//div[@class="program-name"]/text()').get("").strip()
            dept = profile_response.xpath('//div[@class="program-school"]/text()').get("").strip()

            email = profile_response.xpath('//div[@class="program-email"]/a/text()').get("").strip()
            phone = profile_response.xpath('//div[@class="program-phone"]/a/text()').get("").strip()

            full_title = f"{title}, {dept}" if dept else title

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": profile_response.url,
                "Name": re.sub(r"\s+", " ", name),
                "Title": re.sub(r"\s+", " ", full_title),
                "Email": email,
                "Phone Number": phone,
            })

        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    # CALENDAR SCRAPER (NO DATA FOUND)
    def parse_calendar(self, response):
        """
        Placeholder calendar scraper (no data available)
        """

        self.calendar_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": "Data Not Found",
            "Term Name": "Data Not Found",
            "Term Date": "Data Not Found",
            "Term Date Description": "Data Not Found",
        })

        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
