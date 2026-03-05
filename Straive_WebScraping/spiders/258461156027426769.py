import re
import scrapy
import pandas as pd
from ..utils import save_df


class NtcmnSpider(scrapy.Spider):
    name = "ntcmn"

    # Unique Institution ID
    institution_id = 258461156027426769

    # URLs
    course_url = "https://www.ntcmn.edu/academics/schedule/"
    directory_url = "https://www.ntcmn.edu/directory/facstaff/"
    calendar_url = ""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage lists
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # START REQUESTS - CONTROL SCRAPE MODE
    def start_requests(self):
        """
        SCRAPE_MODE options:
        course, directory, calendar, course_directory, all (default)
        """

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        else:
            # Default: scrape everything
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE SCRAPER
    def parse_course(self, response):
        """
        Collects available term values and submits form requests.
        """

        values = response.xpath('//select[@id="yrtr"]/option/@value').getall()

        for val in values:
            payload = {
                "query[yrtr]": val,
                "query[subj]": "",
                "query[location_id]": "",
                "adv_query[begin_time]": "",
                "adv_query[end_time]": "",
                "adv_query[instructor]": "",
                "adv_query[course_name]": "",
                "adv_query[media_code]": "",
                "adv_query[sess_type]": "",
                "class-schedule-submit": "",
            }

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": self.course_url,
            }

            yield scrapy.FormRequest(
                url=self.course_url,
                method="POST",
                headers=headers,
                formdata=payload,
                callback=self.parse_details,
            )

    def parse_details(self, response):
        """
        Parses course schedule table rows.
        """

        rows = response.xpath(
            '//table[contains(@class,"class-schedule")]//tr[count(td)=13 and td[1][normalize-space()]]'
            ' | //table[contains(@class,"class-schedule")]//tr[count(td)=13 and td[1][normalize-space()]]/following-sibling::tr[td[@colspan="6"]]'
        )

        previous_course = {}

        for row in rows:
            class_num = row.xpath("./td[1]/text()").get("").strip()
            dept = row.xpath("./td[2]/text()").get("").strip()
            course_no = row.xpath("./td[3]/text()").get("").strip()
            section = row.xpath("./td[4]/text()").get("").strip()
            raw_title = row.xpath("./td[5]//a/text()").get("").strip()

            textbook_url = row.xpath('./td/span[@class="text-nowrap"]/a/@href').get("")

            dates = row.xpath("./td[9]/text()").get("").strip()
            main_location = row.xpath("./td[10]/text()").get("").strip()
            instructor = row.xpath("./td[11]/text()").get("").strip()

            raw_enrollment = row.xpath("./td[13]//text()").get("").strip()
            enrollment = ""

            if raw_enrollment and "/" in raw_enrollment:
                try:
                    cur, max_ = raw_enrollment.split("/")
                    if int(cur) < int(max_):
                        enrollment = raw_enrollment
                except:
                    pass

            # Parent row check
            is_parent = bool(class_num)
            course_description = ""

            if is_parent:
                desc_nodes = row.xpath(
                    f"following-sibling::tr[@id='course-{class_num}']/td//text()"
                ).getall()
                course_description = " ".join(d.strip() for d in desc_nodes if d.strip())

            if not is_parent and previous_course:
                class_num = previous_course["Class Number"]
                dept = previous_course["Dept"]
                course_no = previous_course["Course No"]
                section = previous_course["Section"]
                course_description = previous_course["Course Description"]
                textbook_url = previous_course["Textbook URL"]

            # Course title handling
            if is_parent and raw_title:
                base_title = raw_title
            else:
                base_title = previous_course.get("Base Title", "")

            if is_parent:
                previous_course = {
                    "Class Number": class_num,
                    "Dept": dept,
                    "Course No": course_no,
                    "Section": section,
                    "Base Title": base_title,
                    "Course Description": course_description,
                    "Textbook URL": textbook_url,
                }

            course_name = f"{dept} {course_no} - {base_title}".strip(" -")

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": course_name,
                "Course Description": re.sub(r"\s+", " ", course_description),
                "Class Number": class_num,
                "Section": section,
                "Instructor": instructor,
                "Enrollment": enrollment,
                "Course Dates": dates,
                "Location": main_location,
                "Textbook/Course Materials": textbook_url,
            })

        # Save course data
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Scrapes faculty/staff directory.
        """

        rows = response.xpath("//div[contains(@class,'bsu-directory-listing')]")

        for emp in rows:
            url = emp.xpath(".//h3[@itemprop='name']//a/@href").get("")
            name = emp.xpath(".//h3[@itemprop='name']//a/text()").get("")
            title = emp.xpath(".//h3[@itemprop='name']//small/text()").get("")
            phone = emp.xpath(".//a[starts-with(@href,'tel:')]/text()").get("")

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": url,
                "Name": name.strip(),
                "Title": title.strip(),
                "Email": "",
                "Phone Number": phone.strip(),
            })

        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    # CALENDAR SCRAPER (NO DATA FOUND)
    def parse_calendar(self, response):
        """
        Calendar data not available on website.
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
