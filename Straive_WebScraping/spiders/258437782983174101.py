import scrapy
import json
import re
import pandas as pd
from ..utils import save_df


class NoccdSpider(scrapy.Spider):
    name = "noccd"

    # Unique institution ID used for all datasets
    institution_id = 258437782983174101

    # URLs
    course_url = "https://schedule.nocccd.edu/?college=1&term=202520"
    directory_url = "https://www.cypresscollege.edu/financial-aid/staff-contact-information/"
    calendar_url = "https://www.nocccd.edu/events-calendars/academic-calendar"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.directory_rows = []
        self.calendar_rows = []
        self.course_rows = []

        # Course mapping dictionary (subject + number → title & description)
        self.course_map = {}

    # ENTRY POINT – BASED ON SCRAPE MODE
    def start_requests(self):
        """
        Dynamically controls scraping mode:
        course / directory / calendar / combinations / all
        """

        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        if mode == 'course':
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == 'directory':
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == 'calendar':
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default → Scrape everything
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE SCRAPER
    def parse_course(self, response):
        """
        Step 1:
        Fetch master course list (contains title + description).
        """
        courses_api = "https://schedule.nocccd.edu/data/202530/courses.json?p=2026125"
        yield scrapy.Request(courses_api, callback=self.parse_course_list)

    def parse_course_list(self, response):
        """
        Step 2:
        Build course_map dictionary:
        key = SUBJECT_NUMBER → {title, description}
        """
        data = json.loads(response.text)

        for course in data:
            subj = course.get("crseSubjCode")
            num = course.get("crseCrseNumb")

            # Ensure both subject & number exist
            if subj and num:
                key = f"{subj}_{num}"
                self.course_map[key] = {
                    "title": course.get("crseTitle"),
                    "desc": course.get("crseText")
                }

        # After building course_map → Fetch section details
        sections_api = "https://schedule.nocccd.edu/data/202530/sections.json?p=20261251747"
        yield scrapy.Request(sections_api, callback=self.parse_course_details)

    def parse_course_details(self, response):
        """
        Step 3:
        Merge section data with course_map
        """
        data = json.loads(response.text)

        for section in data:

            # Match section with course_map
            subj = section.get("sectSubjCode")
            num = section.get("sectCrseNumb")
            key = f"{subj}_{num}"

            course_info = self.course_map.get(key)

            if not course_info:
                self.logger.warning(f"Course mapping not found for {key}")
                continue

            course_title = course_info.get("title")
            course_desc = course_info.get("desc")

            # Format Course Name
            course_name = f"{subj} {num}: {course_title}"

            # Enrollment Logic
            seats_avail = max(int(section.get("sectSeatsAvail") or 0), 0)
            max_enrl = int(section.get("sectMaxEnrl") or 0)

            enrollment = (
                f"{seats_avail}/{max_enrl} seats remain"
                if max_enrl > 0 else None
            )

            # Meeting Details
            instructor = None
            location = None
            date = None

            meetings = section.get("sectMeetings", [])

            if meetings:
                # Instructor → First available instructor
                for meet in meetings:
                    if meet.get("meetInstrName"):
                        instructor = self.clean_text(meet.get("meetInstrName"))
                        break

                # Location & Date → From first meeting
                first_meet = meetings[0]

                bldg = first_meet.get("bldgDesc") or ""
                room = first_meet.get("roomCode") or ""

                raw_location = f"{bldg} {room}".strip()
                location = self.clean_text(raw_location)

                start = first_meet.get("startDate")
                end = first_meet.get("endDate")

                if start and end:
                    date = f"{start} - {end}"

            # Append structured row
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_url,
                "Course Name": self.clean_text(course_name),
                "Course Description": self.clean_text(course_desc),
                "Class Number": section.get("sectCrn"),
                "Section": "",
                "Instructor": instructor,
                "Enrollment": enrollment,
                "Course Dates": date,
                "Location": location,
                "Textbook/Course Materials": "https://www.bkstr.com/cypressstore/home"
            })

        # Save Course Data
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Extract Staff Contact Information
        """
        staff_blocks = response.xpath(
            "//h2[contains(text(),'Staff')]/following-sibling::div//p[strong]"
        )

        for block in staff_blocks:

            name = " ".join(
                block.xpath(".//strong//text()").getall()
            ).strip()

            title = block.xpath(".//br/following-sibling::text()[1]").get()
            email = block.xpath(".//a[contains(@href,'mailto')]/text()").get()

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title.strip() if title else None,
                "Email": email.strip() if email else None,
                "Phone Number": None
            })

        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # CALENDAR SCRAPER
    def parse_calendar(self, response):
        """
        Extract Academic Calendar data by term.
        """
        headers = response.xpath("//h2[contains(text(), 'Academic Calendar')]")

        for header in headers:

            term_name = header.xpath("string(.)").get().strip()

            rows = header.xpath("./following-sibling::table[1]//tbody/tr")

            for row in rows:
                date = row.xpath("./td[1]//text()").get()
                date = date.strip() if date else None

                # Collect all description cells
                desc_cells = row.xpath("./td[position()>1]")

                descriptions = []
                for cell in desc_cells:
                    text = "".join(cell.xpath(".//text()").getall()).strip()
                    if text:
                        descriptions.append(text)

                descriptions = list(dict.fromkeys(descriptions))

                if not date or not descriptions:
                    continue

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": date,
                    "Term Date Description": ", ".join(descriptions)
                })

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")

    # COMMON CLEAN FUNCTION
    def clean_text(self, value):
        """Normalize whitespace and strip text."""
        return re.sub(r"\s+", " ", str(value)).strip() if value else None