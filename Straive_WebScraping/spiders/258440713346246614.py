import re
import scrapy
import PyPDF2
import pdfplumber
import pandas as pd
from io import BytesIO
from ..utils import save_df
from inline_requests import inline_requests


class LasierraSpider(scrapy.Spider):
    name = "lasierra"

    institution_id = 258440713346246614

    course_url = "https://banner.lasierra.edu/pls/lsu/lsu_web.course_list"
    directory_url = "https://lasierra.edu/offices/records/directory/"
    calendar_url = "https://lasierra.edu/fileadmin/documents/records/academic-calendar.pdf"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield from self.course_request()

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["course_directory", "directory_course"]:
            yield from self.course_request()
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            yield from self.course_request()
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:  # default all
            yield from self.course_request()
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE REQUEST
    def course_request(self):
        payload = {
            "TERM_CODE": "202603",
            "INSTRUCTOR": "- ANY -",
            "UNITS": "",
            "BTIME": "ALL TIMES",
            "LOCATIONS": "- ANY -",
            "CAMPUSES": "6",
            "CRN": "",
            "SUBJ": "- ANY -",
            "CRSE": "",
            "SCH_DEPT": "- ANY -",
            "GE_CODE": "- ANY -",
            "CLASSES": "OPEN"
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://banner.lasierra.edu/pls/lsu/lsu_web.courses",
            "User-Agent": "Mozilla/5.0"
        }

        yield scrapy.FormRequest(
            url=self.course_url,
            formdata=payload,
            headers=headers,
            callback=self.parse_course,
            dont_filter=True
        )

    # CLEAN TEXT
    def clean(self, text):
        return re.sub(r"\s+", " ", text).strip() if text else ""

    # COURSE PARSER
    @inline_requests
    def parse_course(self, response):
        rows = response.xpath('//tr[td/a[contains(@href,"course_details")]]')

        for row in rows:
            enrol = row.xpath("./td[9]//font/text()").get(default="")
            enrol_max = row.xpath("./td[10]//font/text()").get(default="")
            enrollment = f"{enrol}/{enrol_max}"

            location1 = row.xpath("./td[20]//font/text()").get(default="").strip()
            location2 = row.xpath("./td[21]//font/text()").get(default="").strip()
            location = location2 if not location1 or location1.upper() == "N/A" else f"{location1}, {location2}"

            instructor = row.xpath("./td[22]//font/text()").get(default="")

            link = row.xpath('.//a[contains(@href,"course_details")]/@href').get("")
            link = f"https://banner.lasierra.edu{link}"

            field_response = yield scrapy.Request(link)

            course_title = field_response.xpath(
                '//th[contains(.,"Title")]/following-sibling::td[1]//font/text()'
            ).get(default="").strip()

            info_row = field_response.xpath('//th[normalize-space()="CRN"]/ancestor::tr/following-sibling::tr[1]')

            subject = info_row.xpath("./td[2]//font/text()").get(default="").strip()
            course_number = info_row.xpath("./td[3]//font/text()").get(default="").strip()
            section = info_row.xpath("./td[4]//font/text()").get(default="").strip()
            class_number = info_row.xpath("./td[1]//font/text()").get(default="").strip()
            course_dates = info_row.xpath("./td[8]//font/text()").get(default="").strip()

            desc_list = field_response.xpath(
                '//th[contains(.,"Class Description")]/following::tr/td[@colspan="5"]//font/text()'
            ).getall()

            description = " ".join(d.strip() for d in desc_list if d.strip())
            description = description.replace("No Current Description Available.", "")

            course_name = f"{course_number}-{subject} - {course_title}"

            textbook_url = (
                f"https://www.bkstr.com/lasierraunivstore/follett-discover-view/booklook"
                f"?shopBy=discoverViewCourse&bookstoreId=2217&termId=202603"
                f"&departmentDisplayName={subject}&courseDisplayName={course_number}&sectionDisplayName=1"
            )

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": field_response.url,
                "Course Name": course_name,
                "Course Description": self.clean(description),
                "Class Number": class_number,
                "Section": section,
                "Instructor": instructor,
                "Enrollment": enrollment,
                "Course Dates": course_dates,
                "Location": location,
                "Textbook/Course Materials": textbook_url
            })

        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY PARSER
    def parse_directory(self, response):
        for card in response.xpath("//div[contains(@class,'staffcardOuter')]"):
            name = card.xpath(".//h4/text()").get(default="").strip()
            title = card.xpath(".//div[contains(@class,'staffcardTitle')]/text()").get(default="").strip()
            department = card.xpath(".//div[contains(@class,'staffcardDepartment')]//div/text()").get(default="").strip()

            phone = card.xpath("normalize-space(.//div[contains(@class,'staffcardPhoneContainer')])").get("")
            email = card.xpath(".//div[contains(@class,'staffcardEmailContainer')]//a/@href").get(default="")
            email = email.replace("mailto:", "").strip()

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": f"{title}, {department}".strip(", "),
                "Email": email,
                "Phone Number": phone,
            })

        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # CALENDAR PARSER
    def parse_calendar(self, response):
        """
        Extract academic calendar term dates from PDF using pdfplumber
        """
        with pdfplumber.open(BytesIO(response.body)) as pdf:

            current_term = "General"

            for page in pdf.pages:
                lines = page.extract_text().split("\n")

                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    # Detect term headers like "Fall Quarter 2025"
                    if "Quarter" in line and "Registration" not in line and ".... " not in line:
                        # Academic Calendar thavira ulla thalaippai edukkirom
                        clean_name = line.replace("Academic Calendar", "").strip()
                        if clean_name:
                            current_term = clean_name
                        continue

                    # Extract event lines: Description .... Date
                    if "...." in line:
                        parts = re.split(r"\.{2,}", line)
                        if len(parts) >= 2:
                            description = parts[0].strip()
                            date_val = parts[-1].strip()

                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Term Name": current_term,
                                "Term Date": date_val,
                                "Term Date Description": re.sub(r"\s+", " ", description)
                            })

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")
