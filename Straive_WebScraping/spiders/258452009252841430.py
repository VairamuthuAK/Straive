import re
import scrapy
import pandas as pd
import codecs
from urllib.parse import quote
from ..utils import save_df


class HcflSpider(scrapy.Spider):
    """
    Spider Name  : hcfl
    Institution  : Hillsborough Community College
    Description  : Scrapes Course, Directory, and Academic Calendar data
    """

    name = "hcfl"

    # Unique Institution ID
    institution_id = 258452009252841430

    # Base URLs
    course_url = "https://classes.hccfl.edu/"
    directory_url = "https://www.hcfl.edu/directory"
    calendar_url = "https://www.hcfl.edu/academics/academic-calendars"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT
    def start_requests(self):
        """
        Controls scrape mode based on SCRAPE_MODE setting.
        Supported Modes:
            - course
            - directory
            - calendar
            - combinations (course_directory, etc.)
            - all (default)
        """

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        else:  # Default: scrape everything
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE SCRAPER
    def parse_course(self, response):
        """
        Calls course API endpoint to fetch JSON course data.
        """

        api_url = "https://classes.hccfl.edu/api/courseSection?term=25/FA"

        yield scrapy.Request(api_url, callback=self.parse_classes)

    def parse_classes(self, response):
        """
        Parses JSON response from course API
        Loops through each course section and extracts required fields
        """

        data = response.json()

        for row in data:

            subject = row.get("subject")
            course_number = row.get("course")
            course_name = row.get("courseName")
            section = row.get("section")
            term = row.get("term")

            # Construct Course Name
            full_course_name = f"{subject} {course_number} {course_name}"
            class_number = f"{subject} {course_number}"

            # Instructor Handling (supports 2 instructors)
            first = row.get("firstName", "").strip()
            last = row.get("lastName", "").strip()
            first2 = row.get("firstName2", "").strip()
            last2 = row.get("lastName2", "").strip()

            instructor = f"{first} {last}".strip()
            if first2 or last2:
                instructor2 = f"{first2} {last2}".strip()
                instructor = f"{instructor}, {instructor2}"

            # Extract Date Range from session field
            session = row.get("session", "")
            match = re.search(r"\((.*?)\)", session)
            date_range = match.group(1) if match else ""

            # Seat Availability Logic
            seat_raw = row.get("availability")
            seat = max(int(seat_raw), 0) if seat_raw else 0
            seat = f"{seat} seats available"

            location = row.get("locationName")

            # Generate Bookstore URL
            campus = "BR"
            course_data = f"{campus}_{subject}_{course_number}_{section}_{term}"
            encoded_data = quote(course_data)

            bookstore_url = (
                "https://hccfl.bncollege.com/course-material-listing-page"
                f"?utm_campaign=storeId=90206_langId=-1_courseData={encoded_data}"
                "&utm_source=wcs&utm_medium=registration_integration"
            )

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_url,
                "Course Name": full_course_name,
                "Course Description": "",
                "Class Number": class_number,
                "Section": section,
                "Instructor": instructor,
                "Enrollment": seat,
                "Course Dates": date_range,
                "Location": location,
                "Textbook/Course Materials": bookstore_url,
            })

        # Save after processing all rows
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Extracts total records and generates paginated requests.
        """

        text = response.xpath("//div[contains(text(),'Showing')]/text()").get()
        total_records = int(re.search(r'of\s+(\d+)', text).group(1))

        per_page = 20
        total_pages = (total_records // per_page) + 1

        for page in range(total_pages):
            page_url = f"{self.directory_url}?page={page}"
            yield scrapy.Request(page_url, callback=self.parse_employee)

    def parse_employee(self, response):
        """
        Extracts faculty/staff details from each page.
        """

        for row in response.xpath("//table[contains(@class,'views-table')]//tbody/tr"):

            name = row.xpath("./td[1]//a/text()").get(default="").strip()
            first_name, last_name = (name.split(" ", 1) + [""])[:2]

            title = row.xpath("./td[2]/text()").get(default="").strip()
            title = re.sub(r"\s+", " ", title)

            # Decode ROT13 encoded email
            encoded_email = row.xpath(".//a[@data-mail-to]/@data-mail-to").get()
            email = ""
            if encoded_email:
                email = codecs.decode(encoded_email, "rot_13")
                email = email.replace("/at/", "@").replace("/dot/", ".")

            phone = row.xpath(".//td[contains(@class,'field-phone-number')]//a/text()").get(default="").strip()

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": f"{first_name} {last_name}".strip(),
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })

        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    # CALENDAR SCRAPER
    def parse_calendar(self, response):
        """
        Extracts all academic calendar detail page URLs.
        """

        calendar_links = response.xpath(
            "//h4[contains(@class,'text-align-center')]/following-sibling::div//table//tbody//tr/td[1]//a/@href"
        ).getall()

        for link in calendar_links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_calendar_details)

    def parse_calendar_details(self, response):
        """
        Extracts term name and date details from calendar pages.
        """

        def get_term_name(url):
            match = re.search(r'calendar-(fall|spring|summer|winter)-(\d{4})', url)
            return f"{match.group(1).title()} {match.group(2)}" if match else ""

        term_name = get_term_name(response.url)

        for row in response.xpath("//div[contains(@class,'table--wysiwyg')]//tbody/tr"):

            description = " ".join(row.xpath("./td[1]//text()").getall()).strip()
            date = " ".join(row.xpath("./td[2]//text()").getall()).strip()

            if not description or not date:
                continue

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": term_name,
                "Term Date": date,
                "Term Date Description": re.sub(r'\s+', ' ', description),
            })

        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")