import json
import scrapy
import pandas as pd
from ..utils import *
from io import BytesIO
from parsel import Selector


class LakeshoreSpider(scrapy.Spider):
    # Unique spider name used by Scrapy
    name = "lakeshore"

    # Unique institution identifier used across all datasets
    institution_id = 258462551971489754

    # Endpoint for course search results (AJAX-based)
    course_url = 'https://scripts.lakeshoretech.org/findaclass/search-results.php'

    # Endpoint for faculty/staff directory data
    directory_url = 'https://scripts.lakeshoretech.org/staff_directory/getStaffbyLocation.php'

    # Academic calendar page URL
    calendar_url = "https://lakeshore.edu/admissions/academic-calendar"

    def __init__(self, *args, **kwargs):
        # Initialize parent Scrapy Spider
        super().__init__(*args, **kwargs)

        # List to store faculty/staff directory records
        self.directory_rows = []

        # List to store academic calendar records
        self.calendar_rows = []

        # List to store course schedule records
        self.course_rows = []

    def start_requests(self):
        # Read scrape mode from Scrapy settings (default: all)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Run only course scraper
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        # Run only directory scraper
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # Run only calendar scraper
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # Run course and directory scrapers
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # Run course and calendar scrapers
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # Run directory and calendar scrapers
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # Default behavior: run all scrapers
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

    def clean(self, value):
        # Helper method to safely strip whitespace
        return (value or "").strip()

    def parse_course(self, response):
        # Payload used to request course search results
        payload = {
            "term": "1262",
            "classDesc": "",
            "classNumber": "",
            "catalogNumber": "",
            "time": "both",
            "delivery": "All",
            "location": "All"
        }

        # Headers required for AJAX-style POST request
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0"
        }

        # Submit form request to fetch course list
        yield scrapy.FormRequest(
            url=self.course_url,
            method="POST",
            headers=headers,
            formdata=payload,
            callback=self.parse_course_details
        )

    def parse_course_details(self, response):
        # Load JSON response containing course list
        data = json.loads(response.text)
        classes = data.get("classes", [])

        # Iterate over each course and request detailed data
        for cls in classes:
            yield scrapy.FormRequest(
                url="https://scripts.lakeshoretech.org/findaclass/class-details.php",
                method="POST",
                formdata={
                    "term": str(cls.get("STRM")),
                    "catalogNumber": str(cls.get("CATALOG_NBR"))
                },
                callback=self.parse_course_datas,
                cb_kwargs={
                    "term_id": cls.get("STRM"),
                    "catalogNumber": cls.get("CATALOG_NBR")
                },
                dont_filter=True
            )

    def parse_course_datas(self, response, term_id, catalogNumber):
        # Load JSON response containing class-level details
        data = json.loads(response.text)
        classes = data.get("classes", [])

        # Extract course information
        for cls in classes:
            enrollment = f"{cls.get('ENRL_TOT')}/{cls.get('ENRL_CAP')}"
            class_num = cls.get("CLASS_NBR")

            # Build base course record
            course_item = {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": f"{class_num}-{cls.get('COURSE_TITLE_LONG')}",
                "Course Description": "",
                "Class Number": class_num,
                "Section": cls.get("CLASS_SECTION"),
                "Instructor": cls.get("NAME"),
                "Enrollment": enrollment,
                "Course Dates": f"{cls.get('Start_Date')} - {cls.get('End_Date')}",
                "Location": f"{cls.get('location')} {cls.get('location_Desc')}",
                "Textbook/Course Materials": "",
            }

            # Request detailed course description
            yield scrapy.FormRequest(
                url="https://scripts.lakeshoretech.org/findaclass/courseDesc.php",
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "User-Agent": "Mozilla/5.0"
                },
                formdata={
                    "Term": str(term_id),
                    "catalogNumber": str(catalogNumber),
                    "classID": str(class_num)
                },
                callback=self.parse_course_description,
                cb_kwargs={"course_item": course_item},
                dont_filter=True
            )

    def parse_course_description(self, response, course_item):
        # Parse course description from JSON response
        data = json.loads(response.text)
        descr = data.get("classDetails", [{}])[0].get("DESCRLONG", "").strip()

        # Attach description to course record
        course_item["Course Description"] = descr
        self.course_rows.append(course_item)

        # Save accumulated course data
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    def parse_directory(self, response):
        # Campus locations used for directory scraping
        locations=response.xpath('//select[@id="selectCampus"]//option//text()').getall()
        # Headers required for directory API requests
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://lakeshore.edu",
            "Referer": "https://lakeshore.edu/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/143.0.0.0 Safari/537.36"
            ),
        }

        # Loop through each campus location
        for location in locations:
            yield scrapy.FormRequest(
                url=self.directory_url,
                method="POST",
                headers=headers,
                formdata={"location": location},
                callback=self.parse_directory_details,
                meta={"location": location},
            )

    def parse_directory_details(self, response):
        # Load JSON response containing staff data
        data = json.loads(response.text)
        location = response.meta["location"]

        # Extract staff list for the given location
        staff_by_location = data.get("Staff", {})
        staff_list = staff_by_location.get(location, [])

        # Build directory records
        for emp in staff_list:
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": emp.get("Emp_Name", "").strip(),
                "Title": emp.get("Department", "").strip(),
                "Email": emp.get("Emp_Email", "").strip(),
                "Phone Number": emp.get("Emp_Phn", "").strip(),
            })

        # Save accumulated directory data
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self, response):
        # Select all rows from the academic calendar table
        rows = response.xpath('//table[@class="b-table"]//tr')

        # Iterate through calendar rows
        for row in rows:
            date = row.xpath('./td[1]//text()').getall()
            desc = row.xpath('./td[2]//text()').getall()

            # Normalize and clean extracted text
            date = " ".join(d.strip() for d in date if d.strip())
            desc = " ".join(d.strip() for d in desc if d.strip())

            # Skip empty rows
            if not date and not desc:
                continue

            # Append calendar record
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": "",
                "Term Date": date,
                "Term Date Description": desc
            })

        # Save accumulated calendar data
        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
