import re
import scrapy
import pandas as pd
from ..utils import save_df


class CwcSpider(scrapy.Spider):
    name = "cwc"

    # Unique institution ID
    institution_id = 258448701779896288

    # URLs
    course_url = "https://www.cwc.edu/wp-content/uploads/cwcsearch.json"
    directory_url = "https://www.cwc.edu/about/directory/"
    calendar_url = "https://www.cwc.edu/academiccalendar/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage lists
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT – MODE BASED SCRAPING
    def start_requests(self):
        """
        SCRAPE_MODE can be:
        course / directory / calendar / all / combinations
        """
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:  # default = scrape everything
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # UTILITY CLEAN FUNCTION
    def clean(self, text):
        """Remove extra spaces and normalize text"""
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    # COURSE JSON SCRAPER
    def parse_course(self, response):
        """
        Parse course JSON data and store in course_rows
        """
        data = response.json()

        for course in data.get("data", {}).get("results", []):
            # Instructor list
            instructor = ", ".join(course.get("instructors", [])) if course.get("instructors") else ""

            # Course details
            department = course.get("department", "")
            number = course.get("course_number", "")
            title = course.get("title", "")

            course_name = f"{department}-{number} {title}"
            class_number = f"{department}-{number}"

            course_description = self.clean(course.get("course_description", ""))
            section = course.get("course_section", "")

            start_date = course.get("start_date", "")
            end_date = course.get("end_date", "")
            course_dates = f"{start_date} - {end_date}"

            location = course.get("location", "")

            # Append row
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": "https://www.cwc.edu/course-search/",
                "Course Name": course_name,
                "Course Description": course_description,
                "Class Number": class_number,
                "Section": section,
                "Instructor": instructor,
                "Enrollment": "",
                "Course Dates": course_dates,
                "Location": location,
                "Textbook/Course Materials": "",
            })

        # Save to CSV
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # FACULTY DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Scrape faculty/staff directory table
        """

        for row in response.xpath("//tbody//tr[td]"):
            name = self.clean(row.xpath(".//td[1]//text()").get())
            title = self.clean(row.xpath(".//td[2]//text()").get())
            department = self.clean(row.xpath(".//td[3]//text()").get())
            phone = self.clean(row.xpath(".//td[5]//text()").get())
            email = self.clean(row.xpath(".//td[6]//a/text()").get())

            # Combine title + department
            full_title = f"{title}, {department}" if department else title

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": full_title,
                "Email": email,
                "Phone Number": phone,
            })

        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # ACADEMIC CALENDAR SCRAPER
    def parse_calendar(self, response):
        """
        Scrape academic calendar semester tables
        """

        # Loop through semester headings
        for semester in response.xpath("//h2[contains(@class,'elementor-heading-title')]"):
            term_name = semester.xpath("normalize-space(.)").get()

            # Get next table after heading
            table = semester.xpath("following::table[1]")

            # Loop through table rows (skip header)
            for row in table.xpath(".//tr[position()>1]"):
                event = self.clean(row.xpath(".//td[1]/text()").get())
                date_list = row.xpath(".//td[2]//text()").getall()
                date = " ".join(d.strip() for d in date_list if d.strip())

                if event and date:
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": date,
                        "Term Date Description": event,
                    })

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")
