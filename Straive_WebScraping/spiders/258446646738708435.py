import re
import scrapy
import pandas as pd
import json
from parsel import Selector
from inline_requests import inline_requests
from ..utils import *


class MinnesotaSpider(scrapy.Spider):
    """
    Scrapy Spider for Minnesota West Technical & Community College.

    This spider scrapes:
    1. Course data
    2. Employee directory (via JSON API)
    3. Calendar (currently empty)

    """
    
    name="minnesota"
    institution_id = 258446646738708435


    # Base URL for building full course detail links
    course_base_url = "https://eservices.minnstate.edu"

    # Entry page for course search
    course_url = "https://eservices.minnstate.edu/registration/search/advanced.html?campusid=146"

    # Directory page (UI)
    directory_url = "https://www.mnwest.edu/about-us/employee-directory/index.php"

    # Directory API endpoint (returns JSON)
    employee_url = "https://www.mnwest.edu/_resources/dmc/php/faculty.php?datasource=employee-directory&xpath=items%2Fitem&returntype=json"

    # Headers for course requests (to mimic a browser)
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    # Headers for directory API requests
    directory_headers = {
        'referer': 'https://www.mnwest.edu/about-us/employee-directory/index.php',
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }

    # Template URL used for dynamically fetching courses
    base_url = (
        "https://eservices.minnstate.edu/registration/search/advancedSubmit.html"
        "?campusid=146"
        "&searchrcid=0209"
        "&searchcampusid="
        "&yrtr={sem}"
        "&subject={subject}"
        "&openValue=ALL"
        "&delivery=ALL"
        "&credittype=ALL"
        "&honorsflag=honorsAll"
        "&textbookcost=all"
        "&resultNumber=250"
    )


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            self.parse_calendar()
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_calendar()
            # self.parse_calendar(self.calendar_url)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

    # Allows inline requests inside this method
    @inline_requests
    def parse_course(self, response):
        """
        Parse course data using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Course Name"                   : str
        - "Course Description"            : str
        - "Class Number"                  : str
        - "Section"                       : str
        - "Instructor"                    : str
        - "Enrollment"                    : int
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """

        # This list will store all extracted course data
        course_data =[]
        
        # Get all semester values
        semester_values = response.xpath(".//select[@name='yrtr']/option[@value!='']/@value").getall()

        # Get all subject values
        subject_values = response.xpath(".//select[@name='subject']/option[@value!='']/@value").getall()

        # Loop through each semester
        for sem in semester_values:

            # Loop through each subject
            for subject in subject_values:

                    # Build the dynamic URL
                    url = self.base_url.format(sem=sem, subject=subject)

                    # Request the course list page
                    all_course_response = yield scrapy.Request(
                        url=url,
                        headers=self.headers,
                        dont_filter=True
                    )

                    sel = Selector(text=all_course_response.text)

                    # Find all course rows
                    rows = sel.xpath('//table[.//a[contains(@href,"detail.html")]]//tbody/tr')
                    
                    # If no courses found, skip
                    if not rows:
                        continue
                    
                    # Extract detail page URLs & remove duplicates
                    detail_urls = rows.xpath('.//a[contains(@href,"/registration/search/detail.html")]/@href').getall()
                    unique_detail_urls = list(set(detail_urls))

                    # Loop through each course detail page
                    for detail in unique_detail_urls:

                        # Build full URL
                        fullUrl = self.course_base_url + detail

                        # Request course detail page
                        course_response = yield scrapy.Request(
                            url=fullUrl,
                            headers=self.headers,
                            dont_filter=True
                        )
                        courseSel = Selector(text=course_response.text)
                        
                        # Create a dictionary for one course
                        item = {}

                        # Fixed fields
                        item["Cengage Master Institution ID"] = self.institution_id
                        item["Source URL"] = fullUrl


                        item["Course Name"] = re.sub(r'\s+', ' ',courseSel.xpath(
                            '//h1//text()[normalize-space()]'
                        ).get(default="").strip())


                        item["Course Description"] = re.sub(r'\s+', ' ',courseSel.xpath(
                            '//div[@class="detaildiv" and contains(.,"Description")]/following-sibling::text()[normalize-space()]'
                        ).get(default="").strip())

                        item["Class Number"] = re.sub(r'\s+', ' ',courseSel.xpath(
                            '//table[contains(@class,"myplantable")]//tbody[@class="course-detail-summary"]//tr/td[2]/text()'
                        ).get(default="").strip())

                        item["Section"] = re.sub(r'\s+', ' ',courseSel.xpath(
                            '//table[contains(@class,"myplantable")]//tbody[@class="course-detail-summary"]//tr/td[5]/text()'
                        ).get(default="").strip())

                        instructors = courseSel.xpath(
                            '//table[contains(@class,"myplantable")]'
                            '//tbody[@class="course-detail-summary"]'
                            '//tr/td[12]//text()[normalize-space()]'
                        ).getall()

                        # Clean + join
                        instructors = [i.strip() for i in instructors if i.strip()]
                        
                        # ✅ Remove duplicates but keep order
                        unique_instructors = list(dict.fromkeys(instructors))
                        item["Instructor"] = "| ".join(unique_instructors)


                        seat_table = courseSel.xpath(
                            '//div[@class="detaildiv" and normalize-space(.)="Seat Availability"]'
                            '/following-sibling::table[1]'
                        )

                        # IMPORTANT: skip the first empty <td>
                        tds = seat_table.xpath('.//tr/td[position() > 1]')

                        size_raw  = tds[0].xpath('normalize-space(.)').get()
                        enrolled_raw  = tds[1].xpath('normalize-space(.)').get()
                        
                        size = re.search(r'\d+', size_raw).group() if size_raw else ""
                        enrolled = re.search(r'\d+', enrolled_raw).group() if enrolled_raw else ""

                        item["Enrollment"] = f"{enrolled} / {size}"

                        
                        dates = courseSel.xpath(
                            './/table[contains(@class,"myplantable")]'
                            '//tbody[@class="course-detail-summary"]'
                            '//tr/td[7]//text()'
                        ).getall()

                        # Clean, normalize, remove empty strings
                        dates = [
                            re.sub(r'\s+', ' ', d.replace('\xa0', ' ').strip())
                            for d in dates
                            if d and d.strip()
                        ]

                        # Optional: keep only date ranges like 01/12 - 05/15
                        dates = [
                            d for d in dates
                            if re.search(r'\d{2}/\d{2}\s*-\s*\d{2}/\d{2}', d)
                        ]

                        # Remove duplicates while preserving order
                        dates = list(dict.fromkeys(dates))

                        item["Course Dates"] = " , ".join(dates)

                        item["Location"] = re.sub(r'\s+', ' ',courseSel.xpath(
                            '//div[@class="detaildiv" and contains(normalize-space(.), "Location Details")]'
                            '/following-sibling::table[1]'
                            '//b[normalize-space(.)="Location:"]/parent::td/text()[normalize-space()]'
                        ).get(default="").strip())

                        link = courseSel.xpath(
                            '//div[@class="detaildiv" and contains(.,"Course Books")]'
                            '/following::table[1]//tr/td[2]//a/@href'
                        ).get()

                        item["Textbook/Course Materials"] = link if link is not None else ""

                        course_data.append(item)

        course_df = pd.DataFrame(course_data)
        save_df(course_df, self.institution_id, "course")

    # Allows inline requests inside this method
    @inline_requests
    def parse_directory(self, response):
        """
        This function scrapes the employee/staff directory data.

        The data is fetched from a JSON API endpoint, not from HTML.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """
        
        # This list will store all employee records
        employeeData = []
        
        # Make a request to the employee JSON API
        employeeResponse = yield scrapy.Request(
                url=self.employee_url,
                headers=self.directory_headers,
                dont_filter=True
            )
        
        # Convert the JSON string into Python dictionary
        data = json.loads(employeeResponse.text)

        # Extract the list of employees from the JSON
        employees = data.get("data", [])

        # Loop through each employee record
        for emp in employees:

            # Dictionary for one employee
            item ={}

            item["Cengage Master Institution ID"] = self.institution_id
            item["Source URL"] = self.directory_url                
            item["Name"] = emp.get("displayName", "")

            # Combine title and department
            item["Title"] = f"{emp.get('title', '')}, {emp.get('department', '')}"
            item["Email"] = emp.get("email", "")
            item["Phone Number"] = emp.get("phone", "")

            # Add this employee to the list
            employeeData.append(item)
        
        directory_df = pd.DataFrame(employeeData)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self):
        """
        Parse calendar.

        Since no calendar data is available, generate a single-row file.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """

        data = [{
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": "data not available",
            "Term Name": "data not available",
            "Term Date": "data not available",
            "Term Date Description": "Calendar data not available"
        }]

        df = pd.DataFrame(data)
        save_df(df, self.institution_id, "calendar")
