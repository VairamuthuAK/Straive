import re                     
import scrapy             
import pandas as pd         
from ..utils import *       
from io import BytesIO        
from parsel import Selector 


class FvccSpider(scrapy.Spider):
    name = "fvcc"

    # Unique institution ID used for all datasets
    institution_id = 258443782670804944

    course_url = 'https://elements.fvcc.edu/Schedules/su26/index.asp'

    # Faculty / Staff directory page
    directory_url = 'https://www.fvcc.edu/directory?category=all'

    # Academic calendar page
    calendar_url = "https://www.fvcc.edu/academics/academic-resources/calendar"

    # Initialize storage lists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Store scraped directory data
        self.directory_rows = []

        # Store scraped calendar data
        self.calendar_rows = []

        # Store scraped course data
        self.course_rows = []

    # Entry Point – Select Scrape Mode
    def start_requests(self):

        # Read scrape mode from settings (course / directory / calendar / combinations)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode Execution ----
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

    def clean(self, value):
        return (value or "").strip()


    def parse_course(self, response):
        """
        Scrapes all course schedule data using Playwright
        because the page is dynamically rendered.
        """
        values= response.xpath('//td[contains(@class,"courseThing")]/a/@href').getall()
        for value in values:
            url_val= f'https://elements.fvcc.edu/Schedules/su26/{value}'
            yield scrapy.Request(url_val, callback=self.parse_course_details)


    def parse_course_details(self, response):
        row = response.xpath('//tr[@id and contains(@id,"_")]')[0]

        # Course code & section from tr id
        # ACTG_150_80 → ACTG_150 , 80
        course_id = row.xpath('./@id').get()
        class_number, section = course_id.rsplit('_', 1)

        # Course title
        title = row.xpath('.//td[@class="courseThing"]/a/text()').get(default='').strip()

        course_name = f"{class_number.replace('_', ' ')} - {title}"

        # Course dates
        course_dates = row.xpath(
            './/span[contains(@class,"sched_Meets")]/text()'
        ).get(default='').replace('Meets:', '').strip()

        # Location
        location = row.xpath(
            './/span[contains(@class,"sched_campus")]/text()'
        ).get(default='').replace('Course', '').strip()

        # Instructor
        instructor = row.xpath(
            './/td[contains(@class,"courseFaculty")]/text()'
        ).get(default='').strip()

        # Enrollment (Seats Available)
        enrollment = row.xpath(
            './td[7]/a/@title'
        ).get(default='').strip()
        # Keep only first two numbers separated by "/"
        enrollment = "/".join(enrollment.split("/")[:2])

        # Course description (from next row)
        course_description = response.xpath(
            '//p[@class="courseDesc"]/text()'
        ).get(default='').strip()

        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Course Name": course_name,
            "Course Description": re.sub(r'\s+',' ', course_description),
            "Class Number": class_number,
            "Section": section,
            "Instructor": instructor,
            "Enrollment": enrollment,
            "Course Dates": course_dates,
            "Location": location,
            "Textbook/Course Materials": "",
        })

        # ---- SAVE ----
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")


    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Scrapes faculty and staff directory profiles.
        """
        values= response.xpath('//div[@class="p-2 mt-1"]/a/@href').getall()
        for value in values:
            yield scrapy.Request(value, callback=self.parse_directory_details)

         # 🔥 Pagination – Next page
        next_page = response.xpath(
            '//li[@class="next"]/a/@href'
        ).get()

        if next_page:
            yield response.follow(next_page, callback=self.parse_directory)

    def parse_directory_details(self, response):
        name = response.xpath('//h1/text()').get('').strip()

        title = response.xpath('//div[@class="font-semibold capitalize text-black"]//text()').get('').strip()

        email = response.xpath('///a[starts-with(@href,"mailto:")]/text()').get('').strip()

        phone = response.xpath('//a[starts-with(@href,"tel:")]/text()').get('').strip()

        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title":title,
            "Email": email,
            "Phone Number": phone,
        })

        # Save directory data
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    # CALENDAR SCRAPER
    def parse_calendar(self, response):
        # Loop each term (Fall 2025, Spring 2026, etc.)
        for term in response.xpath('//h2[contains(@class,"wp-block-heading")]'):

            term_name = term.xpath('text()').get(default='').strip()

            base_url = "https://www.fvcc.edu/academics/academic-resources/calendar"

            # Decide source URL based on term name
            term_lower = term_name.lower()

            if "fall" in term_lower:
                source_url = base_url + "#fall25"
            elif "spring" in term_lower:
                source_url = base_url + "#spring26"
            elif "summer" in term_lower:
                source_url = base_url + "#summer26"
            else:
                source_url = response.url  # fallback

            # UL list immediately after the term heading
            events = term.xpath(
                'following::ul[contains(@class,"wp-block-list")][1]/li'
            )

            for li in events:
                raw_date = li.xpath('.//strong/text()').get(default='').strip()

                # Remove (F), (M), (Th) etc
                term_date = re.sub(r'\s*\([^)]*\)', '', raw_date).strip()
                term_date = term_date.replace(':','').strip()

                # Get description (exclude <strong>)
                description = " ".join(
                    li.xpath('.//text()[not(parent::strong)]').getall()
                )
                description = description.strip()
                # 🔹 remove leading colon only
                description = re.sub(r'^:\s*', '', description)

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": source_url,
                    "Term Name": term_name,
                    "Term Date": term_date,
                    "Term Date Description": description,
                })

        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")


