import scrapy
import requests
import pandas as pd
from ..utils import *
from io import BytesIO
from docx import Document


class WoodsSpider(scrapy.Spider):

    name = "woods"
    institution_id = 258422219900741590
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://smwc.smartcatalogiq.com/en/2025-2026/undergraduate-catalog/courses"
    
    # DIRECTORY CONFIG
    directory_url = "https://www.smwc.edu/offices-resources/faculty-directory/"

    # CALENDAR CONFIG
    calendar_source_url = "https://www.smwc.edu/wp-content/uploads/2025/04/2025-26-Academic-Master-Calendar.docx"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course and directory data are scraped using Scrapy
        inside `parse_course` and `parse_directory`.

        - Calendar data is provided as a Word document, so `parse_calendar` uses
        `io.BytesIO` and `python-docx` (`Document`) to extract dates and descriptions.
        """
        
        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            yield scrapy.Request(url = self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

    # PARSE COURSE
    def parse_course(self,response):
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
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """

        """
        The logic handles:
        - Identification of course entries using structured patterns
        - Extraction of CRN, subject, course number, section, and title
        - Handling of course date values that may span multiple lines
        - Detection of instructor names and course location information
        - Normalization of extracted values into structured course records

        All extracted course data is stored in a consistent format and appended
        to the course output collection for further processing.
        """
        urls = response.xpath('//ul[@class="sc-child-item-links"]/li/a/@href').getall()
        for url in urls:
            url = response.urljoin(url)
            yield scrapy.Request(url = url, callback=self.parse_course_department, dont_filter=True)

    def parse_course_department(self,response):
        urls = response.xpath('//div[@id="main"]/ul/li/a/@href').getall()
        for url in urls:
            url = response.urljoin(url)
            yield scrapy.Request(url = url, callback=self.parse_course_section, dont_filter=True)

    def parse_course_section(self,response):
        name = ''.join(response.xpath('//h1//text()').getall()).strip()
        description = ''.join(response.xpath('//div[@class="desc"]/p//text() | //div[@class="desc"]//text()').getall()).strip()
        location = response.xpath('//div[@id="offered"]/text()').get('').strip()
        if 'Campus' in location:
            location = location
        else:
            location = ''
        
        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Course Name": re.sub(r'\s+',' ',name),
            "Course Description": re.sub(r'\s+',' ',description),
            "Class Number": response.xpath('//h1/span/text()').get('').strip(),
            "Section":  '',
            "Instructor": '',
            "Enrollment": "",
            "Course Dates": '',
            "Location": re.sub(r'\s+',' ',location),
            "Textbook/Course Materials": ""
        })

    # PARSE DIRECTORY
    def parse_directory(self,response):
        """
        Parse directory using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """
        page_count = int(response.xpath('//input[@name="paged"]/@max').get('').strip())
        for page in range(1, page_count + 1):
            url = f"https://www.smwc.edu/offices-resources/faculty-directory/page/{page}/"
            yield scrapy.Request(url = url, callback=self.parse_directory_page, dont_filter=True)

    def parse_directory_page(self,response):
        blocks = response.xpath('//div[contains(@class,"faculty type-faculty")]')
        for block in blocks:
            name = block.xpath('.//h3/a/text()').get('').replace('\xa0', ' ').strip()
            dept = block.xpath('.//h3/following-sibling::div[@class="title"]/text()').get('').strip()
            courses = block.xpath('.//strong[contains(text(),"Departments / Offices")]/parent::div/text()[2]').get('').strip()
            title = f"{dept}, {courses}" if courses else dept
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": re.sub(r'\s+',' ',title),
            "Email": block.xpath('.//a[contains(@href,"mailto")]/text()').get('').strip(),
            "Phone Number": block.xpath('.//a[contains(@href,"tel:")]/text()').get('').strip(),
            })
        
    # PARSE CALENDAR
    def parse_calendar(self):
        """
        Parse calendar using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """     
        response = requests.get(self.calendar_source_url)
        response.raise_for_status()
        
        # Load the Word document from the response content in memory
        doc = Document(BytesIO(response.content))

        current_term = "Fall 2025"
        # Regex pattern to match dates at the start of a paragraph, e.g., "August 12" or "August 12-14"
        DATE_PATTERN = re.compile(r"^([A-Za-z]+\s+\d{1,2}(\s*[-–]\s*\d{1,2})?)")

        for para in doc.paragraphs:
            text = para.text.strip()
            if not text:
                continue

            # Check if paragraph starts with a date
            date_match = DATE_PATTERN.match(text)
            if date_match:
                term_date_str = date_match.group(1) # Extract the date portion
                m_desc = text.replace(term_date_str, "").strip() # Remove date to get the description

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_source_url,
                    "Term Name": current_term,
                    "Term Date": re.sub(r'\s+', ' ', term_date_str),
                    "Term Description": re.sub(r'\s+', ' ', m_desc),
                })

    #Called automatically when the Scrapy spider finishes scraping.
    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")

        