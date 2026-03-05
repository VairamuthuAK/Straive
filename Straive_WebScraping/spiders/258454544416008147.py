import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO


class PrattccSpider(scrapy.Spider):

    name = "prattcc"
    institution_id = 258454544416008147
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_sourse_url = 'https://prattcc.edu/wp-content/uploads/2025/11/Spring-26-Schedule-of-Classes-Online-Only.pdf'

    # DIRECTORY CONFIG
    directory_source_url = "https://prattcc.edu/employee-directory/"
    directory_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        }

    # CALENDAR CONFIG
    calendar_source_url = "https://prattcc.edu/wp-content/uploads/2025/10/25-26-Fall-Spring-Academic-Calendars-6.19.25.pdf"

    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course data is provided as a PDF file.
        The PDF is downloaded in `start_requests` and parsed using
        the `pdfplumber` module to extract structured course information.

        - Directory data is available as a standard HTML page.
        It is scraped using normal Scrapy requests and parsed in
        the `parse_directory`

        - Calendar data is also available as a PDF.
        It is processed similarly using `pdfplumber` during the request flow.
        """
        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

    # PARSE COURSE
    def parse_course(self):
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
        Extracts course schedule data from a PDF file using text-based parsing.

        Workflow:
        - Downloads the course schedule PDF using requests.
        - Reads the PDF in memory using pdfplumber.
        - Iterates through each page and extracts raw text.
        - Splits extracted text into individual lines.
        - Identifies course entries using a course number regex pattern.
        - Parses relevant fields such as:
            - Course number and section
            - Course title
            - Start and end dates
            - Instructor name
        - Constructs structured course records and appends them
        to self.course_rows.

        This approach is used when course data is presented as
        unstructured text rather than tables within the PDF.
        """
        # Send an HTTP GET request to download the course PDF
        response = requests.get(self.course_sourse_url)
        pdf_bytes = BytesIO(response.content)
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                for line in text.split("\n"):
                    # Match course numbers like ABC123-E-1
                    course_no_match = re.search(r"[A-Z]{3}\d{3}-[EO]-\dE?", line)
                    if not course_no_match:
                        continue # Skip lines without a valid course number

                    class_number = course_no_match.group()
                    title = line.split(class_number, 1) # Split the line on the course number to separate the title
                     # Extract course number (ABC123) and section (E-1)
                    course_number = class_number.split('-')[0]
                    section = class_number.split("-", 1)[1]

                    # Extract start and end dates in MM/DD/YY format
                    dates = re.findall(r"\d{2}/\d{2}/\d{2}", line)
                    start_date = dates[0] if len(dates) > 0 else ""
                    end_date = dates[1] if len(dates) > 1 else ""

                    # Extract instructor name in "Lastname, Firstname" format
                    instructor_match = re.search(r"[A-Z][a-z]+, [A-Z][a-z]+", line)
                    instructor = instructor_match.group() if instructor_match else ""

                    self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.course_sourse_url,
                            "Course Name": f'{course_number} - {title}',
                            "Course Description": '',
                            "Class Number": course_number,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": '',
                            "Course Dates": f'{start_date} - {end_date}',
                            "Location": '',
                            "Textbook/Course Materials": ''
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
        blocks = response.xpath('//table/tbody/tr')
        for block in blocks:
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.directory_source_url,
            "Name": block.xpath('.//td[1]/text()').get('').strip(),
            "Title": block.xpath('.//td[2]/text()').get('').strip(),
            "Email":"" if (block.xpath('.//td[3]/text()').get() or "").strip() in {"-", "–", "—"} else (block.xpath('.//td[3]/text()').get() or "").strip(),
            "Phone Number": "" if (block.xpath('.//td[4]/text()').get() or "").strip() in {"-", "–", "—"} else (block.xpath('.//td[4]/text()').get() or "").strip(),
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

        """
        Workflow:
        - Downloads the academic calendar PDF using requests.
        - Opens the PDF in memory using pdfplumber.
        - Selects a specific page and crops the right portion of the page
        where the calendar table is located.
        - Extracts the table from the cropped region.
        - Iterates through table rows while handling merged cells:
        - Month values may appear only once for multiple rows.
        - Day values may be empty for continuation rows.
        - Event descriptions may span multiple rows.
        - Combines fragmented rows into complete calendar entries.
        - Appends cleaned and structured records into self.calendar_rows.
        """

        # Set the page number to parse from the PDF (1-based index)
        page_number = 1
        # Fraction of the page width to crop (only right side of the page)
        start_fraction = 0.45  
        response = requests.get(self.calendar_source_url)
        response.raise_for_status()
        pdf_file = io.BytesIO(response.content)
        # Construct term name from the filename in the URL
        term_name = f"20{self.calendar_source_url.split('/')[-1].split('-Calendars')[0].split('-')[0]}-20{self.calendar_source_url.split('/')[-1].split('-Calendars')[0].split('-')[1]} {' '.join(self.calendar_source_url.split('/')[-1].split('-Calendars')[0].split('-')[2:])}"
        with pdfplumber.open(pdf_file) as pdf:
            # Get the target page
            page = pdf.pages[page_number - 1]
            width, height = page.width, page.height
            # Crop the right portion of the page
            right_bbox = (width * start_fraction, 0, width, height)
            cropped = page.crop(bbox=right_bbox)
            # Extract table from cropped area
            table = cropped.extract_table()
            if not table:
                return
            
            current_month = ""
            current_day = ""
            current_description = ""
            for row in table:
                month = row[0].strip() if row[0] else ""
                day = row[1].strip() if len(row) > 1 and row[1] else ""
                desc = row[2].strip() if len(row) > 2 and row[2] else ""
                # If month column is not empty, update current_month
                if month:
                    current_month = month

                # If day column is not empty, update current_day and store previous row
                if day:
                    # Save previous entry if exists
                    if current_day and current_description:
                        self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_source_url,
                        "Term Name":term_name,
                        "Term Date": f"{current_month} {current_day}",
                        "Term Date Description": current_description.strip(),
                    })
                    current_day = day
                    current_description = desc
                else:
                    # If day is empty, append description to previous row
                    if desc:
                        current_description += " " + desc

            if current_day and current_description:
                self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_source_url,
                        "Term Name":term_name,
                        "Term Date": f"{current_month} {current_day}",
                        "Term Date Description": current_description.strip(),
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
        