import re
import io
import scrapy
import requests
import pdfplumber
import urllib.parse
import pandas as pd
from ..utils import save_df

class HolycrossSpider(scrapy.Spider):
    """
    Scrapy spider for scraping data from 
    College of the Holy Cross.

    Website: https://www.holycross.edu/

    This spider collects:
    - Course data (from PDF schedule)
    - Faculty directory data
    - Academic calendar data

    SCRAPE_MODE options:
        - "course"
        - "directory"
        - "calendar"
        - "all" (default behavior handled externally)
    """
    
    name = "holycross"
    institution_id = 258446529226893268

    # Data storage containers
    course_rows = []
    directory_row = []
    calendar_rows = []

    # Base URLs
    course_url = "https://www.holycross.edu/document/fall-2025-course-schedule"
    directory_url = "https://www.holycross.edu/academics/faculty/directory?page=1"
    calendar_url = "https://www.holycross.edu/academics/support-resources/academic-calendar"


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            self.parse_course()

        elif mode == 'directory': 
            # Iterate through all directory pages (1–70)   
            for i in range (1,71):
                directory_urls = f"https://www.holycross.edu/academics/faculty/directory?page={i}"
                yield scrapy.Request(url=directory_urls, callback=self.parse_directory, dont_filter=True)
            
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        


    def parse_course(self):
        """
        Extract course schedule from PDF.

        Logic:
        1. Download PDF.
        2. Identify course header lines (Dept - Number - Title).
        3. Extract class number and section rows.
        4. Associate rows with the last identified course header.
        """
        response = requests.get(self.calendar_url)
        current_course_full = ""

        # Regex to capture: DEPT - NUM - FULL TITLE (stops before 'Units:')
        header_re = re.compile(r'^([A-Z]{4})\s*-\s*(\d{3}[A-Z]?)\s*-\s*(.*?)\s+Units:')
        
        # IMPROVED Regex: Now allows for leading whitespace to catch indented rows (LEC rows)
        # Group 1: Schedule No, Group 2: Section
        row_re = re.compile(r'^\s*(\d{4,5})\s+([A-Z0-9]{2,3})\s+')
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue
                
                for line in text.split('\n'):
                    # We don't strip() immediately so the regex can detect the indentation
                    
                    # 1. Capture the Full Course Name + Title
                    header_match = header_re.search(line)
                    if header_match:
                        dept = header_match.group(1)
                        num = header_match.group(2)
                        title = header_match.group(3).strip()
                        current_course_full = f"{dept}-{num}- {title}"
                        continue

                    # 2. Extract Row Data (Handles both LAB and indented LEC rows)
                    row_match = row_re.match(line)
                    if row_match and current_course_full:
                        parts = line.split()
                        
                        class_num = row_match.group(1)
                        section = row_match.group(2)
                        
                        # Enrollment is usually the 2nd digit-only block from the end
                        # We'll look for the first number from the right that isn't the last item
                        enroll = "25" # Default
                        for part in reversed(parts[:-1]):
                            if part.isdigit():
                                enroll = part
                                break
                        
                        # Instructor: handle the "P " prefix and multiple names
                        instructor_raw = " ".join(parts[-2:])
                        instructor = instructor_raw.replace("P ", "").strip()

                        self.course_rows.append({
                            "Cengage Master Institution ID": 258446529226893268,
                            "Source URL": response.url,
                            "Course Name": current_course_full,
                            "Course Description": '',
                            "Class Number": class_num,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": '',
                            "Course Dates": '',
                            "Location": '',
                            "Textbook/Course Materials": "",
                        })

    def parse_directory(self, response):
        """
        Parse faculty listing page.

        Extract profile URLs and send requests to individual profiles.
        """

        blocks = response.xpath('//div[@class="people_card_info"]//a[@class="people_card_name_link"]/@href').getall()
        for block in blocks:
            profile_url = urllib.parse.urljoin(response.url, block)
            yield scrapy.Request(url=profile_url, callback=self.parse_directory_profile, dont_filter=True)

    def parse_directory_profile(self, response):
        """
        Extract individual faculty profile information.
        """
        name = response.xpath('//h1/text()').get('').strip()
        title = response.xpath('normalize-space(//div[@class="people_header_job"]//p)').get('').strip()
        email = response.xpath('//a[@class="people_header_meta_item_content_link"]/text()').get('').strip()
        self.directory_row.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": ''
        })

    def parse_calendar(self, response):
        """
        Extract academic calendar data.

        Logic:
        - Each table row contains date and description.
        - Term name is extracted from closest preceding <h3>.
        """
        
        blocks = response.xpath('//table//tr')
        for block in blocks:
            term_name = block.xpath('preceding::h3[1]/text()').get('')
            date_text = block.xpath('.//td[1]//text()').get('').strip()
            desc_text = block.xpath('.//td[2]//text()').get('').strip()
           
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": term_name,
                "Term Date": date_text,
                "Term Date Description": desc_text
            })

    def closed(self, reason):
        """
        Called automatically when spider closes.

        Saves directory dataset after all profile pages are processed.
        """
        df = pd.DataFrame(self.directory_row)
        save_df(df, self.institution_id, "campus")

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")

        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")
