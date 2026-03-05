import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *


class VanguardSpider(scrapy.Spider):
    """
    Spider for Vanguard University.
    
    This spider extracts academic data from three main sources:
    1. PDF Course Schedules (Undergraduate, Graduate, and Online).
    2. Faculty Directory (Placeholder for future implementation).
    3. Academic Calendar (HTML tables on the university website).
    """
    name = "vanguard"
    institution_id = 258420917976197076
    
    # Internal storage for scraped data before exporting to CSV
    calendar_rows = []
    course_rows = []

    # Target URLs for Course Schedules (PDF format)
    course_urls = [
         # GRADUATE PROGRAMS
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesGRADPSYGFA.pdf',
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesGRADEDUGFA.pdf',
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesGRADNURSFA.pdf',
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesGRADPSOGFA.pdf',
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesGRADTHEOFA.pdf',

        # TRADITIONAL UNDERGRADUATE (TUG)
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesTUGFA.pdf',
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesTUGSU.pdf',
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesTUGSP.pdf',

        # ONLINE PROGRAMS
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesOnlineSU.pdf',
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesOnlineSP.pdf',
        'https://resources.vanguard.edu/Registrar/Files/CSWChangesOnlineFA.pdf'
    ]
    
    directory_url = ""

    #Academic calendar PDF
    calendar_url = "https://www.vanguard.edu/academics/academic-calendar"
  
    #Entry point
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            self.parse_course()
        elif mode == 'directory':
           yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

    def parse_course(self, response):
        """
        Parse course schedules from PDF catalogs.

        Extracts:
        - Course code & title
        - Section number
        - Instructor
        - Enrollment availability
        - Course dates
        - Location

        Each PDF page is scanned line-by-line using regex.
        """

        # Regex to identify days of the week (e.g., M W F) or date ranges
        DAY_PATTERN = re.compile(r'^(M|T|W|R|F|S)(\s+(M|T|W|R|F|S))*$')
        DATE_ONLY_PATTERN = re.compile(r'(\d{1,2}/\d{1,2}/\d{2}\s*-\s*\d{1,2}/\d{1,2}/\d{2})')

        # Fetch PDF content
        for url in self.course_urls:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            pdf_bytes = io.BytesIO(response.content)

            # Assign type based on URL naming convention to handle different table layouts
            if "Online" in url:
                pdf_type = "ONLINE"
            elif "TUG" in url:
                pdf_type = "TUG"
            else:
                pdf_type = "GRAD"

            with pdfplumber.open(pdf_bytes) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()

                    for table in tables:
                        for r in table:

                            if not r or not r[0]:
                                continue
                            
                            # Clean whitespace and remove newlines from all cells
                            r = [c.replace("\n", " ").strip() if c else "" for c in r]
                            
                            # Skip header rows
                            if r[0].lower().startswith("section"):
                                continue

                            #GRADUATE FORMAT LOGIC
                            if pdf_type == "GRAD":

                                if len(r) < 11:
                                    continue

                                section = r[0]
                                title = r[1]

                                start_date = r[4] or r[6]
                                end_date = r[5]
                                instructor = r[10]

                                # Shift detection: if the instructor cell contains time, shift index
                                if instructor.endswith("AM") or instructor.endswith("PM"):
                                    instructor = r[13]
                                    end_date = r[8]

                                course_dates = f"{start_date} - {end_date}".strip(" -")
                                location = ""

                            #TUG (Traditional Undergraduate) FORMAT LOGIC
                            elif pdf_type == "TUG":

                                if len(r) < 11:
                                    continue

                                section = r[0]
                                location = r[1]
                                title = r[2]
                                start_end_date = r[6]
                                instructor = r[9]

                                # If instructor column contains day codes (M/T/W), data is shifted
                                if DAY_PATTERN.match(instructor):
                                    instructor = r[14]
                                    start_end_date = r[10]

                                # Clean up dates using regex
                                match = DATE_ONLY_PATTERN.search(start_end_date)
                                course_dates = match.group(1) if match else start_end_date

                            #ONLINE FORMAT LOGIC
                            else:
                                if len(r) < 3:
                                    continue

                                section = r[0]
                                title = r[1]
                                instructor = r[2]
                                course_dates = ""
                                location = ""

                            # Final cleanup and normalization of Course ID and Section
                            if not section or "Key:" in section:
                                continue
                            
                            # Splits 'ENGL-101-01' into 'ENGL-101' and '01'
                            class_number = "-".join(section.split("-")[:2])
                            section_code = section.split("-")[-1]
                            course_name = f"{class_number} {title}".strip()

                            if instructor:
                                self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": url,
                                    "Course Name": course_name,
                                    "Course Description": "",
                                    "Class Number": class_number,
                                    "Section": section_code,
                                    "Instructor": instructor,
                                    "Enrollment": "",
                                    "Course Dates": course_dates,
                                    "Location": location,
                                    "Textbook/Course Materials": "",
                                })
                

        # Export all course data to CSV via utility function
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")
        
    # DIRECTORY PARSER      
    def parse_directory(self,response):
        pass

    # CALENDAR PARSER
    def parse_calendar(self, response):
        """
        Parse academic calendar PDF and extract:
        - Term Name
        - Term Date
        - Term Date Description
        """

        main_blocks = response.xpath('//div[@class="fsElement fsContent nk-table"]')
        for main_block in main_blocks:
            term_name = main_block.xpath('."]//parent::div[1]//parent::div//parent::section//h3/a/text()').get('')

            sub_blocks = main_block.xpath('.//table[@class="dcf-table dcf-table-responsive dcf-table-striped dcf-w-100%"]')
            for sub_block in sub_blocks:
                table_head = sub_block.xpath('.//thead/tr/th[1]/text()').get('')

                # Skip tables specifically for internal sessions if necessary
                if 'SESSION' in table_head:
                    continue

                rows = sub_block.xpath('.//tbody/tr')
                for row in rows:
                    term_description = row.xpath('.//td[@data-label="EVENT"]//text()').getall()
                    term_date = row.xpath('.//td[@data-label="DATE"]//text()').getall()
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": " ".join(term_date).strip(),
                        "Term Date Description": " ".join(term_description).strip()
                    })

        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
