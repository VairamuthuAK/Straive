import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *


class TricountyccSpider(scrapy.Spider):

    name = "tricountycc"
    institution_id = 258435390153713628
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_sourse_url = 'https://www.tricountycc.edu/wp-content/uploads/2025/09/Fall-2025-Schedule-Edited-15.pdf'

    # DIRECTORY CONFIG
    directory_source_url = "https://www.tricountycc.edu/faculty-staff/directory/"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://www.tricountycc.edu/academics/academic-calendar/"
    calendar_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course data is extracted using Playwright, as the course listing
        and detail pages are dynamically rendered and cannot be reliably
        accessed using standard Scrapy requests.

        - Directory data is available as static HTML pages and is scraped
        using normal Scrapy requests in the `parse_directory` callback.

        - Calendar data is provided as PDF files.
        These PDFs are downloaded using HTTP requests and parsed using
        the pdfplumber library to extract academic calendar information.
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

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

        pdf_url = self.course_sourse_url
        try:
            response = requests.get(pdf_url, timeout=30)
            response.raise_for_status()
        except Exception as e:
            print(f"Error: {e}")
            return

        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                words = page.extract_words()
                lines = {}
                for word in words:
                    top = round(word['top'], 1)
                    lines.setdefault(top, []).append(word)

                for top in sorted(lines.keys()):
                    sorted_words = sorted(lines[top], key=lambda x: x['x0'])
                    line_text = " ".join([w['text'] for w in sorted_words])
                    
                    # Regex for Course ID and Section (e.g., ACA-111-O2O1)
                    match = re.search(r'([A-Z]{3}-\d{3}[A-Z]*)-([A-Z0-9]+)', line_text)
                    
                    if match:
                        full_id = match.group(0)
                        class_num = match.group(1)
                        section = match.group(2)
                        
                        # Find all dates
                        dates = re.findall(r'\d{1,2}/\d{1,2}/\d{4}', line_text)
                        course_dates = " - ".join(dates) if dates else ""

                        # --- ADVANCED CLEANING LOGIC ---
                        instructor = ""
                        course_name = ""
                        location = ""
                        
                        if dates:
                            # 1. EXTRACT LOCATION (Everything after the last date)
                            # Example: "10/20/2025 12/17/2025 2nd 8wk Online" -> "2nd 8wk Online"
                            last_date = dates[-1]
                            location_part = line_text.split(last_date)[-1].strip()
                            location = location_part

                            # 2. EXTRACT COURSE & INSTRUCTOR (Between Full ID and First Date)
                            first_date = dates[0]
                            content_block = line_text.split(full_id)[1].split(first_date)[0].strip()
                            
                            # Clean out Time patterns and Day patterns
                            clean_block = re.sub(r'\d{1,2}:\d{2}\s?(?:AM|PM)', '', content_block)
                            clean_block = re.sub(r'\b(?:M|T|W|TH|F|MW|TTH|WF)\b', '', clean_block).strip()
                            
                            parts = clean_block.split()
                            if parts:
                                # Iterate backwards to find the credit/flag digit
                                for i in range(len(parts)-1, -1, -1):
                                    if parts[i].isdigit() or parts[i] == 'Y':
                                        if i > 0:
                                            instructor = parts[i-1]
                                            course_name = " ".join(parts[:i-1]).strip()
                                        break
                        
                        # Fallback for Location if logic above is empty
                        if instructor.isdigit() and course_name != 'Practices in Accounting Glance':
                            name_parts = course_name.split()
                            instructor = name_parts[-1]
                            course_name = " ".join(name_parts[:-1]).replace('MTWTHF','').strip()
                        if course_name == 'Practices in Accounting Glance':
                            instructor = ''
                        if course_name == 'Manicure/Nail Technology':
                            course_name = 'Manicure/Nail Technology I'
                            instructor = ''
                        if course_name == '':
                            instructor = ''
                            course_name = 'Animal Assisted Intro'
                            course_dates = '8/18/2025 - 12/17/2025'
                            location = 'Online'
                        course_name = f'{class_num} {course_name.replace('MTWTHF','')}'
                        if course_name == 'ACC-115 College Accounting':
                            course_name = 'ACC-115 College Accounting Glance'
                            instructor = ''
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Course Name": course_name,
                            "Course Description": "",
                            "Class Number": class_num,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": "",
                            "Course Dates": course_dates,
                            "Location": location,
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
        urls = response.xpath('//div[@id="_dynamic_list-33-19530"]/div/a[1]/@href').getall()
        for url in urls:
            yield scrapy.Request(url,headers=self.directory_headers,callback=self.parse_directory_final)

    def parse_directory_final(self,response):
        dept = response.xpath('//span[@id="span-58-19516"]/text()').get('').strip()
        tit = response.xpath('//span[@id="span-59-19516"]/text()').get('').strip()

        if dept and tit:
            title = f'{dept}, {tit}'
        elif dept:
            title = dept
        else:
            title = tit

        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": response.url,
        "Name": response.xpath('//h1[@class="ct-headline"]/span/text()').get('').strip(),
        "Title": re.sub(r'\s+',' ',title),
        "Email": response.xpath('//span[@id="span-66-19516"]/text()').get('').strip(),
        "Phone Number": response.xpath('//span[@id="span-65-19516"]/text()').get('').strip(),
        })

    # PARSE CALENDAR
    def parse_calendar(self,response):
        """
        Parse calendar using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """     
        blocks = response.xpath('//tbody/tr')
        for block in blocks:
            description = block.xpath('.//td[2]/text()').get('').strip()
            term = ''.join(block.xpath('.//parent::tbody/parent::table//th[1]//text()').getall()).strip()
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_source_url,
                "Term Name": re.sub(r'\s+',' ',term),
                "Term Date": block.xpath('.//td[1]/text()').get('').strip(),
                "Term Date Description": re.sub(r'\s+',' ',description),
            })

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
        