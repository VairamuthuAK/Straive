import re
import os
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from datetime import datetime
from urllib.parse import urlparse
from inline_requests import inline_requests


class CuchicagoSpider(scrapy.Spider):
    name = "cuchicago"
    institution_id = 258435242140919776

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://www.cuchicago.edu/academics/academic-resources/registrar/class-schedules/"
    directory_source_url = "https://www.cuchicago.edu/general-information/faculty-staff-directory/"
    calendar_url = "https://www.cuchicago.edu/academics/academic-resources/academic-calendar/"

   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)


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
        # STEP 1: Initialize list to store extracted courses
        course_rows = []
        
        # Extract PDF URLs and corresponding term names
        course_pdf_urls = response.xpath('//div[@class="wysiwyg"]//li/a/@href').getall()
        course_term_names = response.xpath('//div[@class="wysiwyg"]//li/a/text()').getall()
        
        # STEP 2: Loop through each course PDF
        for course_pdf_url, course_term_name in zip(course_pdf_urls, course_term_names):
            
            # Only process links that represent course PDFs
            if "Courses" in course_term_name:
                
                # STEP 3: Download the PDF
                pdf_response = requests.get(course_pdf_url, timeout=30)
                pdf_bytes = BytesIO(pdf_response.content)
                print("✅ PDF downloaded successfully")
                
                # STEP 4: Extract all text lines from the PDF
                all_lines = []

                with pdfplumber.open(pdf_bytes) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            all_lines.extend(text.split("\n"))

                print(f"✅ Total lines extracted: {len(all_lines)}")
                
                # STEP 5: Group lines into complete course records
                courses = []
                
                current = None

                # Pattern identifying the start of a new course
                course_start = re.compile(r'^[A-Z]{2,4}-\d{4}\b')

                # Lines that should be skipped
                skip_patterns = (
                    "Undergraduate Schedule",
                    "Graduate Schedule"
                    "Course Title",
                    "Last Updated",
                    "CRN=",
                    "Fee column",
                )

                for line in all_lines:
                    line = re.sub(r"\s+", " ", line).strip()

                    # Skip empty and header lines
                    if not line or line.startswith(skip_patterns):
                        continue

                    # Skip department headers
                    if re.match(r'^[A-Z]{2,4}\s+[A-Za-z]', line):
                        continue

                    # Detect beginning of a new course
                    if course_start.match(line):
                        if current:
                            courses.append(current)
                        current = line
                    else:
                        if current:
                            current += " " + line
                            
                # Add last course if exists
                if current:
                    courses.append(current)

                print(f"✅ Total courses found: {len(courses)}")
                
                # STEP 6: Define extraction patterns
                DATE_PATTERN = re.compile(r'\d{2}/\d{2}\s*-\s*\d{2}/\d{2}')
                SEAT_PATTERN = re.compile(r'\d+/\d+')
                INSTRUCTOR_PATTERN = re.compile(r'([A-Z]\.\s*[A-Za-z\-]+)$')
                INSTRUCTOR_PATTERN1 = re.compile(r'([A-Z]\.\s*[A-Za-z\-]+)')
                CAMPUS_VALUES = [
                    "River Forest",
                    "Asynchronou",
                    "Synchronous",
                    "Off Campus",
                    "Hybrid",
                    "Internship"
                ]

                # STEP 7: Extract structured data from each course line
                for text in courses:
                    tokens = text.split()

                    try:
                        # Extract CRN (5-digit number)
                        crn = next(t for t in tokens if t.isdigit() and len(t) == 5)
                        crn_index = tokens.index(crn)

                        section = tokens[crn_index - 2]
                        title = " ".join(tokens[0:crn_index - 2])

                        seats = SEAT_PATTERN.search(text)
                        seats = seats.group() if seats else ""

                        dates = DATE_PATTERN.search(text)
                        dates = dates.group() if dates else ""

                        campus = ""
                        for c in CAMPUS_VALUES:
                            if c in text:
                                campus = c
                                break
                            
                        inst_match = INSTRUCTOR_PATTERN.search(text)
                        instructor = inst_match.group(1) if inst_match else ""
                        if not instructor:
                            inst_match = INSTRUCTOR_PATTERN1.search(text)
                            instructor = inst_match.group(1) if inst_match else ""
                            
                        # Fix truncated names
                        if "A. O" == instructor:
                            instructor = "A. O'Brien"
                        if "K. O" == instructor:
                            instructor = "K. O'Mara"
                        if "C. O" == instructor:
                            instructor = "C. O'Hara"
                           
                        # STEP 8: Store extracted course record 
                        course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": course_pdf_url,
                            "Course Name": title,
                            "Course Description": '',
                            "Class Number": crn,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": seats,
                            "Course Dates": dates,
                            "Location": campus,   
                            "Textbook/Course Materials": '',
                        })
                        
                    except Exception:
                        # Skip malformed course rows
                        pass
                        
        # STEP 9: Save extracted course data  
        course_df = pd.DataFrame(course_rows)
        save_df(course_df,    self.institution_id, "course")
        
    @inline_requests
    def parse_directory(self, response):
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
        # STEP 1: Initialize storage for directory records
        directory_rows = []
        
        # Timestamp used by "Load More" endpoint (cache busting)
        ms_str = str(int(datetime.now().timestamp() * 1000))
        
        # STEP 2: Parse initial directory page entries
        blocks = response.xpath('//div[@id="load-more-container"]/div')
        for block in blocks:
            # Extract profile URL
            url = block.xpath('.//h3[@class="profile-card__name"]/a/@href').get('').strip()
            if "http" not in url:
                # Convert relative URLs to absolute
                url = f"https://www.cuchicago.edu{url}"
            name = block.xpath('.//h3[@class="profile-card__name"]/a/text()').get('').strip()
            title = block.xpath('.//*[@class="profile-card__title"]/text()').get('').strip()
            email = block.xpath('.//div[@class="contact-grid__icon email-icon"]/following-sibling::a/@href').get('').replace('mailto:','').strip()
            phone = block.xpath('.//div[@class="contact-grid__icon phone-icon"]/following-sibling::a/@href').get('').replace('tel:','').strip()
            
            # Remove placeholder or invalid values
            email, phone = ('' if '#' in email else email), ('' if '#' in phone else phone)
            
            # Append extracted staff data to the results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": url,
                        "Name": name,
                        "Title": title,
                        "Email": email,
                        "Phone Number": phone,
                    }
                )
            
        # STEP 3: Detect total number of paginated pages
        page_counts = response.xpath('//form[@id="faculty-listing-form" ]/@data-pages').get('').strip() 
        print(page_counts)
        
        # STEP 4: Loop through "Load More" pages if available
        if page_counts:
            for page in range(1, int(page_counts)+2,1):
        
                page_url = f"https://www.cuchicago.edu/general-information/faculty-staff-directory/LoadMore?p={str(page)}&q=&college=%20&primary=%20&_={ms_str}"
                headers = {
                'Accept': 'text/html, */*; q=0.01',
                'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
                'Content-Type': 'application/html; charset=utf-8',
                'Referer': 'https://www.cuchicago.edu/general-information/faculty-staff-directory/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                }
                
                # Request additional directory page
                page_response = yield scrapy.Request(page_url,headers=headers)
                blocks1 = page_response.xpath('//body/div')
                
                # STEP 5: Parse profiles from paginated results
                for block1 in blocks1:
                    url = block1.xpath('.//h3[@class="profile-card__name"]/a/@href').get('').strip()
                    if "http" not in url:
                        url = f"https://www.cuchicago.edu{url}"
                    name = block1.xpath('.//h3[@class="profile-card__name"]/a/text()').get('').strip()
                    title = block1.xpath('.//*[@class="profile-card__title"]/text()').get('').replace('--','-').strip()
                    email = block1.xpath('.//div[@class="contact-grid__icon email-icon"]/following-sibling::a/@href').get('').replace('mailto:','').strip()
                    phone = block1.xpath('.//div[@class="contact-grid__icon phone-icon"]/following-sibling::a/@href').get('').replace('tel:','').strip()
                    email, phone = ('' if '#' in email else email), ('' if '#' in phone else phone)
                   
                    # Append extracted staff data to the results list
                    directory_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": url,
                            "Name": name,
                            "Title": title,
                            "Email": email,
                            "Phone Number": phone,
                        }
                    )
                    
        # STEP 6: Save directory data
        directory_df = pd.DataFrame(directory_rows)
        save_df(directory_df, self.institution_id, "campus")
        
        
    @inline_requests
    def parse_calendar(self, response):
        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        # STEP 1: Initialize list to store calendar records
        calendar_rows = []
        
        # STEP 2: Extract all academic calendar PDF URLs
        cal_pdf_urls = response.xpath('//div[@data-block-type="collapsibletext"]//a/@href').getall()
        for cal_pdf_url in cal_pdf_urls:
            
            # Build absolute PDF URL
            cal_pdf_url = f"https://www.cuchicago.edu{cal_pdf_url}"
            
            # Extract filename from URL
            filename = os.path.basename(urlparse(cal_pdf_url).path)

            # STEP 3: Extract academic years from filename
            years = re.findall(r'(\d{4})', filename)

           # Generate possible term names automatically
            seasons = ['Summer', 'Fall', 'Spring']
            valid_terms = [f"{season} {year}" for year in years for season in seasons]

            # STEP 4: Initialize parsing state variables
            next_line_status = "No"
            next_line_count = 1
            first_line = ''
            second_line = ''
            summer_date = ''
            fall_date = ''
            spring_date = ''

            term_name1 =None
            term_name2= None
            term_name3=None
            sub_term_name = None
            
            # STEP 5: Download and read PDF text
            response = requests.get(cal_pdf_url)
            pdf_file = BytesIO(response.content)
            text = ""
            with pdfplumber.open(pdf_file) as pdf:
                for page in pdf.pages:
                    text += page.extract_text() + "\n"

            # STEP 6: Process PDF text line by line
            for line in text.splitlines():
                print(line)
                
                # Skip non-relevant header lines
                if "Undergraduate" in line or "Academic Calendars" in line or "Graduate" in line:
                    print(line)
                    continue
                
                # STEP 7: Detect term names (Summer/Fall/Spring)
                if any(term in line for term in valid_terms):
                    print(line)
                    terms = re.findall(r'\b(?:Summer|Fall|Spring)\s+\d{4}\b', line)
                    
                    # Assign up to three term names
                    term_name1, term_name2, term_name3 = (terms + [None]*3)[:3]
                    continue
                
                # STEP 8: Detect sub-term (e.g., 7-Week Term)
                if "-Week Term" in line:
                    print(line)
                    sub_term_name = line
                    continue
                    
                # Stop parsing once census section begins
                if "Census" in line:
                    break
                
                # STEP 9: Match standard single-line event rows
                else:
                    
                    match = re.match(
                        r"^(.*?)(\d{1,2}/\d{1,2}(?:/\d{4})?)\s+"
                        r"(\d{1,2}/\d{1,2}(?:/\d{4})?)\s+"
                        r"(\d{1,2}/\d{1,2}(?:/\d{4})?)$",
                        line
                    )
                    if match and "No" == next_line_status:
                        print(line)
                        event, summer, fall, spring = match.groups()
                        if event and summer and fall and spring:
                            # STEP 10: Store single-line event rows
                            if summer:
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_pdf_url,
                                    "Term Name": term_name1,
                                    "Term Date": summer,
                                    "Term Date Description": f"{sub_term_name} - {event}",
                                })
                            if fall:
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_pdf_url,
                                    "Term Name": term_name2,
                                    "Term Date": fall,
                                    "Term Date Description": f"{sub_term_name} - {event}",
                                })
                            if spring:
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_pdf_url,
                                    "Term Name": term_name3,
                                    "Term Date": spring,
                                    "Term Date Description": f"{sub_term_name} - {event}",
                                })
                                
                    # STEP 11: Handle multi-line wrapped event rows
                    else:
                        next_line_status = "Yes"
                        
                        # First wrapped line → event text
                        if next_line_count == 1:
                            first_line = line
                            next_line_count +=1
                            
                        # Second wrapped line → contains dates
                        elif next_line_count == 2:
                            next_line_count +=1
                            match = re.match(
                                r"^(.*?)(\d{1,2}/\d{1,2}(?:/\d{4})?)\s+"
                                r"(\d{1,2}/\d{1,2}(?:/\d{4})?)\s+"
                                r"(\d{1,2}/\d{1,2}(?:/\d{4})?)$",
                                line
                            )
                            if match:
                                print(line)
                                event, summer, fall, spring = match.groups()
                                summer_date = summer
                                fall_date = fall
                                spring_date = spring
                            
                        # Third wrapped line → remaining description
                        elif next_line_count == 3:
                            second_line = line
                            if summer_date:
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_pdf_url,
                                    "Term Name": term_name1,
                                    "Term Date": summer_date,
                                    "Term Date Description": f"{sub_term_name} - {first_line} {second_line}",
                                })
                            if fall_date:
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_pdf_url,
                                    "Term Name": term_name2,
                                    "Term Date": fall_date,
                                    "Term Date Description": f"{sub_term_name} - {first_line} {second_line}",
                                })
                            if spring_date:
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_pdf_url,
                                    "Term Name": term_name3,
                                    "Term Date": spring_date,
                                    "Term Date Description": f"{sub_term_name} - {first_line} {second_line}",
                                })
                                
                            # Reset state for next event
                            next_line_count = 1
                            next_line_status = "No"
                            first_line = ''
                            second_line = ''
                            summer_date = ''
                            fall_date = ''
                            spring_date = ''
            
        # STEP 12: Save extracted calendar data
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
