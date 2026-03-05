import re
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from inline_requests import inline_requests


def clean_string(data):
    return re.sub(r"\s+", " ", data.replace("\t", "").replace("\n", " ").replace("\\r\\n", "")).strip()


class AicSpider(scrapy.Spider):
    name = "aic"
    institution_id = 258428019952084952

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://www.aic.edu/academics/registrar/course-offerings/"
    directory_source_url = "https://www.aic.edu/contact/phonebook-directory/department-search/"
    calendar_url = "https://www.aic.edu/academics/academic-calendar/"

   
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

    # Parse methods UNCHANGED from your original
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
        # Initialize list to store extracted course records
        course_rows = []
        
        # STEP 1: Extract PDF URL
        pdf_url = response.xpath('//h2[contains(text(),"Current Course Offerings")]/following-sibling::a[1]/@href').get('').strip()
        
        # Download the PDF file (blocking request)
        response = requests.get(pdf_url)
        pdf_bytes = BytesIO(response.content)

        rows = []
        
        # STEP 2: Extract raw text from PDF
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                lines = text.split("\n")

                for line in lines:
                    if "Begin End Course Title" in line:
                        continue
                    if "accurate as of" in line.lower():
                        continue
                    if line.strip() == "":
                        continue
                    rows.append(line.strip())
                    
        # STEP 3: Parse each extracted line using regex
        for line in rows:
            
                start_date = re.findall(r'(\d+\/\d+\/\d+)', line)[0]
                end_date = re.findall(r'(\d+\/\d+\/\d+)', line)[1]
                class_no = re.findall(r'\s(\d{3,5})\s', line)[0]
                sub = re.findall(r'\s(\w+)\s\d{3,5}\s', line)[0]
                sec = re.findall(r'\s\d{3,5}\s(\d+)\s', line)[0]
                title = re.findall(r'\s\d{3,5}\s\d+\s([\w\W]*?)\s*\d+', line)[0].strip()
                instructor = re.findall(r'Online\s*Lecture(.*)', line)[0].strip()

                # STEP 4: Clean & Fix Instructor Name
                first_name = re.findall(r'(^[A-Z].*?)[A-Z]', instructor)[0].replace(' ','')
                if "Ravens" in first_name:
                    first_name = "Ravens-Seger"
                if "Mac" in first_name:
                    first_name = "MacDonald"

                second_name = re.findall(r'^[A-Z].*?([A-Z].*)', instructor)[0].replace(' ','')
                if "Seger" in second_name:
                    second_name = "Robert"
                if "Donald" in second_name:
                    second_name = "Melissa"

                # STEP 5: Clean & Correct Course Title
                full_title = ''
                if title:
                    if "U . S" in title:
                        full_title = "U.S. History to"
                    elif any(word in title for word in ["of", "for", "to"]):
                        # full_title = correct_sentence(title)
                        full_title = re.sub(r'\b([A-Za-z])(?:\s+)(?=[A-Za-z])', r'\1', title)
                    elif "M T E L" in title:
                        full_title = "MTEL Preparation"
                    else:
                        first_part = re.findall(r'(^[A-Z].*?)[A-Z]', title)[0].replace(' ','')
                        second_part = re.findall(r'^[A-Z].*?([A-Z].*)', title)[0]
                        full_title = f"{first_part} {second_part}"

                # STEP 6: Append Structured Course Record
                course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": pdf_url,
                    "Course Name": f"{sub} {class_no} {full_title.replace('(','').strip()}",
                    "Course Description": '',
                    "Class Number": f"{sub} {class_no}",
                    "Section": sec,
                    "Instructor": f"{second_name} {first_name}",
                    "Enrollment": '',
                    "Course Dates": f"{start_date} - {end_date}",
                    "Location": '',   
                    "Textbook/Course Materials": '',
                })
                
        # STEP 7: Convert to DataFrame & Save
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
        
        # STEP 1: Initialize list to store staff records
        directory_rows = []
        
        # STEP 2: Extract staff blocks on first page
        # Each <p> following the form container represents a staff member
        blocks = response.xpath('//div[@id="frm_form_3_container"]/following-sibling::p')
        for block in blocks:
            name = block.xpath('./strong/text()').get('').strip()
            if name:
                details = block.xpath('.//text()').getall()
                detail_count = len(details)
                phone = ''
                email = ''
                title = ''
                full_title = ''
                
                # STEP 3: Assign details based on number of text items
                # This handles inconsistent formatting between staff entries
                if 5 == detail_count:
                    phone = details[4]
                    email = details[2]
                    title = details[1]
                    dept = details[3]
                    full_title = f"{title} - {dept}"
                elif 4 == detail_count:
                    email = details[2]
                    title = details[1]
                    dept = details[3]
                    full_title = f"{title} - {dept}"
                elif 3 == detail_count:
                    email = details[2]
                    title = details[1]
                    
                # STEP 4: Append extracted staff record to results
                directory_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.directory_source_url,
                            "Name": name,
                            "Title": full_title,
                            "Email": email,
                            "Phone Number": phone,
                        }
                    )
                
        # STEP 5: Handle pagination
        # Look for "next page" link and repeat extraction
        next_page = response.xpath('//a[@class="next"]/@href').get('').strip()
        while next_page:
            next_page_url = f"https://www.aic.edu{next_page}"
            next_selector = yield scrapy.Request(next_page_url)
            blocks1 = next_selector.xpath('//div[@id="frm_form_3_container"]/following-sibling::p')
            for block1 in blocks1:
                name = block1.xpath('./strong/text()').get('').strip()
                if name:
                    
                    details = block1.xpath('.//text()').getall()
                    detail_count = len(details)
                    phone = ''
                    email = ''
                    title = ''
                    full_title = ''
                    if 5 == detail_count:
                        
                        phone = details[4]
                        email = details[2]
                        title = details[1]
                        dept = details[3]
                        full_title = f"{title} - {dept}"
                    elif 4 == detail_count:
                        email = details[2]
                        title = details[1]
                        dept = details[3]
                        full_title = f"{title} - {dept}"
                    elif 3 == detail_count:
                        email = details[2]
                        title = details[1]
                        
                    # Append extracted staff data to the results list
                    directory_rows.append(
                            {
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": next_page_url,
                                "Name": name,
                                "Title": full_title,
                                "Email": email,
                                "Phone Number": phone,
                            }
                        )
            next_page = next_selector.xpath('//a[@class="next"]/@href').get('').strip()
            
        # STEP 6: Convert collected records into a DataFrame
        directory_df = pd.DataFrame(directory_rows)
        
        # STEP 7: Save DataFrame using helper function
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
        
        # STEP 2: Extract all term headers (h2)
        # Only consider current/future terms (skip past terms)
        term_names = response.xpath('//div[@class="container-fluid"]//h2/text()').getall()
        for name in term_names:
            if "Past" not in name:
                terms = response.xpath(f'//h2[contains(text(),"{name}")]/following-sibling::div[1]//h4/strong/text()').getall()
                for term in terms:
                    blocks = response.xpath(f'//strong[contains(text(),"{term}")]/parent::h4//following-sibling::table[1]/tbody/tr')
                    for block in blocks:
                        des = block.xpath('./td[1]/text()').get('').strip()
                        date = block.xpath('./td[2]/text()').get('').strip()
                        if "* Missed classroom" not in des:
                            if des or date:
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": self.calendar_url,
                                    "Term Name": clean_string(term),
                                    "Term Date": clean_string(date),
                                    "Term Date Description": clean_string(des),
                                })
                                
        # STEP 3: Extract additional term events (e.g., 8-Week School of Education sessions)
        # Some events may be in a second table under specific sub-divs
        for name in term_names:
            if "Past" not in name:
                terms = response.xpath(f'//h2[contains(text(),"{name}")]/following-sibling::div[1]//h4/strong/text()').getall()
                for term in terms:
                    blocks1 = response.xpath(f'//h2[contains(text(),"{name}")]//following-sibling::div[1]//div[contains(text(),"8 Week School of Education")]/parent::div//h4/strong[contains(text(),"{term}")]/parent::h4/following-sibling::table[2]//tbody/tr')
                    for block in blocks1:
                        des = block.xpath('./td[1]/text()').get('').strip()
                        date = block.xpath('./td[2]/text()').get('').strip()
                        if "* Missed classroom" not in des:
                            if des or date:
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": self.calendar_url,
                                    "Term Name": clean_string(term),
                                    "Term Date": clean_string(date),
                                    "Term Date Description": clean_string(des),
                                })
        
        # STEP 4: Convert collected calendar events into a DataFrame
        calendar_df = pd.DataFrame(calendar_rows)
        
        # STEP 5: Save DataFrame using custom helper
        save_df(calendar_df,  self.institution_id, "calendar")
