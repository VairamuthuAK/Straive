import re
import json
import scrapy
import PyPDF2
import requests
import pandas as pd
from ..utils import *
from io import BytesIO
from inline_requests import inline_requests


class NtccSpider(scrapy.Spider):
    name = "ntcc"
    institution_id = 258451912431527892

    # Course page URL
    course_url = "https://www.ntcc.edu/academics/schedule-classes"
    
    # Directory page URL
    directory_source_url = "https://www.ntcc.edu/about-us/human-resources/directory"

    # Academic calendar page URL
    calendar_url = "https://www.ntcc.edu/academics/academic-calendar"

   
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
        
        # All three (default)
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
        # Initialize list to store parsed course records
        course_rows = []
        
        # Extract all term URLs
        term_urls = response.xpath('//section[@id="block-currentlyenrollingschedule"]//ul/li/a/@href').getall()
        
        # Iterate through each term URL (excluding the last link)
        for term_url in term_urls[:-1]:
            
            # Define request headers for term page
            headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
            'referer': 'https://www.ntcc.edu/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            }
            inn_response = yield scrapy.Request(term_url, headers=headers, dont_filter=True)
            port_location = re.findall(r'portletLocation\s*\=\s*\'(.*?)\'', inn_response.text)[0].strip()
            
            # Extract portlet location and ID required for the POST request
            port_id = re.findall(r'portletId\s*\=\s*\'(.*?)\'', inn_response.text)[0].strip()
            
            post_url = f"https://myeagle.ntcc.edu{port_location}Query.ashx"
            
            # Build POST payload
            post_payload = f"portletId={port_id}&action=RunQuery"
            
            # Define headers for the AJAX POST request
            post_headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://myeagle.ntcc.edu',
            'referer': term_url,
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            }
            # Send POST request to retrieve course data
            post_response = yield scrapy.Request(post_url,method="POST", body=post_payload, headers=post_headers, dont_filter=True)
            
            # Parse JSON response
            json_data = json.loads(post_response.text)
            blocks = json_data['d']['data']
            counts = len(blocks)
            
            # Iterate through each course record
            for i, block in enumerate(blocks, start=1):
                print(f"{i}/{counts}")
                
                start = block[9]
                end = block[11]
                location = block[4] if isinstance(block[4], str) else ""
                enroll = block[7]
                capacity = block[6]
                if enroll and capacity:
                    enrollment = f"{enroll}/{capacity}"
                elif not enroll and capacity:
                    enrollment = f"0/{capacity}"
                else:
                    enrollment = ''
                class_no = re.sub('\s+', ' ',block[0])
                if re.search(r'(.*?\d+)', class_no):
                    class_num = re.findall(r'(.*?\d+)', class_no)[0].strip()
                else:
                    class_num = ''
                    
                if re.search(r'\d+(.*)', class_no):
                    sec_num = re.findall(r'\d+(.*)', class_no)[0].strip()
                else:
                    sec_num = ''
                name = re.sub('\s+', ' ',block[1])
                course_name = f"{class_num} {name}"
                
                # Append parsed course data to results list
                course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": term_url,
                    "Course Name": course_name.strip(),
                    "Course Description": '',
                    "Class Number": class_num,
                    "Section": sec_num,
                    "Instructor": re.sub('\s+', ' ',block[2]),
                    "Enrollment":enrollment,
                    "Course Dates": f"{start} - {end}",
                    "Location": re.sub('\s+', ' ',location.strip()),   
                    "Textbook/Course Materials": '',
                })
        # Convert collected course records into a DataFrame
        course_df = pd.DataFrame(course_rows)
        
        # Save the final course data
        save_df(course_df, self.institution_id, "course")
        

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
        
        # Initialize list to store parsed directory records
        directory_rows = []
       
       # Select all table rows within the employee directory block
        blocks = response.xpath('//section[@id="block-views-block-employee-directory-block-1"]//table/tbody/tr')
        
        # Iterate through each employee row
        for block in blocks:
            
            title = re.sub('\s+',' ',block.xpath('./td[5]//text()').get('')).strip()
            dept = re.sub('\s+',' ',block.xpath('./td[4]//text()').get('')).strip()
            
            # Combine title and department into a single field when available
            if title and dept:
                title_main = f"{title}, {dept}"
            elif title:
                title_main = title
            elif dept:
                title_main = dept
            else:
                title_main = ''
                
            # Append extracted staff data to the results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_source_url,
                        "Name": block.xpath('./td[1]/a/text()').get('').strip(),
                        "Title":title_main,
                        "Email": re.sub('\s+',' ',block.xpath('./td[3]//text()').get('')).strip(),
                        "Phone Number": block.xpath('./td[2]//text()').get('').strip(),
                    }
                )
            
        # Convert collected records into a DataFrame
        directory_df = pd.DataFrame(directory_rows)
        
        # Save the DataFrame using a custom helper function
        save_df(directory_df, self.institution_id, "campus")
        
        
   
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
        # Initialize list to store parsed calendar rows
        calendar_rows = []
        
        # Extract all PDF URLs
        pdf_urls = response.xpath('//div[@class="region region-content"]//div[@class="content"]//a[@target="_blank"]/@href').getall()
        
        # Iterate through each PDF URL found on the page
        for pdf_url in pdf_urls:
            
            # Only process PDFs related to the 2025–2026 academic year
            if "2025" in pdf_url and "2026" in pdf_url:
                
                # Convert relative URL to absolute URL
                pdf_url = f"https://www.ntcc.edu{pdf_url}"
                
                pdf_response = requests.get(pdf_url)
                pdf_response.raise_for_status() # raise error if request failed

                # Load PDF content into memory
                pdf_bytes = BytesIO(pdf_response.content)

                # Read PDF and extract text from each page
                reader = PyPDF2.PdfReader(pdf_bytes)
                all_text = []

                for page_num in range(len(reader.pages)):
                    page = reader.pages[page_num]
                    text = page.extract_text()
                    if text:
                        all_text.append(text)
                
                # Combine all extracted text into a single string
                full_text = "\n\n".join(all_text)
                
                # Initialize term name placeholder
                term_name = ''
                
                # Days of the week used to identify event rows
                days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
                
                # Parse each line of text from the PDF
                for line_number, line in enumerate(full_text.splitlines(), start=1):
                    line = line.strip()
                    
                    # Skip empty lines
                    if not line:
                        continue
                    
                    # Identify lines that contain event dates (day names)
                    if any(day in line for day in days):
                        print("Day found:", line)
                       
                        # Extract event name (text before dotted separator)
                        if re.search(r'(.*?)\.{4}', line):
                            event_name = re.findall(r'(.*?)\.{4}', line)[0].replace('.','').strip()
                        else:
                            event_name = ''
                        if re.search(r'\.{4}(.*)', line):
                            date = re.findall(r'\.{4}(.*)', line)[0].replace('.','').strip()
                        else:
                            date = ''
                            
                        # Append parsed event data to results
                        calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Term Name": term_name,
                            "Term Date": date,
                            "Term Date Description": event_name,
                        })
                    # Non-date lines are assumed to represent term names
                    else:
                        term_name = re.sub('\s+',' ',line)    
        # Convert collected rows into a DataFrame
        calendar_df = pd.DataFrame(calendar_rows)
        
        # Save the final calendar data
        save_df(calendar_df,  self.institution_id, "calendar")
