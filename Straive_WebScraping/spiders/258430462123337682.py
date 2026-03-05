import io
import re
import scrapy
import tabula
import requests
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class SkylinecollegeSpider(scrapy.Spider):
    name = "skylinecollege"
    institution_id = 258430462123337682
    
    # Course page URL
    course_url = "https://webschedule.smccd.edu/"
    
    # Directory page URL
    directory_source_url = "https://skylinecollege.edu/library/about/staff.php"
    
    # Academic calendar page URL
    calendar_url = "https://skylinecollege.edu/academics/academiccalendar.php"

   
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
        # Initialize list to store parsed course records
        course_rows = []
        
        # Build absolute URLs for all Skyline College schedule PDFs
        pdf_urls = [f"https://webschedule.smccd.edu{url}" for url in response.xpath('//h4[contains(text(),"SKYLINE COLLEGE")]/following-sibling::a/@href').getall()]
        
        # Iterate through each PDF URL
        for pdf_url in pdf_urls[:None]:
            term_response= requests.get(pdf_url)
            
            # Load PDF content into memory
            pdf_content = io.BytesIO(term_response.content)
            
            # Extract all tables from the PDF
            tables = tabula.read_pdf(pdf_content, pages="all", multiple_tables=True)
            
            # Combine all extracted tables into a single DataFrame
            df = pd.concat(tables)
            
            # Iterate through each course row (skip header row)
            for _, row in df[1:].iterrows():
                
                # Extract individual course fields
                crn = row[0]
                sub = row[1]
                crse = row[2]
                sec = row[3]
                title = row[4]
                dates = row[5]
                instructor = row[8]
                full_title = f"{sub} {crse} {title}"
                
                # Filter out invalid or header rows
                if "nan" not in full_title and "Instructor" != instructor:
                    course_rows.append({ 
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": pdf_url,
                        "Course Name": re.sub('\s+',' ',full_title).strip(),
                        "Course Description": '',
                        "Class Number": crn,
                        "Section": sec,
                        "Instructor": instructor,
                        "Enrollment": '',
                        "Course Dates": dates,
                        "Location": '',   
                        "Textbook/Course Materials": '',
                    })
                    
        # Convert collected course records into a DataFrame
        course_df = pd.DataFrame(course_rows)
        
        # Save the final course data
        save_df(course_df,    self.institution_id, "course")
        

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
        # List to store extracted staff records
        directory_rows = []
        
        # Select all directory entry blocks
        blocks = response.xpath('//div[@id="MainContent"]/div')
        
        # Iterate through each directory block
        for block in blocks:
            name = block.xpath('.//h4[@class="dir-name"]/text()').get('').replace(',','').strip()
            if not name:
                name = block.xpath('.//span/text()').get('').strip()
            
            dir_title = ''
            check = block.xpath('./h4[@class="dir-name"]//text()').get('').strip()
            if check:
                title = block.xpath('.//span[@class="dir-title"]/text()').get('').strip()
                div = block.xpath('.//span[@class="dir-division"]/text()').get('').strip()
                dept = block.xpath('.//span[@class="dir-department"]/text()').get('').strip()
                dir_title = f"{title}, {div} — {dept}" 
                
            else:
                dir_title = re.sub('\s+',' ',", ".join(block.xpath('.//div[@class="col-xs-12 col-md-9"]//p//text()').getall())).strip()
                
            # Append extracted staff data to the results list
            directory_rows.append(
                {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.directory_source_url,
                    "Name": name,
                    "Title": re.sub('\s+',' ',dir_title).strip(),
                    "Email": block.xpath('.//div[@class="dir-emailaddress"]//text()').get('').strip(),
                    "Phone Number": block.xpath('.//div[@class="dir-phone"]/text()').get('').strip(),
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
        # Initialize list to store parsed calendar records
        calendar_rows = []
        
        # Extract all term names
        term_names = response.xpath('//div[@class="col-xs-12"]//h3/text()').getall()
        
        # Iterate through each academic term
        for term_name in term_names:
            
            # Locate table rows associated with the current term
            blocks = response.xpath(f'//h3[contains(text(),"{term_name}")]//ancestor::table[1]/tbody/tr[not(@class)]')
            
            # Iterate through each calendar row
            for block in blocks:
                
                td_count = len(block.xpath('./td'))
            
                if 3 == td_count:
                    month = block.xpath('./td[1]//text()').get('').strip()
                    day = block.xpath('./td[2]//text()').get('').strip()
                    term_date = f"{month} {day}"
                    event_name = re.sub('\s+',' ',block.xpath('./td[3]//text()').get('')).strip()
                elif 2 == td_count:
                    term_date = block.xpath('./td[1]//text()').get('').strip()
                    event_name = re.sub('\s+',' ',block.xpath('./td[2]//text()').get('')).strip()
                else:
                    term_date = ''
                    event_name = ''
                    
                # Append parsed calendar event to results list
                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": term_name,
                    "Term Date": term_date,
                    "Term Date Description": event_name,
                })
    
        # Convert collected rows into a DataFrame
        calendar_df = pd.DataFrame(calendar_rows)
        
        # Save the final calendar data
        save_df(calendar_df,  self.institution_id, "calendar")
