import re
import json
import scrapy
import pandas as pd
from ..utils import *
from parsel import Selector
from inline_requests import inline_requests


def decode_cf_email(encoded_string):
    # First two hex characters are the key
    key = int(encoded_string[:2], 16)
    
    # Decode the rest
    decoded = ""
    for i in range(2, len(encoded_string), 2):
        byte = int(encoded_string[i:i+2], 16)
        decoded += chr(byte ^ key)
    
    return decoded


class GeorgetowncollegeSpider(scrapy.Spider):
    name = "georgetowncollege"
    institution_id = 258422710059689946

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://catalog.georgetowncollege.edu/classes"
    directory_source_url = "https://www.georgetowncollege.edu/academics/Faculty-directory"
    calendar_url = "https://catalog.georgetowncollege.edu/academic-calendar"
    
    course_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'referer': 'https://catalog.georgetowncollege.edu/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        }
   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, headers=self.course_headers, callback=self.parse_course, dont_filter=True)
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
        # List to store all scraped course records
        course_rows = []
        
        # Custom headers to mimic a real browser (required for AJAX + detail pages)
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
            'referer': 'https://catalog.georgetowncollege.edu/classes',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
            }
        selector = Selector(text=response.text)
        
        # Select all course blocks from the first page
        blocks = selector.xpath('//div[@class="view-content"]//div[@class="views-row-wrapper"]')
        
        for block in blocks:
            detail_url = block.xpath('.//div[@class="col-12 col-md-2"]/a/@href').get('').strip()
            detail_url = f"https://catalog.georgetowncollege.edu{detail_url}"
            
            # Request detail page
            detail_res = yield scrapy.Request(detail_url, headers=headers, dont_filter=True)
            
            # Extract detailed course information and append to list
            course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": detail_url,
                "Course Name": " ".join(detail_res.xpath('//h1//text()').getall()).strip(),
                "Course Description": " ".join(detail_res.xpath('//div[@class="field field--name-field-description field--type-text-long field--label-hidden field__item"]//text()').getall()).strip(),
                "Class Number": detail_res.xpath('//h1/span/text()').get('').strip(),
                "Section": '',
                "Instructor": '',
                "Enrollment": '',
                "Course Dates": '',
                "Location": '',   
                "Textbook/Course Materials": '',
            })

        # Extract dynamic AJAX parameters required for pagination
        libraries = re.findall(r'"libraries":"(.*?)"', response.text)[0] if re.search(r'"libraries":"(.*?)"', response.text) else ''
        views_dom_id = re.findall(r'\"views_dom_id\:(.*?)\"', response.text)[0] if re.search(r'\"views_dom_id\:(.*?)\"', response.text) else ''
        
        # Loop through paginated AJAX pages (Drupal AJAX pagination)
        for page in range(1, 1000,1):
            
            # Construct AJAX pagination URL
            next_page_url = f"https://catalog.georgetowncollege.edu/views/ajax?_wrapper_format=drupal_ajax&view_name=courses&view_display_id=page_1&view_args=&view_path=%2Fclasses&view_base_path=classes&view_dom_id={views_dom_id}&pager_element=0&page={str(page)}&_drupal_ajax=1&ajax_page_state%5Btheme%5D=georgetown&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D={libraries}"
            
            next_page_res = yield scrapy.Request(next_page_url, headers=headers)
            all_text = "".join(next_page_res.xpath('//textarea//text()').getall())
            json_data = json.loads(all_text)
            data = json_data[1].get('data','').strip()
            data_selector = Selector(text=data)
            blocks = data_selector.xpath('//div[@class="view-content"]//div[@class="views-row-wrapper"]')
            
            # If no blocks found → break pagination loop
            if blocks:
                for block in blocks:
                    detail_url = block.xpath('.//div[@class="col-12 col-md-2"]/a/@href').get('').strip()
                    detail_url = f"https://catalog.georgetowncollege.edu{detail_url}"
                    
                    # Request detail page
                    detail_res = yield scrapy.Request(detail_url, headers=headers, dont_filter=True)
                    
                    course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": detail_url,
                        "Course Name": " ".join(detail_res.xpath('//h1//text()').getall()).strip(),
                        "Course Description": " ".join(detail_res.xpath('//div[@class="field field--name-field-description field--type-text-long field--label-hidden field__item"]//text()').getall()).strip(),
                        "Class Number": detail_res.xpath('//h1/span/text()').get('').strip(),
                        "Section": '',
                        "Instructor": '',
                        "Enrollment": '',
                        "Course Dates": '',
                        "Location": '',   
                        "Textbook/Course Materials": '',
                    })
            else:
                # Stop loop if no more paginated data
                break
                     
        # Convert scraped course rows into a DataFrame and save it
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
        # List to store extracted staff records
        directory_rows = []
        
        # Select all staff profile blocks from the main directory page
        blocks = response.xpath('//div[@class="view-content row"]/div')
        
        # Loop through each staff block
        for block in blocks:
            mail = ''
            dept = " ".join(block.xpath('.//h4/following-sibling::p/text()').getall())
            email_url = block.xpath('.//h5/a/@href').get('').strip()
            encoded_string = email_url.split('#')[-1]

            # Decode Cloudflare-protected email if available
            if encoded_string:
                mail = decode_cf_email(encoded_string)
                
            # Append extracted staff data to the results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_source_url,
                        "Name": block.xpath('.//h4/text()').get('').strip(),
                        "Title": dept,
                        "Email": mail,
                        "Phone Number": '',
                    }
                )
        # Extract the "next page" link (for pagination)
        next_page = response.xpath('//a[@title="Go to next page"]/@href').get('').strip()
        
        # Continue scraping while a next page exists
        while next_page:
            next_page_url = f"https://www.georgetowncollege.edu{next_page}"
            next_page_res = yield scrapy.Request(next_page_url)
            
            mail = ''
            blocks = next_page_res.xpath('//div[@class="view-content row"]/div')
            for block in blocks:
                mail = ''
                dept = " ".join(block.xpath('.//h4/following-sibling::p/text()').getall())
                email_url = block.xpath('.//h5/a/@href').get('').strip()
                encoded_string = email_url.split('#')[-1]
                if encoded_string:
                    mail = decode_cf_email(encoded_string)
                    
                # Append extracted staff data to the results list
                directory_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": next_page_url,
                            "Name": block.xpath('.//h4/text()').get('').strip(),
                            "Title": dept,
                            "Email": mail,
                            "Phone Number": '',
                        }
                    )
            next_page = next_page_res.xpath('//a[@title="Go to next page"]/@href').get('').strip()
            
        # Convert collected records into a DataFrame
        directory_df = pd.DataFrame(directory_rows)
        
        # Save the DataFrame using a custom helper function
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
        # Initialize an empty list to store all calendar row dictionaries
        calendar_rows = []
        
        # Extract all <h4> text values (term names), strip whitespace,
        # and filter out any empty values
        terms = [n.strip() for n in response.xpath('//h4/text()').getall() if n.strip()]
        
        # Loop through each extracted term
        for term in terms:
            blocks = response.xpath(f'//h4[contains(text(),"{term}")]/following-sibling::table[1]/tbody/tr')
            for block in blocks:
                event = block.xpath('./td[3]/text()').get('').strip()
                date = block.xpath('./td[1]/text()').get('').strip()
        
                # Append structured data as a dictionary to calendar_rows list
                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": term,
                    "Term Date": date,
                    "Term Date Description": event,
                })
    
        
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
