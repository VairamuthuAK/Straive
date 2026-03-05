import json
import re
import scrapy
import requests
import pandas as pd
from ..utils import *
from parsel import Selector
from datetime import datetime
from inline_requests import inline_requests
from datetime import datetime, date, timedelta


class HardingSpider(scrapy.Spider):
    name = "harding"
    institution_id = 258432123357128657

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://www.harding.edu/about/offices-departments/registrar/courses/"
    directory_source_url = "https://hardingsports.com/staff-directory"
    calendar_url = "https://www.harding.edu/about/calendar/"

   
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
        
        # Current and previous years to filter terms
        current_year = datetime.now().year
        previous_year = current_year - 1
        
        # Extract URL for schedule list (from JS load() function)
        schedule_list_php_url = ''
        if re.search(r'\#schedules\'\)\.load\(\'(.*?)\'', response.text):
            schedule_list_php_url = re.findall(r'\#schedules\'\)\.load\(\'(.*?)\'', response.text)[0]
        
        # Request the schedule list page
        term_urls_response = yield scrapy.Request(schedule_list_php_url)
        
        # Extract term IDs and names
        term_ids = term_urls_response.xpath('//select[@id="archiveList"]/option/@value').getall()
        term_names = term_urls_response.xpath('//select[@id="archiveList"]/option/text()').getall()
        
        # Loop through each term for current or previous year
        for term_id, term_name in zip(term_ids, term_names):
            if str(current_year) in term_name or str(previous_year) in term_name:
                term_url = f"https://misnix.harding.edu/registrar/schedule/?term={term_id}&level=ARCH"
                term_response = yield scrapy.Request(term_url)
                
                # Extract course rows from the term schedule table
                blocks = term_response.xpath('//table[@id="data"]/tbody/tr[@class="row"]')
                for block in blocks:
                    sec = str(block.xpath('./td[3]/text()').get('').strip())
                    title = block.xpath('./td[6]/text()').get('').strip()
                    name = block.xpath('./td[2]/text()').get('').strip()
                    location = block.xpath('./td[10]/text()').get('').strip()
                    if "-" == location:
                        location = ''
                    course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": term_url,
                        "Course Name": f"{name} {title}",
                        "Course Description": '',
                        "Class Number": block.xpath('./td[1]/text()').get('').strip(),
                        "Section": sec,
                        "Instructor": block.xpath('./td[11]/text()').get('').strip(),
                        "Enrollment": '',
                        "Course Dates": block.xpath('./td[7]/text()').get('').strip(),
                        "Location": location,   
                        "Textbook/Course Materials": '',
                    })
                    
        # Convert collected course data into a DataFrame and save
        course_df = pd.DataFrame(course_rows)
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
        
        # Select all staff rows from the directory table
        blocks = response.xpath('//table[@class="sidearm-table collapse-on-medium"]//tr[@class="sidearm-staff-member "]')
        
        # Loop through each staff member row
        for block in blocks:
            
            # Extract relative URL for staff detail page
            staff_url = block.xpath('./td[1]/a/@href').get('').strip()
            
            # Convert relative URL to absolute URL
            staff_url = f"https://hardingsports.com{staff_url}"
            
            # Headers to mimic a real browser request
            headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
            'referer': 'https://hardingsports.com/staff-directory',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            }
            
            # Make HTTP request to staff detail page (blocking call)
            directory_response = requests.request("GET", staff_url, headers=headers)
            
            directory_xpath_obj = Selector(text=directory_response.text)
            
            # Append extracted staff data to the results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_source_url,
                        "Name": block.xpath('./td[1]/a/text()').get('').strip(),
                        "Title": re.sub('\s+',' ',block.xpath('./td[2]/text()').get('')).strip(),
                        
                        # Email extracted from staff detail page
                        "Email": directory_xpath_obj.xpath('//dt[contains(text(),"Email")]/following::dd[1]//text()').get('').strip(),
                        
                        "Phone Number": block.xpath('./td[4]/a/text()').get('').strip(),
                    }
                )
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
        # List to store calendar events
        calendar_rows = []
        
        # Extract Google Calendar iframe URL
        google_cal_url = response.xpath('//div[@class="widget full-embed centered"]/iframe/@src').get('').strip()
        
        # Request headers for fetching the calendar iframe
        headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'referer': 'https://www.harding.edu/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }
        
        # Fetch Google Calendar embed page
        google_cal_response = requests.request("GET", google_cal_url, headers=headers)
        
        # Extract calendar ID and API key from the iframe page
        if re.search(r'(\"embed\.EmbedInitialData[\w\W]*?calendar\.google\.com)', google_cal_response.text):
            dd = re.findall(r'(\"embed\.EmbedInitialData[\w\W]*?calendar\.google\.com)', google_cal_response.text)[0]
        else:
            dd =''
        if re.search(r'harding\.edu\_([\w\W]*)', dd):
            id = re.findall(r'harding\.edu\_([\w\W]*)', dd)[0].replace('@','%40')
        else:
            id =''
        if re.search(r'Data.*?\".*?\"(.*?)\"\,', dd):
            key = re.findall(r'Data.*?\".*?\"(.*?)\"\,', dd)[0].replace("\\",'')
        else:
            key =''
        
        # Set time range: today to end of year
        today_date = date.today()
        today_str = today_date.strftime("%Y-%m-%d")
        end_of_year = date(today_date.year, 12, 31)
        end_of_year_str = end_of_year.strftime("%Y-%m-%d")
        
        # Construct Google Calendar API URL
        cal_url = f"https://clients6.google.com/calendar/v3/calendars/harding.edu_{id}/events?calendarId=harding.edu_{id}&singleEvents=true&eventTypes=default&eventTypes=focusTime&eventTypes=outOfOffice&timeZone=America%2FChicago&maxAttendees=1&maxResults=250&sanitizeHtml=true&timeMin={str(today_str)}T00%3A00%3A00%2B18%3A00&timeMax={str(end_of_year_str)}T00%3A00%3A00-18%3A00&key={key}&%24unique=gc456"
        
        # Headers for Google Calendar API request
        headers = {
        'accept': '*/*',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'origin': 'https://calendar.google.com',
        'referer': 'https://calendar.google.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
       }
        # Fetch calendar events via Scrapy request
        calender_response = yield scrapy.Request(cal_url, headers=headers)
        json_data = json.loads(calender_response.text)
        blocks = json_data['items']
        if blocks:
            
            # Process each event
            for block in blocks:
                start_date = block['start'].get('date','')
                if not start_date:
                    start_date = block['start'].get('dateTime','')
                    
                # Normalize to YYYY-MM-DD
                if "T" in start_date:
                    start_date = start_date.split('T')[0]
                end_date = block['end'].get('date','')
                if not end_date:
                    end_date = block['end'].get('dateTime','')
                if "T" in end_date:
                    end_date = end_date.split('T')[0]
                
                # Convert to datetime objects
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
                
                days_counts = (end - start).days 
                term_formatted_date = ''
                
                # Format term name as "Month Year"
                term_formatted_date = start.strftime("%B %Y")
                clean_summary = re.sub(r'\s+', ' ', block.get('summary', '')).strip()
                
                dt = datetime.strptime(start_date, "%Y-%m-%d")
                formatted_date = f"{dt.day} {dt.strftime('%b')}, {dt.strftime('%a')}"
                
                # Single-day event
                if 1 == days_counts:
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_url,
                        "Term Name": term_formatted_date,
                        "Term Date": formatted_date,
                        "Term Date Description": clean_summary,
                    })
                else:
                    # Multi-day event: create a row for each day
                    date_obj = datetime.strptime(start_date, "%Y-%m-%d")
                    for count in range(1, int(days_counts + 1), 1):
                        
                        formatted_date = f"{date_obj.day} {date_obj.strftime('%b')}, {date_obj.strftime('%a')}"
                        term_formatted_date = date_obj.strftime("%B %Y")
                        calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_url,
                            "Term Name": term_formatted_date,
                            "Term Date": formatted_date,
                            "Term Date Description": f"{clean_summary} (Day {count}/{days_counts})",
                        })
                        date_obj = date_obj + timedelta(days=1)
        
        # Convert scraped directory rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
