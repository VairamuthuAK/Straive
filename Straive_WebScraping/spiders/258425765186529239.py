import re
import json
import scrapy
import requests
import urllib.parse
import pandas as pd
from ..utils import *
from inline_requests import inline_requests
from datetime import datetime,  timedelta, time


class LosmedanosSpider(scrapy.Spider):
    
    # Scrapy Spider Identifier
    name = "losmedanos"
    
    institution_id = 258425765186529239

    # Target URLs
    course_url = "https://webapps.4cd.edu/apps/courseschedulesearch/search-course.aspx?search=lmc"
    directory_source_url = "https://www.losmedanos.edu/directory/index.aspx"
    calendar_url = "https://www.losmedanos.edu/dates/index.aspx"


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
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        # List to store all scraped course data
        course_rows = []
        
        # Extract ASP.NET hidden fields from the initial page
        # These are needed to maintain session state for POST requests
        VIEWSTATE_text_main  = urllib.parse.quote(response.xpath('//input[@id="__VIEWSTATE"]/@value').get('')).strip()
        VIEWSTATEGENERATOR_text_main  = urllib.parse.quote(response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip())
        EVENTVALIDATION_text_main  = urllib.parse.quote(response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get('')).strip()
        
        # Extract all available academic terms from the dropdown
        terms = response.xpath('//select[@id="ctl00_PlaceHolderMain_SEC_TERM"]/option[not(@selected)]/@value').getall()
        print(terms)
        for term in terms:
            print(term)
            
            # Prepare the first POST payload to select the term
            payload = f'__EVENTTARGET=ctl00%24PlaceHolderMain%24SEC_TERM&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={VIEWSTATE_text_main}&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text_main}&__EVENTVALIDATION={EVENTVALIDATION_text_main}&ctl00%24PlaceHolderMain%24hiddenCurrentUserName=&ctl00%24PlaceHolderMain%24hiddenCourseSectionId=&ctl00%24PlaceHolderMain%24hiddenSearch=lmc&ctl00%24PlaceHolderMain%24hiddenCampus=LMC&ctl00%24PlaceHolderMain%24hiddenTerm=&ctl00%24PlaceHolderMain%24SEC_LOCATION=LMC&ctl00%24PlaceHolderMain%24SEC_TERM={term}'
            
            # Headers for POST request
            headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://webapps.4cd.edu',
            'referer': 'https://webapps.4cd.edu/apps/courseschedulesearch/search-course.aspx?search=lmc',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
           }
            # Send first POST request for the term    
            post_response = yield scrapy.Request(url=self.course_url,method="POST",headers=headers,body=payload)
            if post_response.status == 200:
               
               # Extract updated hidden fields for further POST requests
                VIEWSTATE_text  = urllib.parse.quote(post_response.xpath('//input[@id="__VIEWSTATE"]/@value').get('')).strip()
                VIEWSTATEGENERATOR_text  = urllib.parse.quote(post_response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip())
                EVENTVALIDATION_text  = urllib.parse.quote(post_response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get('')).strip()
                
                # Prepare the payload for searching courses with filters
                payload1 = f'__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={VIEWSTATE_text}&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text}&__EVENTVALIDATION={EVENTVALIDATION_text}&ctl00%24PlaceHolderMain%24hiddenCurrentUserName=&ctl00%24PlaceHolderMain%24hiddenCourseSectionId=&ctl00%24PlaceHolderMain%24hiddenSearch=lmc&ctl00%24PlaceHolderMain%24hiddenCampus=LMC&ctl00%24PlaceHolderMain%24hiddenTerm={term}&ctl00%24PlaceHolderMain%24SEC_LOCATION=LMC&ctl00%24PlaceHolderMain%24SEC_TERM={term}&ctl00%24PlaceHolderMain%24ONLINE_STATUS=ALL&ctl00%24PlaceHolderMain%24COURSE_TYPE_EVENING_WEEKEND%240=on&ctl00%24PlaceHolderMain%24COURSE_TYPE_EVENING_WEEKEND%241=on&ctl00%24PlaceHolderMain%24COURSE_TYPE_EVENING_WEEKEND%242=on&ctl00%24PlaceHolderMain%24COURSE_LENGTH%240=on&ctl00%24PlaceHolderMain%24COURSE_LENGTH%241=on&ctl00%24PlaceHolderMain%24tbxStartDate01=&ctl00%24PlaceHolderMain%24tbxStartDate02=&ctl00%24PlaceHolderMain%24X_SUBJ=&ctl00%24PlaceHolderMain%24SEC_FACULTY_NAME=&ctl00%24PlaceHolderMain%24X_COURSE=&ctl00%24PlaceHolderMain%24btnSearch=Search'
                headers1 = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
                'content-type': 'application/x-www-form-urlencoded',
                'origin': 'https://webapps.4cd.edu',
                'referer': 'https://webapps.4cd.edu/apps/courseschedulesearch/search-course.aspx?search=lmc',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                }
                # Send second POST request to retrieve courses for the term
                post_response1 = yield scrapy.Request(url=self.course_url,method="POST",headers=headers1,body=payload1)
                
                if post_response1.status == 200:
                    VIEWSTATE_text  = urllib.parse.quote(post_response1.xpath('//input[@id="__VIEWSTATE"]/@value').get('')).strip()
                    VIEWSTATEGENERATOR_text  = urllib.parse.quote(post_response1.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip())
                    EVENTVALIDATION_text  = urllib.parse.quote(post_response1.xpath('//input[@id="__EVENTVALIDATION"]/@value').get('')).strip()
                    blocks = post_response1.xpath('//td[contains(text(),"Term")]/parent::tr/following-sibling::tr')
                    for block in blocks[:-1]:
                        
                        course = re.sub('\s+',' ',block.xpath('./td[4]//span[contains(@id,"lblCourse")]/text()').get('')).strip()
                        section = block.xpath('./td[3]//a/text()').get('').strip()
                        section_url = block.xpath('./td[3]//a/@href').get('').strip()
                        section_url = urllib.parse.quote(re.findall(r'\'([\w\W]*?)\'', section_url)[0])
                        section_payload = f'__EVENTTARGET={section_url}&__EVENTARGUMENT=&__VIEWSTATE={VIEWSTATE_text}&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text}&__EVENTVALIDATION={EVENTVALIDATION_text}&ctl00%24PlaceHolderMain%24hiddenCurrentUserName=&ctl00%24PlaceHolderMain%24hiddenCourseSectionId=&ctl00%24PlaceHolderMain%24hiddenSearch=lmc&ctl00%24PlaceHolderMain%24hiddenCampus=LMC&ctl00%24PlaceHolderMain%24hiddenTerm={term}'
                        section_response = yield scrapy.Request(
                            url=self.course_url,
                            method="POST",
                            headers=headers1,
                            body=section_payload
                        )
                        description = re.sub(r'\s+', ' '," ".join(section_response.xpath('//div[@id="ctl00_PlaceHolderMain_pnlCatalogView"]/div//text()').getall())).strip()
                        
                        # Append course info to list
                        course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.course_url,
                            "Course Name": course,
                            "Course Description": description,
                            "Class Number": course.split(' - ')[0].strip(),
                            "Section": section,
                            "Instructor": " | ".join(block.xpath('./td[6]//td/text()').getall()).strip(),
                            "Enrollment": '',
                            "Course Dates": block.xpath('./td[4]//span[contains(@id,"lblDates")]/text()').get('').strip(),
                            "Location": block.xpath('./td[2]/text()').get('').strip(),
                            "Textbook/Course Materials": '',
                        })
                    
                    # Handle pagination (process subsequent "Next" pages)  
                    next_page = post_response1.xpath('//span[contains(text(),"Next")]/parent::a/@href').get('').strip()
                    while next_page:
                        event_target = urllib.parse.quote(re.findall(r'\'([\w\W]*?)\'', next_page)[0])
                        print(event_target)
                        if not event_target:
                            break
                        
                        # Build payload for next page
                        next_page_payload = f'__EVENTTARGET={event_target}&__EVENTARGUMENT=&__VIEWSTATE={VIEWSTATE_text}&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text}&__EVENTVALIDATION={EVENTVALIDATION_text}&ctl00%24PlaceHolderMain%24hiddenCurrentUserName=&ctl00%24PlaceHolderMain%24hiddenCourseSectionId=&ctl00%24PlaceHolderMain%24hiddenSearch=lmc&ctl00%24PlaceHolderMain%24hiddenCampus=LMC&ctl00%24PlaceHolderMain%24hiddenTerm={term}'
                        next_page_response = yield scrapy.Request(url=self.course_url,method="POST",headers=headers1,body=next_page_payload)
                        
                        # Extract updated ASP.NET hidden fields
                        VIEWSTATE_text  = urllib.parse.quote(next_page_response.xpath('//input[@id="__VIEWSTATE"]/@value').get('')).strip()
                        VIEWSTATEGENERATOR_text  = urllib.parse.quote(next_page_response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip())
                        EVENTVALIDATION_text  = urllib.parse.quote(next_page_response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get('')).strip()
                        blocks = next_page_response.xpath('//td[contains(text(),"Term")]/parent::tr/following-sibling::tr')
                        for block in blocks[:-1]:
                            section = block.xpath('./td[3]//a/text()').get('').strip()
                            section_url = block.xpath('./td[3]//a/@href').get('').strip()
                            section_url = urllib.parse.quote(re.findall(r'\'([\w\W]*?)\'', section_url)[0])
                            section_payload = f'__EVENTTARGET={section_url}&__EVENTARGUMENT=&__VIEWSTATE={VIEWSTATE_text}&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text}&__EVENTVALIDATION={EVENTVALIDATION_text}&ctl00%24PlaceHolderMain%24hiddenCurrentUserName=&ctl00%24PlaceHolderMain%24hiddenCourseSectionId=&ctl00%24PlaceHolderMain%24hiddenSearch=lmc&ctl00%24PlaceHolderMain%24hiddenCampus=LMC&ctl00%24PlaceHolderMain%24hiddenTerm={term}'
                            section_response = yield scrapy.Request(
                                url=self.course_url,
                                method="POST",
                                headers=headers1,
                                body=section_payload
                            )
                            description = re.sub(r'\s+', ' '," ".join(section_response.xpath('//div[@id="ctl00_PlaceHolderMain_pnlCatalogView"]/div//text()').getall())).strip()
                            course = re.sub('\s+',' ',block.xpath('./td[4]//span[contains(@id,"lblCourse")]/text()').get('')).strip()
                            
                            # Append course info to list
                            course_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": self.course_url,
                                "Course Name": course,
                                "Course Description": description,
                                "Class Number": course.split(' - ')[0].strip(),
                                "Section": section,
                                "Instructor": " | ".join(block.xpath('./td[6]//td/text()').getall()).strip(),
                                "Enrollment": '',
                                "Course Dates": block.xpath('./td[4]//span[contains(@id,"lblDates")]/text()').get('').strip(),
                                "Location": block.xpath('./td[2]/text()').get('').strip(),
                                "Textbook/Course Materials": '',
                            })
                            
                        # Check if there is another "Next" page 
                        next_page = next_page_response.xpath(
                            '//span[contains(text(),"Next")]/parent::a/@href'
                        ).get('').strip()
                else:
                    print(f"Request failed with status code post response1 {post_response1.status_code}")
            else:
                print(f"Request failed with status code post response {post_response.status_code}")
        
        # Convert collected course data into a DataFrame and save it
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
        
        # Initialize an empty list to store each row of the parsed directory data
        directory_rows = []
        
        # Select all rows in the directory table, excluding the ones with an 'id' attribute
        blocks = response.xpath('//table[@id="myTable"]/tbody/tr[not(@id)]')
        
        # Loop through each row (block) and extract relevant data
        for block in blocks:
            email = block.xpath('./td[@class="name email"]/a/@href').get('').replace("mailto:", "").strip()
            name = re.sub('\s+', ' ', block.xpath('./td[@class="name email"]//text()').get('')).strip()
            phones = block.xpath('./td[@class="phone1"]//text()').getall()
            titles = block.xpath('./td[@class="dept"]//text()').getall()
            titles = [t.strip() for t in titles if t.strip()]
            # for phone, title in zip(phones, titles):
            varient = 1
            for i, (phone, title) in enumerate(zip(phones, titles), start=0):
                
                
                phone = block.xpath('./td[@class="phone1"]//text()').getall()
                phone = [t.strip() for t in phone if t.strip()]
                phone = ''.join(phone).strip()
                if "Hubbard, Scott" in name:
                    if varient ==1:
                        varient +=1

                        if re.search(r'(\(925\).*?)\(925\)', phone):
                            phone = re.findall(r'(\(925\).*?)\(925\)', phone)[0]
                    elif varient == 2:
                        if re.search(r'\(925\).*?(\(925\).*)', phone):
                            phone = re.findall(r'\(925\).*?(\(925\).*)', phone)[0]
                        
                # Append a dictionary of the parsed data to the directory_rows list
                directory_rows.append(
                        {   
                            # Use the stored institution ID and directory source URL from the class
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.directory_source_url,
                            
                            # Extract the 'Name' from the 'name email' class and clean up extra spaces
                            "Name": name,
                            
                            # Extract the 'Title' from the 'dept' class and clean up extra spaces
                            "Title": re.sub('\s+', ' ', title).strip(),
                            
                            # Extract the email address, remove 'mailto:'
                            "Email": email,
                            
                            # Extract the phone number from the 'phone1' class
                            "Phone Number": phone.strip(),
                        }
                    )
        # Convert the list of directory rows into a pandas DataFrame for easier manipulation and saving
        directory_df = pd.DataFrame(directory_rows)
        
        # Save the DataFrame to a file with a specific name format (self.name + "directory")
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
        # List to store calendar events
        calendar_rows = []
        
        now = datetime.now()
        
        # Default cutoff time: 18:30 (6:30 PM)
        default_time = time(18, 30, 0)  
        current_time = now.time()

        # Use today's date if after 6:30 PM, otherwise yesterday
        if current_time >= default_time:
            target_date = now.date()
        else:
            target_date = (now - timedelta(days=1)).date()

        # Combine target date with default time
        dt = datetime.combine(target_date, default_time)

        # Convert datetime to a 10-digit Unix timestamp (seconds)
        timestamp_10_digits = int(dt.timestamp()) 
        
        url = f"https://timelyapp.time.ly/api/calendars/54704557/events?group_by_date=1&timezone=Asia/Calcutta&view=stream&start_date_utc={str(timestamp_10_digits)}&per_page=1000&page=1"
        headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'origin': 'https://calendar.time.ly',
        'referer': 'https://calendar.time.ly/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'x-api-key': 'c6e5e0363b5925b28552de8805464c66f25ba0ce'
        }
        
        #Execute API request
        api_response = requests.request("GET", url, headers=headers)
        
        all_dates = []
        json_data = json.loads(api_response.text)
        blocks = json_data['data']['items']
        for block in blocks:
            date = str(block)
            all_dates.append(date)
        for d in all_dates:
            blocks1 = json_data['data']['items'][d]
            for block1 in blocks1:
                title = re.sub('\s+', ' ', block1.get('title','')).strip()
                start_date = block1.get('start_utc_datetime','')
                end_date = block1.get('end_utc_datetime','')
                start_date = str(datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").date())
                end_date = str(datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").date())
                start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
                end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
                
                # Format dates for display
                start_formatted_date = start_date_obj.strftime("%b %d").replace(" 0", " ")
                end_formatted_date = end_date_obj.strftime("%b %d").replace(" 0", " ")
                day_name = end_date_obj.strftime("%a")
                des_short = re.sub('\s+', ' ',block1.get('description_short','')).strip()
                
                # Append structured row
                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": title,
                    "Term Date": f"{start_formatted_date} - {day_name} {end_formatted_date}",
                    "Term Date Description": des_short,
                })
                
        # Create DataFrame and save results
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")