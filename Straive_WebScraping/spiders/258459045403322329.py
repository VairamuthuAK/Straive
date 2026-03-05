import scrapy
import requests
import pandas as pd
from ..utils import *
from ..utils import save_df
from parsel import Selector

class WestSpider(scrapy.Spider):
    name = "west"
    institution_id = 258459045403322329

    # In-memory storage for datasets that are processed in bulk at the end
    course_rows = []
    calendar_rows = []
    directory_rows = []
    
    # Target URLs for the 2025-2026 academic year
    course_urls = [
        'https://www4.westminster.edu/resources/academics/course-schedule.cfm?year=2526&term=30&clusters_only=0&open_only=0&sl_only=0&rs_only=0&division=UG',
        "https://www4.westminster.edu/resources/academics/course-schedule.cfm?year=2526&term=20&clusters_only=0&open_only=518&sl_only=0&rs_only=0&division=UG"
        ]
    
    course_headers  = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://www.westminster.edu/academics/course-schedule.cfm',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }

    directory_url = "https://www.westminster.edu/sitemap.cfm"
    calendar_URL = "https://www.westminster.edu/academics/calendar.cfm"


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory,dont_filter=True)
        
        elif mode == 'calendar':
             yield scrapy.Request(url=self.calendar_URL,callback=self.parse_calendar,dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory,dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            for pdf_url in self.calendar_URL:
                yield scrapy.Request(url=pdf_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            for pdf_url in self.calendar_URL:
                yield scrapy.Request(url=pdf_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory,dont_filter=True)
        
        # All three (default)
        else:
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory,dont_filter=True)
            for pdf_url in self.calendar_URL:
                yield scrapy.Request(url=pdf_url, callback=self.parse_calendar)
       

    # Parse methods UNCHANGED from your original
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

        """
        Parse course Excel files and normalize rows.
        """
        
        blocks = response.xpath('//table[@id="course-schedule"]//tr')
        for idx, block in enumerate(blocks):

            # Clean up course title whitespace
            course_title = ' '.join(block.xpath('.//td[@class="text-nowrap"]//text()').get('').split())
            if course_title != '':
                title = block.xpath('.//td[2]/text()').get('').strip()
                instructor = ' '.join(t.strip() for t in block.xpath('.//td[4]/text()').getall() if t.strip())
                location =  ' '.join(''.join(t.strip() for t in block.xpath('.//td[6]//text()').getall() if t.strip()).split())
                start_date = block.xpath('.//td[8]//text()').get('')
                end_date = block.xpath('.//td[11]//text()').get('')
                seats_available = block.xpath('.//td[12]//text()').get('')
                capacity = block.xpath('.//td[13]//text()').get('')

                # Split 'ACC 201 01' into Code (ACC 201) and Section (01)
                courses = ' '.join(course_title.split(' ')[0:2])
                course_name = courses + " " + title
                try:
                    section = course_title.split(' ')[2]
                except:
                   pass
                course_dates = start_date + ' - ' + end_date
                enrollment = seats_available + '/' + capacity
                
                self.course_rows.append(
                    {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": course_name,
                    "Course Description": '',
                    "Class Number": courses,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": enrollment,
                    "Course Dates": course_dates,
                    "Location": location,
                    "Textbook/Course Materials": "",
                    }
                )

        # Split 'ACC 201 01' into Code (ACC 201) and Section (01)
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

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

        """
        Parse employee directory profiles and extract emails via hCaptcha.
        """

        # Find all profile links within the 'people' section
        blocks = response.xpath('//div[@id="people"]//div//a/@href').getall()
        for index, block in enumerate(blocks):
            links = response.urljoin(block)
            res = requests.get(links)
            people_response = Selector(text=res.text)
        
            name = people_response.xpath('//h1/text()').get('').strip()
            title = people_response.xpath('//h3[@id="profile-title"]/text()').get('').strip()

            # Clean phone number data
            phone = [x.strip() for x in people_response.xpath('//h4[@class="text-dark-blue no-margin"]//text()').getall() if x.strip()]
            phone = ''.join(phone)
            if '0' == phone:
                phone = ''
            email = people_response.xpath("//div[@class='col-sm-4']//a[starts-with(@href, 'mailto:')]/text()").get('').strip()
           
            self.directory_rows.append(
            {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": links,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
                }
            )
    
        


    def parse_calendar(self,response):

        nodes = response.xpath('//h2 | //div[@class="row data-row"]')
        current_term = None
        for node in nodes:
            # If node is a header, update the current term context
            if node.root.tag == 'h2':
                current_term = node.xpath('text()').get('').strip()

            # If node is a data row, extract date and description
            else:
                term_date = node.xpath('./div[1]//text()').getall()
                term_date = ''.join(term_date)
                term_desc = node.xpath('./div[2]//text()').getall()
                term_desc = ' '.join(term_desc)
                self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": current_term,
                "Term Date": term_date,
                "Term Date Description": term_desc
            })
   
    def closed(self, reason):

        """
        Final cleanup and persistence.

        Saves:
        - Directory dataset
        - Calendar dataset
        - Closes all file handles
        """

        if self.directory_rows:
            save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

        if self.calendar_rows:
            save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")




 