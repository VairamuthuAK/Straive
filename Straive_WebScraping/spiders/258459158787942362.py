import re
import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests

class NorthwesternSpider(scrapy.Spider):

    name = "north"
    institution_id = 258459158787942362

    course_rows = []
    course_url = "https://class-descriptions.northwestern.edu/"

    directory_url = "https://www.mccormick.northwestern.edu/research-faculty/directory/faculty-search-list.xml"


    calendar_url ="https://www.registrar.northwestern.edu/calendars/academic-calendars/"
    calendar_headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'priority': 'u=1, i',
        'referer': 'https://culver.edu/events/list/page/3/?shortcode=42cf1b8c',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        }

    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        # If only calendar data is required
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # All three (default)
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    
    @inline_requests
    def parse_course(self,response):

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

        links = response.xpath('//div[@class="content"]/ul/li/a[contains(text(),"2026") or contains(text(),"2025")]/@href').getall()
        rows=[]

        for link in links:
            url = f"https://class-descriptions.northwestern.edu/{link}"
            listing_response =yield scrapy.Request(url,dont_filter=True)
            list_links = listing_response.xpath('//div[@class="content"]/h2[contains(normalize-space(),"Select a school:")]/following-sibling::*[1][self::ul]/li/a/@href').getall()
            
            for list_link in list_links:
                
                url = f"https://class-descriptions.northwestern.edu/{list_link}"
                cat_response =yield scrapy.Request(url,dont_filter=True)
                course_urls = cat_response.xpath('//div[@class="content"]/ul/li/a/@href').getall()
                
                for course_url in course_urls:

                    url =  cat_response.urljoin(course_url)
                    course_response =yield scrapy.Request(url,dont_filter=True)
                    page_urls = course_response.xpath('//div[@class="expand-collapse jos"]/div/ul/li/a/@href').getall()

                    for page in page_urls:
                        url = course_response.urljoin(page)
                        response =yield scrapy.Request(url,dont_filter=True)

                        title = response.xpath('//h1/span/text()').get("").split(" ")
                        title = [t.strip() for t in title if t.strip()]
                        main_title = " ".join(title[:-1])
                        class_number=title[-1].split("-")[0].replace("(","").strip()
                        course_title = f"{class_number} {main_title}"
                        section="-".join(title[-1].split("-")[1:]).replace(")","").strip()
                        instructor = response.xpath('//div[@class="content"]/h2[normalize-space()="Instructors"]/following-sibling::p[preceding-sibling::h2[1][normalize-space()="Instructors"]]/text()').get("").strip()
                        desc = response.xpath('//div[@class="content"]/h2[normalize-space()="Overview of class"]/following-sibling::p[preceding-sibling::h2[1][normalize-space()="Overview of class"]]/text()').get("").strip()

                        rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Course Name": course_title or "",
                        "Course Description":desc or "",
                        "Class Number": class_number or "",
                        "Section": section or "",
                        "Instructor": instructor or "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": '',
                    })
                        
        if rows:
            course_df = pd.DataFrame(rows)
            save_df(course_df, self.institution_id, "course")

    @inline_requests    
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
        

        links= ""
        if re.search(r"\<pageLink\>(.*?)\<\/pageLink\>",response.text):
            links  = re.findall(r"\<pageLink\>(.*?)\<\/pageLink\>",response.text)
            rows=[]
            for link in links:
                url = f"https://www.mccormick.northwestern.edu{link}"
                response = yield scrapy.Request(url=url,dont_filter=True)

                title = response.xpath('//div[@id="faculty-profile-left"]/p[@class="title"]/text()  |  //div[@id="faculty-profile-left"]//h2[normalize-space()="Departments"]/following-sibling::p[preceding-sibling::h2[1][normalize-space()="Departments"]]/a/text()').getall()
                title = [t.strip() for t in title if t.strip()]
                title = ", ".join(title).strip()

                rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Name": response.xpath('//h1/text()').get('').strip(),
                        "Title":title,
                        "Email":response.xpath('//div[@id="faculty-profile-left"]//a[@class="mail_link"]/@href').get('').replace('mailto:','').strip(),
                        "Phone Number": response.xpath('//div[@id="faculty-profile-left"]//a[@class="phone_link"]/span/text()').get('').strip()
                    })
                
            if rows:
                df = pd.DataFrame(rows)
                save_df(df, self.institution_id, "campus")


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

        tabs = response.xpath('//div[@id="tab-content"]/div/div[@class="calendar-event"]')
        rows=[]
        current_term_name = None
        
        for tab in tabs:
            tab_id= tab.xpath('./parent::div/@aria-labelledby').get('').strip()
            term_year = tab.xpath(f'./parent::div/parent::div/parent::div/ul/li/a[@id="{tab_id}"]/text()').get('').strip()
            term_name = tab.xpath('./parent::div/h3/text()').get('').strip()

            if term_name:
                current_term_name = f"{term_name.strip()}-{term_year.strip()}"
            else:
                current_term_name=current_term_name
      
            date = tab.xpath('./div[@class="event-date"]/text()').get("").strip().split(',')[1].strip().split(" ")[0]
            
            rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": f"{date}-{term_year}" ,
                        "Term Date": tab.xpath('./div[@class="event-date"]/text()').get("").strip(),
                        "Term Date Description": tab.xpath('./div[@class="event-name"]/a/text()').get('').strip()
                    })
            
        if rows:
            calendar_df = pd.DataFrame(rows)
            save_df(calendar_df, self.institution_id, "calendar")
           
            
    