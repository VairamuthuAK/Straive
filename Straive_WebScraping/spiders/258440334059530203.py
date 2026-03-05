import scrapy
import pandas as pd
from ..utils import *
from parsel import Selector
from inline_requests import inline_requests

class CulverSpider(scrapy.Spider):

    name = "culver"
    institution_id = 258440334059530203

    course_rows = []
    course_url = "https://culver.edu/course-schedules/"

    directory_url = "https://culver.edu/directory/"

    calendar_url ="https://culver.edu/wp-json/tribe/views/v2/html?pu=%2F%3Fpost_type%3Dtribe_events%26eventDisplay%3Dmonth%26shortcode%3D42cf1b8c%26paged%3D3%26eventDate%3D2026-01&u=%2Fevents%2Flist%2Fpage%2F3%2F%3Fshortcode%3D42cf1b8c&smu=false&shortcode=42cf1b8c&tvn1=ddcdc74676&tvn2="

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
            yield scrapy.Request(self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

        # All three (default)
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)


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
        
        rows=[]
        years = response.xpath('//select[@name="year"]/option/@value').getall()

        for year in years:
            terms =response.xpath('//select[@name="term"]/option/@value').getall()

            for term in terms:
                url = "https://culver.edu/wp-admin/admin-ajax.php"
                payload = f"action=data_fetch&year={year}&term={term}&view=A"
                headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'origin': 'https://culver.edu',
                'priority': 'u=1, i',
                'referer': 'https://culver.edu/course-schedules/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest'
                }
                data_response=yield scrapy.Request(url=url,body=payload,headers=headers,method="POST",dont_filter=True)

                json_data = data_response.json()
                results = json_data.get('results','')
                response_sel = Selector(text=results)
                main_rows = response_sel.xpath('//tr[@class="vi-toggle-details"]')

                for row in main_rows:
                    detail_row = row.xpath('following-sibling::tr[1]')
                    course = row.xpath('./td[1]/text()').get("").strip().split(" ")
                    course = [c.strip() for c in course if c.strip()]
                    class_number = f"{course[0]} {course[1]}"

                    if len(course) > 2:
                        section = course[2].strip()
                    else:
                        section = ""

                    title= row.xpath('./td[2]/text()').get("").strip()
                    course_title = f"{class_number} {title}"
                    course_date = detail_row.xpath('./td/div/div//ul/li/strong[contains(text(),"Date:")]/parent::li/text()').get('').strip()
                    cap = detail_row.xpath('./td/div/div//ul/li/strong[contains(text(),"Capacity:")]/parent::li/text()').get('').strip()
                    enroll = detail_row.xpath('./td/div/div//ul/li/strong[contains(text(),"Current Enrollment:")]/parent::li/text()').get('').strip()
                    instructor = detail_row.xpath('./td/div/div//ul/li/strong[contains(text(),"Instructor:")]/parent::li/text()').get('').strip()

                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Course Name": course_title or "",
                        "Course Description": "",
                        "Class Number": class_number or "",
                        "Section": section or "",
                        "Instructor": instructor,
                        "Enrollment": re.sub(r'\s*See\s*Parent\s*Class.*$','',f"{enroll}/{cap}") or "",
                        "Course Dates": course_date,
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

        list_of_key_words = response.xpath('//select[@title="Search by Department"]/option/@value').getall()
        rows=[]

        for key_word in list_of_key_words[1:]:
            listing_url = f"https://culver.edu/?sfid=4343&sf_action=get_data&sf_data=results&_sft_departments_cpt={key_word}&lang=en"
            listing_response = yield scrapy.Request(url=listing_url,dont_filter=True)
            json_data = listing_response.json()
            results = json_data.get('results','')
            response_sel = Selector(text=results)
            links = response_sel.xpath('//div[@class="csc-directory-individual group"]//div[@class="csc-directory-more"]/a/@href').getall()

            for link in links:
                response = yield scrapy.Request(url=link.strip(),dont_filter=True)
                title = response.xpath('//h2[@class="heading-title"]/span/text()').get('').strip()
                dept = response.xpath('//li[@class="uabb-info-list-item info-list-item-dynamic1"]/div//div[@class="uabb-info-list-title"]/text()').get('').strip()

                rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Name": response.xpath('//h1/span/text()').get('').strip(),
                        "Title":f"{title}, {dept}",
                        "Email":response.xpath('//li[@class="uabb-info-list-item info-list-item-dynamic1"]/a//div[@class="uabb-info-list-title"]/text()').get('').strip(),
                        "Phone Number": response.xpath('//li[@class="uabb-info-list-item info-list-item-dynamic0"]/a//div[@class="uabb-info-list-title"]/text()').get('').strip()
                    })
                
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")

        
    @inline_requests   
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

        rows=[]
        current_term_name = None
        for page in range(0,9):
            calendar_url = (
            f"https://culver.edu/wp-json/tribe/views/v2/html?"
            f"pu=%2Fevents%2Flist%2F%3Fposts_per_page%3D15"
            f"%26shortcode%3D2f15d7b7"
            f"%26eventDisplay%3Dpast"
            f"&u=%2Fevents%2Flist%2Fpage%2F{page}%2F%3Fshortcode%3D2f15d7b7"
            f"&smu=false"
            f"&shortcode=2f15d7b7"
            f"&tvn1=ddcdc74676"
            f"&tvn2="
        )
            calendar_headers = {
            'accept': '*/*',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'priority': 'u=1, i',
            'referer': 'https://culver.edu/events/category/academic-events/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            }
            page_response = yield scrapy.Request(calendar_url,headers=calendar_headers,dont_filter=True)
            json_data =page_response.json()
            results= json_data.get('html','')
            response_sel = Selector(text=results)
            blocks = response_sel.xpath('//li[@class="tribe-common-g-row tribe-events-calendar-list__event-row"]')
            
            for block in blocks:
                term_name = block.xpath('.//preceding-sibling::li[1]/h3/time/text()').get('').strip()
                if term_name:
                    current_term_name = term_name.strip()
                    continue
                rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": "https://culver.edu/events/category/academic-events/",
                        "Term Name": current_term_name ,
                        "Term Date": block.xpath('./div//time/span[@class="tribe-event-date-start"]/text()').get("").strip().split("@")[0].strip(),
                        "Term Date Description": block.xpath('./div//a/text()').get('').strip()
                    })
        if rows:
            calendar_df = pd.DataFrame(rows)
            save_df(calendar_df, self.institution_id, "calendar")
        
    