import scrapy
import pandas as pd
from ..utils import *


class AdelphiSpider(scrapy.Spider):
    name = "adelphi"

    institution_id = 258455463719364569
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_source_url = "https://search.adelphi.edu/course-search/?semester=all&campus%5B0%5D=&level=all&school=all&courseName=&professorName=&distribution=&submitted=1&startrow=1"
    course_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'referer': 'https://search.adelphi.edu/course-search/?semester=all&campus%5B0%5D=&level=all&school=all&courseName=&professorName=&distribution=&submitted=1&startrow=100',
    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    }

    # DIRECTORY CONFIG
    directory_source_url = 'https://www.adelphi.edu/directory/'

    # CALENDAR CONFIG
    calendar_source_url = "https://www.adelphi.edu/academics/academic-calendar/"
    

    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course data is scraped using scrapy inside `parse_course`

        - Directory data is scraped using scrapy inside `parse_directory`

        - Calendar data is scraped using scrapy inside `parse_calendar`
        """
        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_source_url, headers =self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_source_url, headers =self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)
            yield scrapy.Request(url = self.course_source_url, headers =self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_source_url, headers =self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

    # PARSE COURSE
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
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        # Extract total number of available course records
        count = int(response.xpath('//p[contains(text(),"Showing records")]/strong/text()').get('').split('of')[-1].strip())
        # Number of records returned per page
        page_size = 100
        # Loop through all result pages using calculated offsets
        for startrow in range(1, count + 1, page_size):
            # Adjust offset to match server-side pagination behavior
            if startrow != 1:
                startrow -= 1
                
            course_source_url = f"https://search.adelphi.edu/course-search/?semester=all&campus%5B0%5D=&level=all&school=all&courseName=&professorName=&distribution=&submitted=1&startrow={startrow}"
            yield scrapy.Request(url = course_source_url, headers =self.course_headers, callback=self.parse_course_pagination, dont_filter=True)

    def parse_course_pagination(self,response):
        detail_urls = response.xpath('//tr[@class="details"]/td[1]/a/@href').getall()
        for details_url in detail_urls:
            details_url = response.urljoin(details_url)
            yield scrapy.Request(url = details_url, callback=self.parse_course_detail_page, dont_filter=True)
    
    def parse_course_detail_page(self,response):    
        course_name = response.xpath('//h2[@class="course-title"]/text()').get('').strip()
        if course_name:
            class_number = response.xpath('//td[contains(text(),"Number:")]/parent::tr/td[2]/strong/text()').get('').strip()
            description = response.xpath('//td[contains(text(),"Description:")]/parent::tr/td[2]/p/text()').get('').strip()
            course_date = response.xpath('//td[contains(text(),"Meets:")]/parent::tr/td[2]/strong/text()').get('').replace('\t','').strip()
            instructor = response.xpath('//td[contains(text(),"Instructor:")]/parent::tr/td[2]/strong/text()').get('').strip()

            if instructor == 'TBA':
                instructor = ''

            else:
                instructor = instructor

            self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": re.sub('\s+',' ',course_name),
                    "Course Description": re.sub(r'\s+',' ',description),
                    "Class Number": class_number,
                    "Section": '',
                    "Instructor": response.xpath('//td[contains(text(),"Instructor:")]/parent::tr/td[2]/strong/text()').get('').strip(),
                    "Enrollment": '',
                    "Course Dates": re.sub(r'\s+',' ',course_date),
                    "Location": response.xpath('//td[contains(text(),"Location:")]/parent::tr/td[2]/strong/text()').get('').strip(),
                    "Textbook/Course Materials": response.xpath('//td[contains(text(),"Materials:")]/parent::tr/td[2]/b/a/@href').get('').strip()
                })
        
    # PARSE DIRECTORY
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
        page_count = int(response.xpath('//span[@class="pagination_form_suffix"]/text()').get('').split('of')[-1].strip())
        for page in range(1,page_count+1):
            page_url = f'https://www.adelphi.edu/directory/?current_page={page}#item_list'
            yield scrapy.Request(url = page_url, callback=self.parse_directory_detail_page, dont_filter=True)

    def parse_directory_detail_page(self,response):
        blocks = response.xpath( '//div[@class="entity"]/div')
        for block in blocks:
            name = ' '.join(block.xpath('.//span[@class="entity_meta_name"]//text()').getall()).split('(')[0].strip()
            title_parts = []
            title_blocks = block.xpath('.//div[@class="entity_meta_info_wrap"]')
            if title_blocks:
                for title_block in title_blocks:
                    position = title_block.xpath('.//div[@class="contact_title"]/text()').get('').strip()
                    dept = ' '.join(title_block.xpath('.//div[@class="contact_dept"]//text()').getall()).strip()
                    # Combine position and department based on what is available
                    if position and dept:
                        title_parts.append(f"{position}, {dept}") # Both exist → "Position, Department"

                    elif position:
                        title_parts.append(position) # Only position exists

                    elif dept:
                        title_parts.append(dept)# Only department exists

                 # Remove duplicates while preserving order and join multiple titles with " | "
                title = " | ".join(dict.fromkeys(title_parts))
            email_link = block.xpath('.//a[@class="entity_link"]/@href').get('').strip()
            base_item = {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": re.sub(r'\s+', ' ', name),
            "Title": re.sub(r'\s+', ' ', title),
            "Email":'',
            "Phone Number": block.xpath('.//a[contains(@href,"tel:")]/span/text()').get('').strip(),
            }
            if email_link:
                yield scrapy.Request(
                    email_link,
                    callback=self.parse_directory_profile,
                    cb_kwargs={"item": base_item},
                    dont_filter=True
                )
            else:
                base_item["Email"] = ""
                self.directory_rows.append(base_item)
    
    def parse_directory_profile(self, response, item):
        # Extract the email address from the campus profile page
        item["Email"] = response.xpath('//span[@id="email"]/a/text()').get('').strip()
        self.directory_rows.append(item)
            
    # PARSE CALENDAR
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
        blocks = response.xpath('//table/tbody/tr[position() > 1]')
        for block in blocks:
            self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_source_url,
                    "Term Name":block.xpath('.//ancestor::table/preceding-sibling::h2[1]/text() | .//parent::tbody/parent::table/parent::div/@aria-label').get('').replace('</p>','').replace('<p>','').strip(),
                    "Term Date": block.xpath('.//td[1]/strong/text()').get('').strip(),
                    "Term Date Description": block.xpath('.//td[2]/text()').get('').strip(),
                })
       
    #Called automatically when the Scrapy spider finishes scraping.
    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")
        