import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests

class WindwardSpider(scrapy.Spider):

    name = "wind"
    institution_id = 258424099573884890

    course_rows = []
    course_url = "https://windward.hawaii.edu/programs-of-study/class-availability/"

    directory_url = "https://windward.hawaii.edu/about-wcc/directory/"

    calendar_url = "https://windward.hawaii.edu/campus-life/events-calendar/academic-calendar/"

    def start_requests(self):

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        # Single functions
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # All three (default)
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
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

        blocks = response.xpath('//table[@class="classAvailabilityTable"]/tbody/tr')
        rows=[]

        for block in blocks:
            course_date = block.xpath('./td[6]/text()').get('').strip()
            crn_number = block.xpath('./td[7]/a/text()').get('').strip()
            instructor = block.xpath('./td[3]/text()').get('').strip()
            course_number = block.xpath('./td[1]/a/text()').get('').strip()
            title = block.xpath('./td[2]/a/text()').get('').strip()
            course_title = f"{course_number} {title}"
            desc_url =block.xpath('./td[1]/a/@href').get('').strip()
            desc_response = yield scrapy.Request(desc_url,dont_filter=True,meta={"handle_httpstatus_all": True})

            if desc_response.status == 200:
                desc = desc_response.xpath('//div[contains(@class,"field--name-field-description")]/p/text() | //div[contains(@class,"field--name-field-description")]//text()').get("").strip()
                rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": "https://windward.hawaii.edu/programs-of-study/class-availability/",
                "Course Name": course_title or "",
                "Course Description":desc or "",
                "Class Number":  crn_number or "",
                "Section": "",
                "Instructor": instructor or "",
                "Enrollment": "",
                "Course Dates": course_date,
                "Location": "",
                "Textbook/Course Materials": "",
            })
            else:
                rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": "https://windward.hawaii.edu/programs-of-study/class-availability/",
                "Course Name": course_title or "",
                "Course Description": "",
                "Class Number":  crn_number or "",
                "Section": "",
                "Instructor": instructor or "",
                "Enrollment": "",
                "Course Dates": course_date,
                "Location": "",
                "Textbook/Course Materials": "",
            })
                
        if rows:
            course_df = pd.DataFrame(rows)
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

        blocks = response.xpath('//div[@id="wccDirectoryListing"]/ul/li')
        rows=[]

        for block in blocks[2:]:
            source_url = block.xpath('.//div[@class="wccDirectoryInfoLeft"]/a/@href').get('').strip()
            name = block.xpath('.//div[@class="wccDirectoryInfoLeft"]/a/text()').get('').strip()
            title = block.xpath('.//div[@class="wccDirectoryInfoLeft"]/span/text()').get('').strip()
            phone = block.xpath('.//div[@class="wccDirectoryInfoRight"]/a[contains(@href,"tel:")]/text()').get('').strip()
            email = block.xpath('.//div[@class="wccDirectoryInfoRight"]/a[contains(@href,"mailto")]/text()').get('').strip()

            if "/windward.hawaii.edu" in source_url or "library.wcc.hawaii.edu" in source_url or "gallery.windward.hawaii.edu" in source_url:
                continue

            rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": f"https://windward.hawaii.edu/about-wcc/directory/{source_url}",
                    "Name": name,
                    "Title": title,
                    "Email":email,
                    "Phone Number": phone,
                })
            
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")
    
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

        for term in ["Fall 2025", "Spring 2026", "Summer 2026"]:
            term_name = response.xpath(f'//h2[contains(text(),"{term}")]/text()').get()
            rows = response.xpath(f'//h2[contains(text(),"{term}")]/following::table[1]//tr')

            for row in rows:
                date = row.xpath('./td[1]//text()').get(default='').strip()
                desc = " ".join(row.xpath('./td[2]//text()').getall()).strip()

                if date and desc:
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": date,
                        "Term Date Description": desc
                    })

        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")


    
