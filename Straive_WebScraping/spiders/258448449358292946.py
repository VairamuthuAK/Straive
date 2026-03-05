import json
import scrapy
import pandas as pd
from ..utils import *
from html import unescape
from datetime import datetime


class FoothillSpider(scrapy.Spider):
    
    name = "foothill"
    institution_id = 258448449358292946
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_source_urls = ["https://foothill.edu/schedule/?dept=every&srchinst=&srchcrn=&Quarter=2026S&modality=anymodality&oer=any&availability=all&type=any&GEArea=any&time=Any+Time&location=anywhere&ADay=A",
                        "https://foothill.edu/schedule/?dept=every&srchinst=&srchcrn=&Quarter=2026W&modality=anymodality&oer=any&availability=all&type=any&GEArea=any&time=Any+Time&location=anywhere&ADay=A"]
    course_headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
                'Connection': 'keep-alive',
                'Referer': 'https://foothill.edu/schedule/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                }
    # DIRECTORY CONFIG
    directory_source_url = "https://foothill.edu/directory/"
    directory_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://foothill.edu/calendar/index.html"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        All data extraction in this spider is performed entirely using Scrapy.
        """
        # Single functions
        if mode == "course":
            for course_source_url in self.course_source_urls:
                yield scrapy.Request(url = course_source_url, headers =self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            for course_source_url in self.course_source_urls:
                yield scrapy.Request(url = course_source_url, headers =self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)
            for course_source_url in self.course_source_urls:
                yield scrapy.Request(url = course_source_url, headers =self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            for course_source_url in self.course_source_urls:
                yield scrapy.Request(url = course_source_url, headers =self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
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
        blocks = response.xpath('//container[@class="fh_sched-wrap"]')
        for block in blocks:
            class_number = block.xpath('.//strong[contains(text(),"Course Number (CRN):")]/parent::h4/text()').get('').strip()
            section = block.xpath('.//strong[contains(text(),"Section:")]/parent::h4/text()').get('').strip()
            name = block.xpath('(./preceding-sibling::div//div[@class="fh_grid-title"]/h3/text())[last()]').get('').strip()
            name_id = block.xpath('(./preceding-sibling::div//div[@class="fh_grid-id"]/h3/text())[last()]').get('').strip()
            description = ''.join(block.xpath('(./preceding-sibling::div[@class="panel-group fh_panel-group"]//li/span[contains(text(),"Description:")]/parent::li/text())[last()]').getall()).strip()
            self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": f'{name_id} - {name}',
                    "Course Description": re.sub(r'\s+',' ',description),
                    "Class Number": class_number,
                    "Section": section,
                    "Instructor": block.xpath('.//a[contains(@href,"/directory/profile")]/text()').get('').strip(),
                    "Enrollment": block.xpath('.//div[@class="meet-availability"]/span/text()').get('').replace(' seats open','').strip(),
                    "Course Dates": block.xpath('.//strong[contains(text(),"Dates:")]/parent::h4/text()').get('').strip(),
                    "Location": '',
                    "Textbook/Course Materials": block.xpath('.//a[contains(text(),"Check Bookstore")]/@href').get('').strip()
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
        rows = response.xpath('//table[@id="fh_dirIndex"]/tbody/tr')
        for row in rows:
            name = ''.join(row.xpath('.//td[1]//text()').getall()).strip()
            email = f"{name.lower().replace(',', '').replace(' ', '')}@fhda.edu"
            phone = row.xpath('.//td[2]/text()').get('').strip()
            detail_url = row.xpath('.//td[1]/a/@href').get()
            item = {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": '',
                "Name": re.sub(r'\s+', ' ', name),
                "Title": '',
                "Email": email,
                "Phone Number": phone,
            }
            if detail_url:
                yield response.follow(
                    detail_url,
                    callback=self.parse_directory_detail,
                    meta={"item": item, "source_url": response.urljoin(detail_url)}
                )

            else:
                item["Title"] = ""
                item["Source URL"] = response.url
                self.directory_rows.append(item)

    def parse_directory_detail(self, response):
        item = response.meta["item"]
        item["Source URL"] = response.meta["source_url"]
        pos = response.xpath('//h3[contains(@class,"fh_user-title")]/text()').get('')
        dep = ''.join(response.xpath('//h3[normalize-space(text())="Department"]/following::div[1]//text() | //h3[normalize-space(text())="Departments"]/following::div[1]//text()').getall()).strip()
        div = ''.join(response.xpath('//h3[normalize-space(text())="Division"]/following::div[1]//text() | //h3[normalize-space(text())="Divisions"]/following::div[1]//text()').getall()).strip()
        # Combine position, department, and division into a single string, skipping empty values
        title = ', '.join(filter(None, [
            pos.strip(),
            dep.strip(),
            div.strip()
        ]))
        item["Title"] = re.sub(r'\s+', ' ', title)
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
        events = json.loads(response.text)
        for event in events:
            title = event.get("title", "").strip()
            description = event.get("descriptionText", "").strip()
            raw_desc = event.get("descriptionText", "") or event.get("description", "")
            desc = re.sub(r"<.*?>", "", raw_desc)
            desc = unescape(desc)
            desc = re.sub(r"\s+", " ", desc).strip()
            description = desc
            start_date = event.get("startDate") or event.get("startDatetime") or ""
            end_date = event.get("endDate") or event.get("endDatetime") or ""
            term_date = ""
            if start_date and end_date:
                start = datetime.fromisoformat(start_date.replace("Z", ""))
                end = datetime.fromisoformat(end_date.replace("Z", ""))

                if start.date() == end.date():
                    term_date = start.strftime("%B %d, %Y")
                elif start.month == end.month:
                    term_date = f"{start.strftime('%B %d')} – {end.strftime('%d, %Y')}"
                else:
                    term_date = f"{start.strftime('%B %d, %Y')} – {end.strftime('%B %d, %Y')}"

            self.calendar_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.calendar_source_url,
            "Term Name": title,
            "Term Date": term_date,
            "Term Date Description": description
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
        
