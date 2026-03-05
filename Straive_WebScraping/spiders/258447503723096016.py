import re                     
import json                   
import scrapy             
import PyPDF2              
import pandas as pd         
from ..utils import *       
from io import BytesIO    
from urllib.parse import quote    
from inline_requests import inline_requests 
from parsel import Selector   


class ClackamasSpider(scrapy.Spider):
    name = "clackamas"

    # Unique institution ID used for all datasets
    institution_id = 258447503723096016 

    course_url = "https://www.clackamas.edu/academics/courses-registration/schedule-of-classes?sched-term=2026%2FWI"

    # Faculty / Staff directory page
    directory_url = 'https://www.clackamas.edu/meta/directory'

    # Academic calendar page
    calendar_url = "https://www.clackamas.edu/academics/academic-calendar"

    # Initialize storage lists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Store scraped directory data
        self.directory_rows = []

        # Store scraped calendar data
        self.calendar_rows = []

        # Store scraped course data
        self.course_rows = []

    # Entry Point – Select Scrape Mode

    def start_requests(self):
        #why using playwrite

        # Read scrape mode from settings (course / directory / calendar / combinations)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode Execution ----
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

    def clean(self, value):
        return (value or "").strip()

    def parse_course(self, response):
        """
        Scrapes all course schedule data using Playwright
        because the page is dynamically rendered.
        """
        departments = response.xpath(
            '//select[@name="schedule_subject"]//option/@value'
        ).getall()

        departments = list(set([d for d in departments if d]))  # remove empty + dupes
        self.logger.info(f"Total departments found: {len(departments)}")

        for dept in departments:
            yield self.make_grid_request(dept)

    def make_grid_request(self, dept):

        source_url=f'https://www.clackamas.edu/academics/courses-registration/schedule-of-classes?sched-term=2026%2FWI&sched-name={dept}&sched-location=&sched-learning=&sched-open='
        grid_url = (
            "https://www.clackamas.edu/MVCGridHandler.axd"
            "?sched-term=2026%2FWI"
            f"&sched-name={dept}"
            "&sched-location="
            "&sched-learning="
            "&sched-open="
            "&Name=ClassScheduleGrid"
            f"&_=1768993829744"
        )

        headers = {
            "accept": "*/*",
            "x-requested-with": "XMLHttpRequest",
            "referer": (
                "https://www.clackamas.edu/academics/"
                "courses-registration/schedule-of-classes"
                f"?sched-term=2026%2FWI&sched-name={dept}"
            ),
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143 Safari/537.36"
            ),
        }

        return scrapy.Request(
            url=grid_url,
            headers=headers,
            callback=self.parse_course_details,
            cb_kwargs={
                "department": dept,
                "source_url": source_url
            }
        )

    def parse_course_details(self, response, department, source_url):
        sel = Selector(text=response.text)

        rows = sel.xpath("//table//tbody/tr")
        if not rows:
            self.logger.info(f"No data: {department}")
            return
        
        for row in rows:
            course_parts = row.xpath('./td[@class="cell-course"]//text()').getall()
            course_text = " ".join(course_parts).replace("Course", "").strip()
            if "-" in course_text:
                course_id, section = course_text.rsplit("-", 1)
            else:
                course_id = course_text
                section = ""

            title=row.xpath('./td[@class="cell-title"][1]//text()').get('')

            date_parts=row.xpath('./td[@class="cell-start-date"]//text()').getall()
            date_parts = [d.strip() for d in date_parts if d.strip() and d.strip() not in ("Dates", "End Date")]
            date = "".join(date_parts)

            parts = row.xpath('./td[@class="cell-number"]//text()').getall()
            class_num = parts[-1].strip() if parts else ""

            parts = row.xpath('./td[@class="cell-faculty"]//text()').getall()
            instructor = next((p.strip() for p in parts if p.strip() and p.strip() != "Faculty"), "")

            available=row.xpath('./td[@class="cell-available"]//text()').getall()
            available=''.join(available).replace('Available','').strip()

            self.course_rows.append(
                {
                    "Cengage Master Institution ID": 258447503723096016,
                    "Source URL": source_url,
                    "Course Name": f'{course_id} {title}',
                    "Course Description": '',
                    "Class Number":class_num,
                    "Section":  section,
                    "Instructor": instructor,
                    "Enrollment": available,
                    "Course Dates": date,
                    "Location": row.xpath("./td[@class='cell-campus']/a//text()").get(''),
                    "Textbook/Course Materials": '',
                })

        # Save course data
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")


    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Scrapes faculty and staff directory profiles.
        """
        departments = response.xpath('//select[@name="schedule_subject"]/option/@value').getall()

        for dept in departments:
            if not dept:
                continue

            encoded_dept = quote(dept)
            source_url=f'https://www.clackamas.edu/meta/directory?grid1first_name=&grid1last_name=&grid1extension=&grid1department={encoded_dept}'

            url = (
                "https://www.clackamas.edu/MVCGridHandler.axd"
                f"?grid1first_name="
                f"&grid1last_name="
                f"&grid1extension="
                f"&grid1department={encoded_dept}"
                f"&Name=DirectoryGridPeople"
            )

            headers = {
                "accept": "*/*",
                "x-requested-with": "XMLHttpRequest",
                "referer": (
                    "https://www.clackamas.edu/meta/directory"
                    f"?grid1department={encoded_dept}"
                ),
                "user-agent": "Mozilla/5.0"
            }

            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse_directory_details,
                cb_kwargs={'source_url':source_url}
            )

    def parse_directory_details(self,response,source_url):
        rows = response.xpath('//table[@id="MVCGridTable_DirectoryGridPeople"]/tbody/tr')

        for row in rows:
            name = row.xpath('.//td[@class="cell-name"]/text()').get('')
            title = row.xpath('.//td[@class="cell-title"]/text()').get('')
            department=row.xpath('.//td[@class="cell-department"]/text()').get('')
            email = row.xpath('.//td[@class="cell-email"]//a/@href').get('')
            phone = row.xpath('.//td[@class="cell-extension"]//a/text()').get('')

            # Clean values
            name = name.strip() if name else ""
            title = title.strip() if title else ""
            department = department.strip() if department else ""
            email = email.replace("mailto:", "").strip() if email else ""
            phone = phone.strip() if phone else ""

            self.directory_rows.append({
                "Cengage Master Institution ID": 258447503723096016,
                "Source URL": source_url,
                "Name": name,
                "Title": f'{title}, {department}',
                "Email": email,
                "Phone Number": phone,
            })

        # Save directory data
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    # CALENDAR SCRAPER

    def parse_calendar(self, response):
        # Select all term headers
        terms = response.xpath('//h3')
        for term in terms:
            term_name = term.xpath('text()').get(default='').strip()

             # <ul> immediately after the h3
            lis = term.xpath('following-sibling::ul[1]/li')

            for li in lis:
                text = li.xpath('string(.)').get(default='').strip()

                if not text:
                    continue

                # Split on LAST comma only
                if ',' in text:
                    date_part, desc_part = text.rsplit(',', 1)
                    date_part = date_part.strip()
                    desc_part = desc_part.strip()
                else:
                    date_part = text
                    desc_part = ""
    
                self.calendar_rows.append ({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": date_part,
                    "Term Date Description": desc_part,
                })
             # Save directory data
            cleaned_df = pd.DataFrame(self.calendar_rows)
            save_df(cleaned_df, self.institution_id, "calendar")

