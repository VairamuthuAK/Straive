import re
import time
import scrapy
import requests
import pandas as pd
from ..utils import *
from inline_requests import inline_requests
from playwright.sync_api import sync_playwright

class VancourverSpider(scrapy.Spider):

    name = "van"
    institution_id = 258429364411066331

    course_url = "https://schedules.wsu.edu/api/Data/GetHomePageDTO/"

    directory_url = "https://directory.vancouver.wsu.edu/?field_last_name_value=&field_first_value="


    calendar_url ="https://catalog.wsu.edu/api/Data/GetCalendarEvents/2025/1?u=a44220f7974747e08947f0ea11205742"
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

        elif mode == "calendar":
            self.parse_calendar()
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_calendar()

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        # All three (default)
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()


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

        json_data = response.json()
        all_terms = json_data.get('terms',[])
        rows=[]

        for all_term in all_terms:
            terms = all_term.get('terms',[])

            for term in terms:
                campus = term.get('campus','').strip()
                if campus == "Vancouver":
                    year = term.get('year','')
                    cr_term= term.get('term','').strip()
                    prefix_url = f"https://schedules.wsu.edu/api/Data/GetPrefixList/{campus}/{cr_term}/{year}"
                    prefix_response= yield scrapy.Request(prefix_url,dont_filter=True)

                    prefix_datas = prefix_response.json()
                    for prefix_data in prefix_datas:
                        prefix = prefix_data.get('prefix',"").strip()
                        data_url =f"https://schedules.wsu.edu/api/Data/GetSectionListDTO/{campus}/{cr_term}/{year}/{prefix}"
                        data_response= yield scrapy.Request(data_url,dont_filter=True)

                        json_datas = data_response.json()
                        sections = json_datas.get('sections',[])
                        for section in sections:
                            subject = section.get('subject','')
                            course_number = section.get('courseNumber','')
                            section_number = str(section.get('sectionNumber','')).strip()
                            title = section.get('title','')
                            course_title = f"{subject}{str(course_number)} {title}"
                            location = section.get('location','').strip()
                            class_number = str(section.get('sln','')).strip()
                            limit = section.get('enrollmentLimit','')
                            enroll = section.get('enrollment','')
                            isLab =section.get('isLab','')
                            desc_info_url = f"https://schedules.wsu.edu/api/Data/GetSectionInfo/{campus}/{cr_term}/{year}/{prefix}/{course_number}/{section_number}/{isLab}/true"
                            headers = {
                            'accept': '*/*',
                            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                            'content-type': 'application/json; charset=utf-8',
                            'priority': 'u=1, i',
                            'referer': 'https://schedules.wsu.edu/sectionList/',
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                            'x-requested-with': 'XMLHttpRequest',
                            }
                            desc_info_response= yield scrapy.Request(desc_info_url,headers=headers,dont_filter=True)

                            desc_json_data = desc_info_response.json()
                            desc_datas = desc_json_data.get('sectionInfo',{})
                            desc = desc_datas.get('courseDescription','')
                            descrip = re.sub(r"<.*?>","",desc)
                            descrip = re.sub(r"\s+"," ",descrip)
                            instructors = desc_datas.get('instructors',[])
                            instructor_names = ", ".join([f"{i.get('firstName', '').strip()} {i.get('lastName', '').strip()}".strip() for i in instructors])
                            start_date = desc_datas.get('startDate',"").strip()
                            end_date = desc_datas.get('endDate',"").strip()
                            course_date = f"{start_date} - {end_date}"
                        
                            rows.append({
                                        "Cengage Master Institution ID": self.institution_id,
                                        "Source URL": "https://schedules.wsu.edu//",
                                        "Course Name": course_title or "",
                                        "Course Description":descrip or "",
                                        "Class Number":  class_number or "",
                                        "Section": section_number or "",
                                        "Instructor": instructor_names or "",
                                        "Enrollment": f"{enroll}/{limit}",
                                        "Course Dates": course_date,
                                        "Location": location or "",
                                        "Textbook/Course Materials": "https://wsubookie.bncollege.com/",
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

        links = response.xpath('//div[@class="content clearfix"]/div/strong/following-sibling::a/@href').getall()
        rows=[]

        for link in links:
            url = response.urljoin(link)
            cat_response = yield scrapy.Request(url,dont_filter=True)

            listing_links = cat_response.xpath('//div[contains(@class,"views-field-field-professional-name person-name")]/div[@class="field-content"]/a/@href').getall()
            for listing_link in listing_links:
                url = cat_response.urljoin(listing_link)
                data_response = yield scrapy.Request(url,dont_filter=True)

                title = data_response.xpath('//div[contains(@class,"field-name-field-position-title")]/div/div/text()').get("").strip()
                dept = data_response.xpath('//div[contains(@class,"field-name-field-department")]/div/div/span/a/text()').getall()
                dept = [d.strip() for d in dept if d.strip()]
                if len(dept) > 1:
                    department = ", ".join(dept)
                else:
                    department = "".join(dept)

                if department:
                    course_title = f"{title}, {department}"
                else:
                    course_title = title

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": data_response.url,
                    "Name": data_response.xpath('//h1/text()').get("").strip(),
                    "Title":course_title,
                    "Email":data_response.xpath('//div[contains(@class,"field-name-field-email")]/div/div/a/text()').get('').strip(),
                    "Phone Number": data_response.xpath('//div[contains(@class,"field-name-field-phone")]/div/div/text()').get("").strip(),
                })
                
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")   
                        
    def parse_calendar(self):

        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            page.goto("https://catalog.wsu.edu/AcademicCalendar",wait_until="domcontentloaded", timeout=60000)
            time.sleep(12)

            options= page.locator('//select[@class="form-select"]/option')
            option_count = options.count()
            print(f"Total options: {option_count}")

            # Loop through options to find indexes for 2025-2030
            filtered_options = []
            for i in range(option_count):
                text = options.nth(i).inner_text().strip()
                if text.isdigit() and 2025 <= int(text) <= 2030:
                    print(f"Option index {i}, year: {text}")
                    filtered_options.append(i)  # store the index

            rows=[]
            for idx in filtered_options:
            
                with page.expect_response(
                    lambda response: "GetCalendarEvents" in response.url
                ) as response_info:
                    page.select_option("select", index=int(idx))
                    response = response_info.value
                    url =response.url
                    for term_num in range(1,4):
                        if term_num == 1:
                            term_name = "Spring"
                        elif term_num == 2:
                            term_name = "Summer"
                        else:
                            term_name = "Fall"
                        new_url = re.sub(r'/GetCalendarEvents/(\d{4})/\d+',rf'/GetCalendarEvents/\1/{term_num}',url)
                        term_response = requests.get(new_url)
                        json_dats = term_response.json()

                        for data in json_dats:
                            year = data.get('year','')
                            calendar_term_name = f"{term_name} {year}"
                            date = data.get('dateTime',"").split("T")[0]
                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": "https://catalog.wsu.edu/AcademicCalendar",
                                "Term Name": calendar_term_name,
                                "Term Date":date.strip() or "",
                                "Term Date Description":  data.get('description','').strip() or ""
                            })
            
            if rows:
                browser.close()
                calendar_df = pd.DataFrame(rows)
                save_df(calendar_df, self.institution_id, "calendar")

    