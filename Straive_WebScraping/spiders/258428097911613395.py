import re
import scrapy
import json
import pandas as pd
from inline_requests import inline_requests
from ..utils import save_df
from datetime import datetime


class ThomasSpider(scrapy.Spider):
    
    # Spider name used in command: scrapy crawl thomas
    name = "thomas"
    
    # Unique institution ID
    institution_id = 258428097911613395
    
    # Lists to store scraped data
    courseData = []
    directory_rows = []
    
    # Base URLs
    base_url = "http://www.stthom.edu/"
    course_url = "https://classes.aws.stthomas.edu/index.htm"
    directory_url = ""
    calendar_url = "https://www.stthomas.edu/academics/calendars/"
    
    # Common headers for all requests
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

    def start_requests(self):

        # Read scrape mode from settings (course / directory / calendar / combinations)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode Execution ----
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        elif mode == 'directory':
            self.parse_directory()

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            self.parse_directory()

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

    @inline_requests
    def parse_course(self, response):
        """
        Parse course data using request session response.

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
        
        # Get current year (used to filter future terms)
        current_year = datetime.now().year
        
        # Extract available years from dropdown
        years = response.xpath("//select[@id='year']/option/@value").getall()
        years = [y for y in years if y.isdigit() and int(y) >= current_year]
        
        # Extract available terms
        terms = response.xpath("//select[@id='term']/option/@value").getall()
        
        # Loop through each year and term
        for year in years:
            for term in terms:
                
                # API to get subject list
                subjectUrl = (
                    "https://classes.aws.stthomas.edu/json/getSubjectList"
                    f"?year={year}&term={term}&levelCode=ALL&schoolCode=ALL&hideInvisible=true"
                )
                
                # Request subject list
                subjectRes = yield scrapy.Request(
                    url=subjectUrl,
                    headers=self.headers,
                    dont_filter=True
                )
                subjects = json.loads(subjectRes.text)

                # Loop through each subject
                for sub in subjects:

                    subCode = sub.get("subjectCode")

                    if not subCode:
                        continue
                    
                    # Build course listing URL for subject
                    course_url = (
                        "https://classes.aws.stthomas.edu/index.htm"
                        f"?year={year}"
                        f"&term={term}"
                        f"&schoolCode=ALL"
                        f"&levelCode=ALL"
                        f"&displaySubjectCode={subCode}"
                        f"&selectedSubjects={subCode}"
                    )
                    
                    # Request course list page
                    termRes = yield scrapy.Request( url=course_url, headers=self.headers, dont_filter=True)
                    
                    # Extract course blocks
                    courses = termRes.xpath("//div[@class='course']")

                    for course in courses:
                        
                        # Extract subject number and section
                        raw_section = course.xpath(".//div[contains(@class,'numberAndCourse')]//span/text()").get()
                        
                        subject_number = ""
                        section = ""

                        if raw_section and "-" in raw_section:
                            subject_number, section = raw_section.split("-", 1)

                        subject_number = subject_number.strip() if subject_number else ""
                        section = section.strip() if section else ""
                        
                        # Extract course title
                        title = course.xpath(
                            ".//div[contains(@class,'medium-3')]/strong/text()"
                        ).get()
                        
                        # Extract subject code text
                        subject_text = course.xpath(
                            ".//p[contains(.,'Subject:')]//text()"
                        ).getall()
                        subject_text = " ".join(t.strip() for t in subject_text if t.strip())
                        subject_code = subject_text.split("(")[-1].replace(")", "").strip()

                        course_title = f"{subject_code} {subject_number} {title}"
                        
                        # ---- INLINE SUBJECT NORMALIZATION ----
                        sc = subCode.strip().upper() if subCode else ""
                        sc1 = subject_code.strip().upper() if subject_code else ""

                        # MAIL == AI
                        if sc in ("MAIL", "AI"):
                            sc = "AI"
                        if sc1 in ("MAIL", "AI"):
                            sc1 = "AI"

                        # REDP == RESIDENCY
                        if sc in ("REDP", "RESIDENCY"):
                            sc = "RESIDENCY"
                        if sc1 in ("REDP", "RESIDENCY"):
                            sc1 = "RESIDENCY"

                        # ---- FINAL COMPARISON ----
                        if sc != sc1:
                            continue

                        crn = course.xpath(".//span[@class='crn']/text()").get()

                        instructor = course.xpath(
                            "(.//a[contains(@href,'search.stthomas.edu')]//strong/text() | "
                            ".//p[strong[normalize-space(.)='Instructor:']]/text())[1]"
                        ).get()

                        size = course.xpath(
                            ".//div[contains(text(),'Size')]/text()"
                        ).re_first(r"\d+")

                        enrolled = course.xpath(
                            ".//div[contains(text(),'Enrolled')]/text()"
                        ).re_first(r"\d+")

                        enrollment = f"{enrolled}/{size}"

                        course_dates = course.xpath(".//table[contains(@class,'courseCalendar')]//label[normalize-space(.)='Dates:']/parent::td/text()").get()
                        course_dates = course_dates.strip() if course_dates else ""
                        
                        location = course.xpath(".//span[@class='locationHover']/text()").get()
                        
                        location = location.strip() if location else ""
                        
                        description = course.xpath(
                            ".//p[@class='courseInfo']//text()"
                        ).getall()
                        description = " ".join(t.strip() for t in description if t.strip())

                        book_url = course.xpath(
                            ".//a[contains(@href,'bncvirtual.com')]/@href"
                        ).get()
                        
                        
                        if course_dates:
                            self.courseData.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": termRes.url,
                                    "Course Name": re.sub(r"\s+", " ", course_title) if course_title else "",
                                    "Course Description": re.sub(r"\s+", " ", description) if description else "",
                                    "Class Number": crn,
                                    "Section": section,
                                    "Instructor": re.sub(r"\s+", " ", instructor) if instructor else "",
                                    "Enrollment": enrollment,
                                    "Course Dates": course_dates,
                                    "Location": location,
                                    "Textbook/Course Materials": book_url
                            })
                            continue
                        
                        schedule_blocks = course.xpath(".//td[contains(@class,'time')]/p")

                        seen_schedule = set()

                        # If schedule exists → split rows
                        if schedule_blocks:
                            for block in schedule_blocks:

                                # -------- Course Date --------
                                course_dates = block.xpath(".//strong/text()").get()
                                course_dates = course_dates.replace(":", "").strip() if course_dates else ""

                                # -------- Location --------
                                location = block.xpath(
                                    ".//span[@class='calendarLocation']/text()"
                                ).get()
                                location = location.strip() if location else ""

                                schedule_key = (course_dates, location)

                                if schedule_key in seen_schedule:
                                    continue

                                seen_schedule.add(schedule_key)
                                
                                self.courseData.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": termRes.url,
                                    "Course Name": re.sub(r"\s+", " ", course_title) if course_title else "",
                                    "Course Description": re.sub(r"\s+", " ", description) if description else "",
                                    "Class Number": crn,
                                    "Section": section,
                                    "Instructor": re.sub(r"\s+", " ", instructor) if instructor else "",
                                    "Enrollment": enrollment,
                                    "Course Dates": course_dates,
                                    "Location": location,
                                    "Textbook/Course Materials": book_url
                                })

                        
                        # -------- Asynchronous Coursework --------
                        asynch_text = course.xpath(".//td[contains(@class,'asynchNotice')]/text()").getall()
                        asynch_text = " ".join(asynch_text).strip()

                        # Extract only date range
                        asynch_dates = asynch_text.replace(":", "").strip()

                        if asynch_dates:

                            self.courseData.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": termRes.url,
                                "Course Name": re.sub(r"\s+", " ", course_title) if course_title else "",
                                "Course Description": re.sub(r"\s+", " ", description) if description else "",
                                "Class Number": crn,
                                "Section": section,
                                "Instructor": re.sub(r"\s+", " ", instructor) if instructor else "",
                                "Enrollment": enrollment,
                                "Course Dates": asynch_dates,
                                "Location": "Online",
                                "Textbook/Course Materials": book_url
                            })

        df = pd.DataFrame(self.courseData )
        save_df(df, self.institution_id, "course")
        

    def parse_directory(self):
        """
        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """
        
        # Directory data is not available, so save placeholder row
        rows=[
            {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.base_url,
            "Name": "Data not found",
            "Title":"Data not found",
            "Email": "Data not found",
            "Phone Number": "Data not found",
            }
        ]
        
        directory_df = pd.DataFrame(rows)
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
        
        # List to store calendar data
        calendar_data = []
        
        # Extract all calendar page links
        calendarLinks =  response.xpath("//main//div[@class='block__primary']//ul/li//a[not(contains(@href, 'final'))]//@href").getall()
        
        # Loop through each calendar link
        for calendarlink in calendarLinks:
            
            fullUrl = response.urljoin(calendarlink)
            
            # Request calendar page
            res = yield scrapy.Request(url=fullUrl, headers=self.headers)
            
            # Extract calendar sections
            sections = res.xpath("//section[contains(@class,'block-table-full')]")
            
            for section in sections:
                
                # Extract term name
                term_name = section.xpath(".//h2/text()").get()
                term_name = term_name.strip() if term_name else ""
                
                # Extract calendar rows
                rows = section.xpath(".//table//tbody/tr")
                
                for row in rows:
                    
                    # Extract date
                    term_date = row.xpath(".//th[@data-label][1]//text()").getall()
                    term_date = " ".join(t.strip() for t in term_date if t.strip())
                    term_date = term_date.replace("*", "").strip()
                    
                    # Extract description
                    term_desc = row.xpath(
                        ".//td[@data-label][1]//text()[not(ancestor::a)]"
                    ).getall()
                    term_desc = " ".join(t.strip() for t in term_desc if t.strip())
                    term_desc = re.sub(r"[\*;]+", "", term_desc)
                    term_desc = re.sub(r"\s+", " ", term_desc).strip()
                    
                    calendar_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": res.url,
                        "Term Name": term_name,
                        "Term Date": term_date.strip() if term_date else "",
                        "Term Date Description":  term_desc
                    })
                    
        cleaned_df = pd.DataFrame(calendar_data)
        save_df(cleaned_df, self.institution_id, "calendar")
