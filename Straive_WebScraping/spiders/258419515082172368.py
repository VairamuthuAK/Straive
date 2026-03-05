import scrapy
import pandas as pd
from ..utils import *


class BrookSpider(scrapy.Spider):

    name = "brook"
    institution_id = 258419515082172368
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_source_url = "https://globalsearch.cuny.edu/CFGlobalSearchTool/CFSearchToolController"
    course_url = "https://globalsearch.cuny.edu/CFGlobalSearchTool/CFSearchToolController"
    course_terms = {
        '2026%20Spring%20Term': '1262',
        '2026%20Fall%20Term': '1269',
        '2026%20Summer%20Term': '1266'
    }
    course_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://globalsearch.cuny.edu',
            'Referer': 'https://globalsearch.cuny.edu/CFGlobalSearchTool/CFSearchToolController',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            }
    
    # DIRECTORY CONFIG
    directory_source_url = "https://websql.brooklyn.cuny.edu/directory/web_pages_faculty.jsp"

    # CALENDAR CONFIG
    calendar_url = "https://www.brooklyn.edu/registrar/academic-calendars/"

    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        -All three method datas are collecting using scrapy
        """

        # Single functions
        if mode == "course":
            for term_name, term_value in self.course_terms.items():
                payload = f"selectedInstName=Brooklyn%20College%20%7C%20&inst_selection=BKL01&selectedTermName={term_name}&term_value={term_value}&next_btn=Next"
                term_name = term_name.replace('%20',' ')
                yield scrapy.Request(url = self.course_source_url, method = 'POST',headers=self.course_headers,body=payload,callback=self.parse_course, cb_kwargs={'term_name':term_name},dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(
                url=self.directory_source_url,
                callback=self.parse_directory,
                dont_filter=True,
            )

        elif mode == "calendar":
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar,
                dont_filter=True,
            )

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            for term_name, term_value in self.course_terms.items():
                payload = f"selectedInstName=Brooklyn%20College%20%7C%20&inst_selection=BKL01&selectedTermName={term_name}&term_value={term_value}&next_btn=Next"
                term_name = term_name.replace('%20',' ')
                yield scrapy.Request(url = self.course_source_url, method = 'POST',headers=self.course_headers,body=payload,callback=self.parse_course, cb_kwargs={'term_name':term_name},dont_filter=True)
            yield scrapy.Request(
                url=self.directory_source_url,
                callback=self.parse_directory,
                dont_filter=True,
            )

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar,
                dont_filter=True,
            )
            for term_name, term_value in self.course_terms.items():
                payload = f"selectedInstName=Brooklyn%20College%20%7C%20&inst_selection=BKL01&selectedTermName={term_name}&term_value={term_value}&next_btn=Next"
                term_name = term_name.replace('%20',' ')
                yield scrapy.Request(url = self.course_source_url, method = 'POST',headers=self.course_headers,body=payload,callback=self.parse_course, cb_kwargs={'term_name':term_name},dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(
                url=self.directory_source_url,
                callback=self.parse_directory,
                dont_filter=True,
            )
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar,
                dont_filter=True,
            )

        #  All three (default)
        else:
            for term_name, term_value in self.course_terms.items():
                payload = f"selectedInstName=Brooklyn%20College%20%7C%20&inst_selection=BKL01&selectedTermName={term_name}&term_value={term_value}&next_btn=Next"
                term_name = term_name.replace('%20',' ')
                yield scrapy.Request(url = self.course_source_url, method = 'POST',headers=self.course_headers,body=payload,callback=self.parse_course, cb_kwargs={'term_name':term_name},dont_filter=True)
            yield scrapy.Request(
                url=self.directory_source_url,
                callback=self.parse_directory,
                dont_filter=True,
            )
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar,
                dont_filter=True,
            )

    # PARSE COURSE
    def parse_course(self,response, term_name):
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
        subject_blocks = response.xpath('//select[@name="subject_name"]/option[position() > 1]')
        for subject_block in subject_blocks:
            sub_value = subject_block.xpath('.//@value').get('').strip()
            sub_name = subject_block.xpath('.//text()').get('').strip()
            course_blocks = response.xpath('//select[@name="courseCareer"]/option[position() > 1]')
            for course_block in course_blocks:
                course_value = course_block.xpath('.//@value').get('').strip()
                course_name = course_block.xpath('.//text()').get('').strip()
                url = "https://globalsearch.cuny.edu/CFGlobalSearchTool/CFSearchToolController"
                payload = f'selectedSubjectName={sub_name}%2C%20Bilingual%2C%20Spec%20Ed&subject_name={sub_value}&selectedCCareerName={course_name}&courseCareer={course_value}&selectedCAttrName=&courseAttr=&selectedCAttrVName=&courseAttValue=&selectedReqDName=&reqDesignation=&selectedSessionName=&class_session=&selectedModeInsName=&meetingStart=LT&selectedMeetingStartName=less%20than&meetingStartText=&AndMeetingStartText=&meetingEnd=LE&selectedMeetingEndName=less%20than%20or%20equal%20to&meetingEndText=&AndMeetingEndText=&daysOfWeek=I&selectedDaysOfWeekName=include%20only%20these%20days&instructor=B&selectedInstructorName=begins%20with&instructorName=&search_btn_search=Search'
                yield scrapy.Request(url = url, method = 'POST',headers=self.course_headers,body=payload,callback=self.parse_course_final, dont_filter=True)
    
    def parse_course_final(self,response):
        blocks = response.xpath('//td[@data-label="Section"]')
        if blocks:
            for block in blocks:
                detail_page_url = response.urljoin(block.xpath('.//a/@href').get('').strip())
                section = block.xpath('.//a/text()').get('').strip()
                yield scrapy.Request(url = detail_page_url, callback=self.parse_course_detail_page, cb_kwargs={'section':section},dont_filter=True)

    
    def parse_course_detail_page(self,response,section): 
        enrolled = response.xpath('//table[4]//tr[5]/td/span/text()').get('').strip()
        max_capacity = response.xpath('//table[4]//tr[3]/td/span/text()').get('').strip()
        description = response.xpath('//b[contains(text(),"Description")]//following::table/tr/td/text()').get('').strip()
        name = response.xpath('//div[@class="shadowbox"]/p[1]/text()').get('').strip()
        course_dates = response.xpath('//td[@data-label="Meeting Dates"]/text()').get('').strip()
        if course_dates == '-':
            course_dates = ''
        if name:
            self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": re.sub(r'\s+',' ',name),
                    "Course Description": re.sub('\s+',' ',description),
                    "Class Number": response.xpath('//td[contains(text(),"Class Number")]/parent::tr/following-sibling::tr[1]/td/text()').get('').strip(),
                    "Section": section,
                    "Instructor": response.xpath('//td[@data-label="Instructor"]/text()').get('').strip(),
                    "Enrollment": f'{enrolled} of {max_capacity}',
                    "Course Dates": course_dates,
                    "Location": response.xpath('//td[contains(text(),"Location")]/parent::tr/following-sibling::tr[1]/td/text()').get('').strip(),
                    "Textbook/Course Materials": "",
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

        blocks = response.xpath(
            '//table[@class="table table-striped"]/tr'
        )
        for block in blocks:
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.directory_source_url,
                "Name":  block.xpath('.//td[1]/text()').get('').strip(),
                "Title": block.xpath('.//parent::tr/parent::table/thead/tr/th/text()').get('').strip(),
                "Email": '',
                "Phone Number": '',
            })
        
    # PARSE CALENDAR
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
        # get main blocks
        last_date = None
        blocks = response.xpath(
            '//h3[contains(text(),"2026")]/parent::div/table/tbody/tr'
        )  
        # get sub blocks
        for block in blocks:  # iterate through sub blocks
            date = block.xpath(".//td[1]/text()").get('').strip()
            if date:
                last_date = date
            else:
                date = last_date

            self.calendar_rows.append(
                {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": block.xpath("./parent::tbody/parent::table/parent::div/h3/text()").get().replace('Main Academic Calendar','').replace('Main Calendar','').strip(),
                    "Term Date": date,
                    "Term Date Description": block.xpath(".//td[2]//text()").get('').replace('\u2013', ' - ').replace('\u2014', ' - ').strip(),
                }
            )
    

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