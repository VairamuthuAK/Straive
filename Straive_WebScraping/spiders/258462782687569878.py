import re
import io
import json
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class FmccSpider(scrapy.Spider):

    name ="fmcc"
    institution_id =258462782687569878

    course_url = "https://cs.fmcc.edu/index.asp?sem=202509"
    directory_url = "https://fmcc.edu/about-fmcc/faculty-staff"
    calendar_url = "https://fmcc.edu/images/Downloads/AY2025_2026-02-26.pdf"

    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        # If only calendar data is required
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

    @inline_requests
    def parse_course(self,response):

        """
        Scrape course offerings from FMCC by iterating through academic terms,
        enriching course data via multiple backend APIs, and normalizing results
        into a unified course dataset.

        This method:
            - Extracts all available semester options from the term selector.
            - Requests each semester-specific course listing page.
            - Parses base course information from the HTML schedule table.
            - Uses embedded XML data to construct textbook / bookstore URLs.
            - Calls PowerCampus JSON APIs to retrieve detailed section data,
            descriptions, schedules, and enrollment counts.

        Term iteration and base parsing:
            - Iterates over all <option> values in the semester selector.
            - Builds semester-specific URLs and fetches the corresponding
            course listing pages.
            - Extracts course number, instructor, and hidden form values
            needed for downstream API requests.

        Textbook URL construction:
            - Parses department, course number, section, and term values
            from embedded XML strings.
            - Constructs bookstore URLs dynamically using extracted identifiers.

        Section search API workflow:
            - Submits a POST request to the PowerCampus section search endpoint
            using semester-derived period values.
            - Retrieves structured section data including meeting dates,
            seat availability, and schedule metadata.

        Section detail enrichment:
            - Requests detailed section descriptions using section IDs.
            - Extracts long-form course descriptions when available.
            - Handles courses with multiple scheduled locations by emitting
            separate records per location.

        Fallback handling:
            - Writes course identifiers to a local file when no section
            data is returned from the API, allowing for audit or reprocessing.

        Data normalization:
            - Combines event IDs and titles into a standardized course name.
            - Formats enrollment as "seats left / maximum seats".
            - Formats course dates as start–end ranges.
            - Normalizes location fields into a single readable string.

        Data persistence:
            - Aggregates all parsed course records into a single list.
            - Converts the results into a pandas DataFrame.
            - Saves the dataset using `save_df`, keyed by institution ID.

        Args:
            response: Scrapy response object containing the semester selector.

        Returns:
            None
        """
        option_ids = response.xpath('//select[@name="sem"]/option')
        rows=[]
        for option_id in option_ids:

            value = option_id.xpath('./@value').get('').strip()
            semester_list =option_id.xpath('./text()').get('').strip()
            url = f"https://cs.fmcc.edu/index.asp?sem={value}"
            response = yield  scrapy.Request(url, dont_filter=True)
            blocks = response.xpath('//table[@id="table1"]/tr')
            
            for block in blocks[1:]:
                course_number = block.xpath('./td[1]/text()').get('').strip()
                instructor = block.xpath('./td[6]/text()').get('').strip()
                store_id = block.xpath('./td[last()]/form/input[@name="storeId"]/@value').get('').strip()
                course_xml = block.xpath('./td[last()]/form/input[@name="courseXml"]/@value').get('').strip()
                dept = re.findall(r'dept\=\"(.*?)\"',course_xml)[0]
                num = re.findall(r'num\=\"(.*?)\"',course_xml)[0]
                sect = re.findall(r'sect\=\"(.*?)\"',course_xml)[0]
                term = re.findall(r'term\=\"(.*?)\"',course_xml)[0]
                text_book_url =f"https://fmcc.bncollege.com/course-material-listing-page?utm_campaign=storeId={store_id}_langId=-1_courseData={dept}_{num}_{sect}_{term}&utm_source=wcs&utm_medium=registration_integration"
                url = "https://ss.fmcc.edu/PowerCampusSelfService/Sections/Search"
                periods = semester_list.split(" ")
                payload = json.dumps({
                "sectionSearchParameters": {
                    "eventId": course_number,
                    "keywords": "",
                    "period": f"{periods[-1]}/{periods[0]}",
                    "registrationType": "TRAD",
                    "session": "",
                    "campusId": "",
                    "classLevel": "",
                    "college": "",
                    "creditType": "",
                    "curriculum": "",
                    "department": "",
                    "endDate": "",
                    "endTime": "",
                    "eventSubType": "",
                    "eventType": "",
                    "generalEd": "",
                    "meeting": "",
                    "nonTradProgram": "",
                    "population": "",
                    "program": "",
                    "startDate": "",
                    "startTime": "",
                    "status": "",
                    "academicTerm": "",
                    "academicYear": ""
                },
                "startIndex": 0,
                "length": 5
                })
                headers = {
                'accept': 'application/json',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'cache-control': 'max-age=0',
                'content-type': 'application/json',
                'origin': 'https://ss.fmcc.edu',
                'priority': 'u=1, i',
                'referer': f'https://ss.fmcc.edu/PowerCampusSelfService/Search/Section/Share?&eventId={course_number}&period={periods[-1]}%2F{periods[0]}',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
                }
                data_response = yield  scrapy.Request(url,method="POST",headers=headers,body=payload, dont_filter=True)
                json_datas = data_response.json()
                datas = json_datas.get('data',{})
                blocks = datas.get('sections',[])
                if blocks:
                    for blog in blocks:
                        ids = blog.get('id','')
                        url = "https://ss.fmcc.edu/PowerCampusSelfService/Sections/AnonymousDetails"
                        payload = json.dumps(ids)
                        desc_response = yield  scrapy.Request(url,method="POST",headers=headers,body=payload, dont_filter=True)
                        desc_response=desc_response.json() 
                        desc_data= desc_response.get('data',{}) 
                        desc = (desc_data.get("description") or "").strip()
                        locations = blog.get('schedules',[])
                        if locations:
                            for location in locations:
                                rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Course Name": f"{blog.get('eventId','')} {blog.get('eventName','')}",
                                "Course Description": desc,
                                "Class Number": blog.get('eventId',''),
                                "Section": blog.get('section',''),
                                "Instructor": instructor,
                                "Enrollment": f"{blog.get('seatsLeft','')} / {blog.get('maximumSeats','')}",
                                "Course Dates": f"{blog.get('startDate','')} - {blog.get('endDate','')}",
                                "Location": f"{location.get('orgName','')}, {location.get('bldgName','')}, {location.get('roomId','')}",
                                "Textbook/Course Materials": text_book_url
                                })
                        else:
                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Course Name": f"{blog.get('eventId','').strip()} {blog.get('eventName','').strip()}",
                                "Course Description": desc,
                                "Class Number": blog.get('eventId','').strip(),
                                "Section": blog.get('section','').strip(),
                                "Instructor": instructor,
                                "Enrollment": f"{blog.get('seatsLeft','')} / {blog.get('maximumSeats','')}",
                                "Course Dates": f"{blog.get('startDate','')} - {blog.get('endDate','')}",
                                "Location": "",
                                "Textbook/Course Materials": text_book_url
                                })
                else:
                    with open("missing_block.txt","a",encoding="utf-8")as f:
                        f.write(f"{semester_list} - {course_number}\n")
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

        rows=[]
        block_ids = response.xpath('//div[@class="staff-cards uk-margin"]//div[@class="el-content uk-panel uk-margin-top"]/text()').getall()
        for ids in block_ids:
            url = f"https://fmcc.edu/html/helpers/staff_grid.php?id={ids}"
            response = yield  scrapy.Request(url, dont_filter=True)
            phones = response.xpath('//div[@class="uk-margin-small-top"]/text()').getall()
            phone = phones[-1].strip().replace("FMCC","").replace("P: ","").split('-')
            phone = phone[0]+"-"+phone[-1].replace("(","").replace(")","").strip()
            ext_num = phones[0].split(':')[-1].strip()
            if ext_num:
                if "Ext" in ext_num :
                    phone_with_ext = f"{phone}, {ext_num}"
                else:
                    phone_with_ext = f"{phone} Ext. {phones[0].split(':')[-1].strip()}"
            else:
                phone_with_ext = f"{phone}"
            rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": response.xpath('//h5[@class="uk-margin-remove"]/text()').get('').strip(),
                    "Title": response.xpath('//div[contains(@class,"job_title")]/text()').get('').strip(),
                    "Email":response.xpath('//div/a[@class="link-email"]/@title').get("").strip(),
                    "Phone Number": phone_with_ext,
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
         
        with pdfplumber.open(io.BytesIO(response.body)) as pdf:
            rows=[]
            for page in pdf.pages:

                # Extract tables using line-based detection
                tables = page.extract_tables(
                    table_settings={
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines",
                        "intersection_tolerance": 5,
                    }
                )
                for table in tables:
                    for row in table:
                        name=''
                        if page.page_number==1:
                            name = "FALL 2025"
                        elif page.page_number==2:
                            name = "WINTER 2025"
                        elif page.page_number==3:
                            name = "SPRING 2026"
                        elif page.page_number==4:
                            name = "SUMMER 2026"
                        elif page.page_number==5:
                            name = "SUMMER 2026"
                        # skip_row = [,"","8-WEEK EVENING SESSION","10-WEEK SESSION"]
                        if row[-1] =="DAY SESSION I" :
                            continue
                        if row[-1] =="DAY SESSION II" :
                            continue
                        if row[-1] =="8-WEEK EVENING SESSION" :
                            continue
                        if row[-1] =="10-WEEK SESSION" :
                            continue
                        desc = row[-1].strip().replace("–",'-')
                        term_date = f"{row[0].strip()} - {row[1].strip()}".strip()
                        if term_date == "-":
                            continue
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": name,
                            "Term Date":  term_date,
                            "Term Date Description": desc,
                        })
        if rows:
            calendar_df = pd.DataFrame(rows)  # load to dataframe
            save_df(calendar_df, self.institution_id, "calendar") 
   

