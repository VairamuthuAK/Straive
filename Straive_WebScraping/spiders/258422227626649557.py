import re
import io
import scrapy
import base64
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class SchreinerSpider(scrapy.Spider):

    name = "sch"
    institution_id = 258422227626649557

    course_url ="https://schreiner.edu/academics/course-schedule/"
    directory_url = "https://schreiner.edu/su-directory/?wpbdp_view=all_listings"
    calendar_url ="https://schreiner.edu/academics/academic-calendar/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.directory_rows = []

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,callback=self.parse_course)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)
            
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url,callback=self.parse_course)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url,callback=self.parse_course)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        else:
            yield scrapy.Request(url=self.course_url,callback=self.parse_course)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)


    @inline_requests
    def parse_course(self,response):

        """
        Parse course schedules from downloadable PDF links by processing
        corresponding term-specific Excel files and normalizing the data
        into a unified course dataset.

        This method:
            - Extracts PDF download links from the response page.
            - Requests each PDF URL to determine the academic term.
            - Uses pre-generated Excel files (derived from the PDFs) for
            structured data extraction rather than parsing PDF text directly.

        PDF and file handling:
            - Identifies Winter 2025 schedules by filename in the PDF URL and
            applies Winter-specific parsing logic.
            - Uses Spring 2026 Excel schedules for all other PDFs.
            - Reads only specific worksheet indexes relevant to each term.

        Winter 2025 parsing logic:
            - Iterates through a single worksheet.
            - Skips rows without a valid course code.
            - Constructs class number and section from the course code field.
            - Combines multiple title columns into a single course name.
            - Applies inheritance logic for:
                * Instructor names when instructor cells are empty.
                * Course date ranges when start or end dates are missing.
            - Formats course dates as "YYYY-MM-DD - YYYY-MM-DD".

        Spring 2026 parsing logic:
            - Iterates through multiple worksheets representing departments.
            - Detects continuation rows (e.g., rows starting with "TRAV"
            or missing course codes).
            - Appends additional descriptive text from continuation rows to
            the previous course description.
            - Creates a new course entry when a valid course code is found.
            - Extracts course title, section, and instructor information.

        Data aggregation and persistence:
            - Accumulates all parsed course records into a single list.
            - Normalizes all records into a consistent schema.
            - Converts the results into a pandas DataFrame.
            - Saves the dataset once using `save_df`, keyed by institution ID.

        Args:
            response: Scrapy response object containing PDF download links.

        Returns:
            None
        """
            
        pdf_links =response.xpath('//span[contains(text(),"Download ")]/parent::a/@href').getall()
        rows =[]

        for pdf_link in pdf_links:

            url = f"https://schreiner.edu{pdf_link}"
            response = yield scrapy.Request(url=url)

            with pdfplumber.open(io.BytesIO(response.body)) as pdf:
                
                if "Winter-2025.pdf"in response.url:
                    file_path = "D:\Lapis\Scrapy_works\schreiner\schreiner\Course-Schedule-Winter-2025.xlsx"
                    sheet_indexes = [1]

                    dfs = pd.read_excel(
                        file_path,
                        sheet_name=sheet_indexes
                    )
                    
                    for sheet_idx, df in dfs.items():

                        previous_instructor = None
                        previous_course_date = None

                        for _, row in df.iterrows():
                            if pd.isna(row[2]):
                                continue

                            parts = str(row[2]).split()
                            class_number = f"{parts[0]} {parts[1]}" if len(parts) >= 2 else str(row[2])
                            section = parts[-1] if parts else ""

                            part1 = str(row[3]).strip() if pd.notna(row[3]) else ""
                            part2 = str(row[4]).strip() if pd.notna(row[4]) else ""
                            course_name = f"{part1} {part2}".strip()

                            if pd.notna(row[6]) or pd.notna(row[7]):
                                instructor = f"{str(row[6]).strip()} {str(row[7]).strip()}".strip()
                                previous_instructor = instructor
                            else:
                                instructor = previous_instructor

                            start_date = pd.to_datetime(row[8], errors="coerce")
                            end_date = pd.to_datetime(row[9], errors="coerce")

                            if pd.notna(start_date) and pd.notna(end_date):
                                course_date = f"{start_date:%Y-%m-%d} - {end_date:%Y-%m-%d}"
                                previous_course_date = course_date
                            else:
                                course_date = previous_course_date

                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": url,
                                "Course Name": f"{class_number} {course_name}".strip(),
                                "Course Description": "",
                                "Class Number": class_number,
                                "Section": section,
                                "Instructor": instructor,
                                "Enrollment": "",
                                "Course Dates": course_date,
                                "Location": "",
                                "Textbook/Course Materials": ""
                            })

                else:
                    file_path = "D:\Lapis\Scrapy_works\schreiner\schreiner\Course-Schedule-Spring-2026.xlsx"
                    sheet_indexes = [6, 8, 10, 12, 14, 16, 18, 20]

                    dfs = pd.read_excel(
                        file_path,
                        sheet_name=sheet_indexes
                    )

                    previous_course = None

                    for sheet_idx, df in dfs.items():
                        for idx, row in df.iterrows():

                            raw_code = row[2]

                            # 🔹 Continuation row (TRAV info)
                            if pd.isna(raw_code) or str(raw_code).startswith("TRAV"):
                                if previous_course:
                                    extra_desc = str(row[3]).strip() if pd.notna(row[3]) else ""
                                    note = str(row[4]).strip() if pd.notna(row[4]) else ""
                                    previous_course["Course Description"] += f" {extra_desc} {note}".strip()
                                continue

                            # 🔹 Normal course row
                            parts = str(raw_code).split()
                            if len(parts) < 2:
                                continue

                            course_code = f"{parts[0]} {parts[1]}"
                            section = parts[-1]

                            course_title = row[3] if pd.notna(row[3]) else row[4]

                            instructor = f"{row[-2]} {row[-1]}".strip()

                            course_data = {
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": url,
                                "Course Name": f"{course_code} {course_title}",
                                "Course Description": "",
                                "Class Number": course_code,
                                "Section": section,
                                "Instructor": instructor,
                                "Enrollment": "",
                                "Course Dates": "",
                                "Location": "",
                                "Textbook/Course Materials": ""
                            }

                            rows.append(course_data)
                            previous_course = course_data

        if rows:
            course_df = pd.DataFrame(rows)
            save_df(course_df, self.institution_id, "course")    
                

    @inline_requests
    def parse_directory(self, response):

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

        blocks = response.xpath('//div[@class="listing-details"]')

        for block in blocks:
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": block.xpath(
                    './div/div/a/parent::div/parent::div[contains(@class,"wpbdp-field-campus_map")]//a/text()'
                    ' | ./div[contains(@class,"wpbdp-field-name wpbdp")]/div/text()'
                ).get("").strip(),
                "Title": ", ".join(
                    block.xpath(
                        './/div[contains(@class,"wpbdp-field-department")]/div/text()'
                        ' | .//div[contains(@class,"wpbdp-field-title wpbdp")]/div/text()'
                    ).getall()
                ).strip(),
                "Email": block.xpath('./div/div/a[contains(@href,"mailto")]/text()').get("").strip(),
                "Phone Number": block.xpath('./div/div/a[contains(@href,"tel")]/text()').get("").strip(),
            })

        next_page_link = response.xpath(
            '//div[@class="wpbdp-pagination"]/span[@class="next"]/a/@href'
        ).get()

        if next_page_link:
            yield response.follow(next_page_link, callback=self.parse_directory)
        else:
            # ✅ LAST PAGE — save once
            if self.directory_rows:
                directory_df = pd.DataFrame(self.directory_rows)
                save_df(directory_df, self.institution_id, "campus")

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

        tabs = response.xpath('//div[contains(@aria-labelledby,"fusion-tab")]')
        rows=[]
        for tab in tabs:
            table_rows = tab.xpath('./div/table/tbody/tr')
            for table_row in table_rows:
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name":tab.xpath('./p/strong/text() | ./strong/text()').get("").strip(),
                    "Term Date": " ".join(table_row.xpath('./td[1]/text() | ./td[2]/text()').getall()).strip(),
                    "Term Date Description": " ".join(table_row.xpath('./td[4]/text() | ./td[5]/text()').getall()).strip(),

                })
        if rows:
            calendar_df = pd.DataFrame(rows)  # load to dataframe
            save_df(calendar_df, self.institution_id, "calendar") 




    