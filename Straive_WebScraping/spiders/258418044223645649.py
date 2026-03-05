import re
import io
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class Spider(scrapy.Spider):

    name ="kvcc"
    institution_id =258418044223645649

    course_url = "https://www.kvcc.me.edu/academics/courses/"
    directory_url = "https://www.kvcc.me.edu/about-kvcc/faculty-staff-directory-2/"
    calendar_url = "https://www.kvcc.me.edu/wp-content/uploads/2025/07/Academic-Calendar-2025_2026-V4.pdf"

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
        semesters = response.xpath('//select[@id="year-term-chooser"]/option/@value').getall()
        rows=[]
        for semester in semesters:
            course_year = semester.split("_")[0]
            course_term = semester.split("_")[-1]
            url = f"https://www.kvcc.me.edu/academics/courses/?courseyear={course_year}&courseterm={course_term}"
            response = yield scrapy.Request(url)
            blocks = response.xpath('//table[@id="results"]/tbody/tr')
            for block in blocks:
                course_number = block.xpath('./td[1]/text()').get('').strip()
                course_name = block.xpath('./td[3]/text()').get('').strip()
                # if "BIO 102" in course_number:
                #     breakpoint()
                # breakpoint()
                results = [t.split("in", 1)[1].strip() for t in block.xpath('./td[5]/text()').getall() if "in" in t]
                if results:
                    for result in results:
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Course Name": f"{course_number} {course_name}",
                            "Course Description":  block.xpath('./td[10]/text()').get('').strip(),
                            "Class Number": course_number,
                            "Section": block.xpath('./td[2]/text()').get('').strip(),
                            "Instructor": block.xpath('./td[8]/text()').get('').strip(),
                            "Enrollment": block.xpath('./td[9]/text()').get('').strip(),
                            "Course Dates": block.xpath('./td[6]/text()').get('').strip(),
                            "Location": result,
                            "Textbook/Course Materials":"",
                            })
                else:
                    rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Course Name": f"{course_number} {course_name}",
                            "Course Description":  block.xpath('./td[10]/text()').get('').strip(),
                            "Class Number": course_number,
                            "Section": block.xpath('./td[2]/text()').get('').strip(),
                            "Instructor": block.xpath('./td[8]/text()').get('').strip(),
                            "Enrollment": block.xpath('./td[9]/text()').get('').strip(),
                            "Course Dates": block.xpath('./td[6]/text()').get('').strip(),
                            "Location": "",
                            "Textbook/Course Materials":"",
                            })
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "course")

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
        
        blocks = response.xpath('//table[@id="results"]/tbody/tr')
        rows=[]
        for block in blocks:
            first_name = block.xpath('./td[1]/text()').get('').strip()
            last_name = block.xpath('./td[2]/text()').get('').strip()
            title = block.xpath('./td[3]/text()').get('').strip()
            dept = block.xpath('./td[4]/text()').get('').strip()
            rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": f"{first_name} {last_name}",
                    "Title": f"{title}, {dept}",
                    "Email": block.xpath('./td[6]/text()').get('').strip(),
                    "Phone Number": block.xpath('./td[5]/text()').get('').strip(),
                })
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")


    
    def parse_calendar(self,response):
        
        """
        Parse academic calendar events from a multi-page PDF and normalize them
        into structured calendar records.

        This method reads an academic calendar PDF using pdfplumber and extracts
        dated events by detecting lines that begin with a month-and-day pattern.
        The PDF layout differs between the first page and subsequent pages, so
        page-specific column and parsing logic is applied.

        First page parsing logic:
            - Splits the page into four vertical columns and processes alternating
            columns where event text appears.
            - Merges multi-line event descriptions by buffering text until a new
            date pattern is detected.
            - Skips header and non-event lines.
            - Extracts:
                * Term Date (e.g., "Aug 24")
                * Term Date Description (event text following the date)
            - Assigns events to the "2025–2026 Academic Calendar" term.

        Subsequent page parsing logic:
            - Splits each page into two equal-width columns.
            - Processes each column independently.
            - Skips headers, titles, and month-only labels.
            - Extracts only lines that begin with a valid date pattern.
            - Assigns events to the "2026–2027 Academic Calendar" term.

        Data cleaning and filtering:
            - Uses a compiled regular expression to identify valid date prefixes.
            - Filters out header text and irrelevant lines.
            - Handles wrapped lines and merged descriptions gracefully.

        Data aggregation and persistence:
            - Accumulates all parsed calendar entries into a list of records.
            - Converts the results into a pandas DataFrame.
            - Saves the dataset using `save_df`, keyed by institution ID.

        Args:
            response: Scrapy response object containing the PDF binary data.

        Returns:
            None
        """
        
        DATE_RE = re.compile(r"^(Jan|Feb|Mar|Apr|April|May|Jun|June|Jul|July|Aug|Sept|Sep|Oct|Nov|Dec)\s+\d{1,2}")
        with pdfplumber.open(io.BytesIO(response.body)) as pdf:
            rows=[]
            for page in pdf.pages:
                width, height = page.width, page.height
                if page.page_number == 1:
                    columns = [
                        page.crop((0, 0, width / 4, height)),
                        page.crop((width / 4, 0, width / 2, height)),
                        page.crop((width / 2, 0, width * 0.70, height)),  # end of 3rd
                        page.crop((width * 0.70, 0, width, height)),      # 30% width
                    ]

                    for col in columns[1::2]:
                        text = col.extract_text()
                        lines = text.split("\n")
                        merged_lines = []
                        buffer = ""

                        for line in lines:
                            line = line.strip()

                            # skip header
                            if "Kennebec Valley Community" in line:
                                continue

                            # if line starts with a date → new event
                            if DATE_RE.match(line):
                                if buffer:
                                    merged_lines.append(buffer.strip())
                                buffer = line
                            else:
                                # continuation of previous line
                                buffer += " " + line

                        # append last buffer
                        if buffer:
                            merged_lines.append(buffer.strip())
                        
                        for line in merged_lines:
                            
                            if "dar" in line:
                                continue

                            parts = line.split()
                            term_date = f"{parts[0]} {parts[1]}"
                            term_desc = " ".join(parts[2:])

                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": self.calendar_url,
                                "Term Name": "2025-2026 Academic Calendar",
                                "Term Date": term_date,
                                "Term Date Description": term_desc,
                            })
                else:
                    columns = [
                        page.crop((0, 0, width * 0.5, height)),   # left 50%
                        page.crop((width * 0.5, 0, width, height))  # right 50%
                    ]
                    for col in columns:
                        text = col.extract_text()
                        if not text:
                            continue

                        lines = [l.strip() for l in text.split("\n") if l.strip()]

                        for line in lines:

                            # skip headers
                            if "Kennebec Valley Community" in line:
                                continue
                            if "Academic Calendar" in line:
                                continue

                            # ✅ skip month-only headers (August, September, etc.)
                            if not DATE_RE.match(line):
                                continue

                            term_data = line.split()

                            term_date = f"{term_data[0]} {term_data[1]}"
                            term_desc = " ".join(term_data[2:])

                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": self.calendar_url,
                                "Term Name": "2026-2027 Academic Calendar",
                                "Term Date": term_date,
                                "Term Date Description": term_desc,
                            })


            if rows:
                calendar_df = pd.DataFrame(rows)  # load to dataframe
                save_df(calendar_df, self.institution_id, "calendar")  

                    

       