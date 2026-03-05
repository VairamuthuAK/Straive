import re
import os
import io
import scrapy
import unicodedata
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class LincolnuSpider(scrapy.Spider):

    name ="lincolnu"
    institution_id =258426543091509202

    course_url = "https://www.lincoln.edu/academics/academic-affairs/registrar/class-schedules.html"
    directory_url = "https://www.lincoln.edu/directory/index.html"
    calendar_url = "https://www.lincoln.edu/academics/academic-affairs/registrar/academic-calendar.html"

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

    def parse_course(self,response):
        """
        Parse course schedules from multiple Excel files stored in a local directory
        and normalize them into a unified course dataset.

        This method iterates through a directory of Excel files representing
        different academic terms and delivery formats (on-ground, graduate center,
        online). Each file follows a slightly different layout, so file-specific
        parsing logic is applied based on the filename.

        General processing logic:
            - Iterates through all Excel files in the configured directory.
            - Reads each workbook into a pandas DataFrame and replaces NaN values
            with empty strings for consistent processing.
            - Skips empty rows, headers, and non-course metadata rows.
            - Aggregates all parsed course records into a single list.

        On-ground / Graduate Center / Summer files:
            (COURSE_SCHEDULE2025FA.xlsx,
            COURSE_SCHEDULE2025SU_GC-1.xlsx,
            COURSE_SCHEDULE2026SU_GCNEW.xlsx)

            - Skips header rows, session labels, and pagination artifacts.
            - Extracts course number, title, section, instructor, and location.
            - Detects embedded location codes (MC, GC, RL, ONLINE) within the
            course title and separates them into a dedicated Location field.
            - Applies special handling for Graduate Center summer files where
            section and course number formatting differs.
            - Resolves instructor names from alternate columns when needed.
            - Maps each Excel file to its corresponding PDF source URL.

        Spring term files:
            (COURSE_SCHEDULE2025SP-v2.xlsx,
            COURSE_SCHEDULE2026SPNEW.xlsx)

            - Starts parsing after a fixed row offset to bypass front-matter content.
            - Skips headers and pagination rows.
            - Normalizes class numbers and extracts section values when embedded
            in the course code.
            - Extracts instructor and location from fixed columns.
            - Associates records with the correct PDF source URL.

        Online-only summer files:
            (COURSE_SCHEDULE2025SU_OL-1.xlsx,
            COURSE_SCHEDULE2026SU_OLNEW.xlsx)

            - Parses course number and section from a combined course code field.
            - Skips non-course rows and session labels.
            - Extracts instructor, location, and title using online-specific
            column positions.
            - Associates records with the correct online schedule PDF URL.

        Data normalization and persistence:
            - Produces consistent output fields across all file types, including:
                * Course Name
                * Class Number
                * Section
                * Instructor
                * Location
            - Converts all collected rows into a pandas DataFrame.
            - Saves the dataset using `save_df`, keyed by institution ID.

        Args:
            response: Scrapy response object (not directly used; included for
                    framework compatibility).

        Returns:
            None
        """
        excel_dir = "GIVE THE ACTUAL FOLDER PATH"
        rows=[]
        for file_name in os.listdir(excel_dir):
            # Read only Excel files
            if not file_name.lower().endswith((".xlsx", ".xls")):
                continue

            file_path = os.path.join(excel_dir, file_name)
            df = pd.read_excel(file_path, engine="openpyxl")
            df = df.fillna("")
            
            print(f"Reading file: {file_name}")

            if file_name =="COURSE_SCHEDULE2025FA.xlsx" or file_name == "COURSE_SCHEDULE2025SU_GC-1.xlsx" or file_name =="COURSE_SCHEDULE2026SU_GCNEW.xlsx":
                file_name = file_name.replace("xlsx","pdf")
                
                for _, row in df.iloc[1:].iterrows():
                    cells = [str(cell).strip() for cell in row if pd.notna(cell)]
                    if not cells:
                        continue

                    # skip headers / junk rows
                    skip_words = (
                        "COURSE",
                        "Course Schedule",
                        "Session A",
                        "Session B",
                        "Session C",
                        "Page"
                    )

                    if any(word in cell for word in skip_words for cell in cells):
                        continue
                    # breakpoint()
                    title = row[2] if len(row) > 2 else ""
                    loc_lists = ["MC", "GC", "RL", "ONLINE"]

                    course_name = title
                    location = row[3]

                    if title:
                        for loc in loc_lists:
                            if loc in title.split():
                                course_name = title.replace(loc, "").strip()
                                location = loc
                                break

                    section = str(row[1]).strip().split()[0] if len(row) > 1 and pd.notna(row[1]) else ""
                    course_title = f"{row[0]} {course_name}"
                    class_number = row[0]
                    sources_url =""
                    if file_name == "COURSE_SCHEDULE2025SU_GC-1.pdf":
                        sources_url = "https://www.lincoln.edu/_files/academics/Class%20Schedules/COURSE_SCHEDULE2025SU_GC-1.pdf"
                        section = row[0].split(" ")[-1]
                        class_number =row[0].split(' ')[0]
                        course_title =f"{row[0].split(' ')[0]} {course_name}"
                    instructor = row[7] if len(row) > 7 else ""
                    if instructor == "" or instructor == "GRAD":
                        instructor = row[6] if len(row) > 6 else ""
                    # if instructor == "C. Borror":
                    
                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": sources_url or f"https://www.lincoln.edu/academics/academic-affairs/registrar/{file_name}",
                        "Course Name": course_title,
                        "Course Description": "",
                        "Class Number": class_number,
                        "Section": section,
                        "Instructor": instructor,
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": location,
                        "Textbook/Course Materials": ""
                    })
            elif file_name =="COURSE_SCHEDULE2025SP-v2.xlsx" or file_name == "COURSE_SCHEDULE2026SPNEW.xlsx":
                file_name = file_name.replace("xlsx","pdf")
                for _, row in df.iloc[90:].iterrows():
                    cells = [str(cell).strip() for cell in row if pd.notna(cell)]
                    if not cells:
                        continue

                    # skip headers / junk rows
                    skip_words = (
                        "COURSE",
                        "Course Schedule",
                        "Page"
                    )

                    if any(word in cell for word in skip_words for cell in cells):
                        continue
                    section = row[1] if len(row) > 1 else ""
                    if section =="":
                        section = row[0].split(" ")[-1]
                    # breakpoint()
                    class_number = row[0]
                    testing_class_number = class_number.split(" ")
                    if len(testing_class_number) >= 2:
                        class_number = testing_class_number[0]
                    # breakpoint()
                    sources_url=""
                    if file_name =="COURSE_SCHEDULE2025SP-v2.pdf":
                        sources_url = "https://www.lincoln.edu/_files/academics/Class%20Schedules/COURSE_SCHEDULE2025SP-v2.pdf"
                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL":sources_url or f"https://www.lincoln.edu/academics/academic-affairs/registrar/{file_name}",
                        "Course Name": f"{row[0]} {row[3]}",
                        "Course Description": "",
                        "Class Number": class_number,
                        "Section": section,
                        "Instructor": row[8] if len(row) > 8 else "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": row[4] if len(row) > 4 else "",
                        "Textbook/Course Materials": ""
                    })
            elif file_name == "COURSE_SCHEDULE2025SU_OL-1.xlsx" or file_name =="COURSE_SCHEDULE2026SU_OLNEW.xlsx":
                file_name = file_name.replace("xlsx","pdf")
                for _, row in df.iloc[1:].iterrows():
                    cells = [str(cell).strip() for cell in row if pd.notna(cell)]
                    if not cells:
                        continue

                    # skip headers / junk rows
                    skip_words = (
                        "COURSE",
                        "Course Schedule",
                        "Session A",
                        "Session B",
                        "Session C",
                        "Page"
                    )

                    if any(word in cell for word in skip_words for cell in cells):
                        continue
                    course_number =row[0].split(" ")[0]
                    section =row[0].split(" ")[-1]
                    sources_url =""
                    if file_name == "COURSE_SCHEDULE2025SU_OL-1.pdf":
                        sources_url ="https://www.lincoln.edu/_files/academics/Class%20Schedules/COURSE_SCHEDULE2025SU_OL-1.pdf"
                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": sources_url or f"https://www.lincoln.edu/academics/academic-affairs/registrar/{file_name}",
                        "Course Name": f"{course_number} {row[2]}",
                        "Course Description": "",
                        "Class Number": course_number,
                        "Section": section,
                        "Instructor": row[-1],
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": row[3] if len(row) > 3 else "",
                        "Textbook/Course Materials": ""
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

        blocks = response.xpath('//div[contains(@class,"mix directoryListing")]')
        rows=[]
        for block in blocks:
            title= block.xpath('./div/p/text()').get('').strip()
            if title:
                rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Name": block.xpath('./div/a/h3/text()').get('').strip(),
                        "Title": block.xpath('./div/p/text()').get('').strip(),
                        "Email":block.xpath('./div/ul/li/b[contains(text(),"Email:")]/parent::li/a/text()').get("").strip(),
                        "Phone Number":" ".join(block.xpath('./div/ul/li[b[contains(text(),"Phone:")]]//text()').getall()).replace("Phone:", "").strip(),
                    })
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")
            


    @inline_requests
    def parse_calendar(self, response):
        """
        Parse academic calendar events from multiple PDF documents and normalize
        them into structured term-based calendar records.

        This method:
            - Extracts PDF URLs from the response page.
            - Downloads each PDF using inline Scrapy requests.
            - Reads and processes each page using pdfplumber.
            - Identifies academic terms, dates, and event descriptions using
            robust regular-expression–based parsing.

        Term detection and validation:
            - Detects academic term headers such as "Fall Semester 2025",
            "Spring Term 2026", etc.
            - Maintains a rolling `current_term` so that events inherit the most
            recently detected term header.
            - Excludes false-positive term matches that contain administrative
            keywords (e.g., deadlines, registration, grades).

        Line normalization and merging:
            - Normalizes Unicode text to handle special characters and dashes.
            - Merges wrapped lines into single logical events.
            - Preserves standalone month headers while associating them with
            subsequent events.

        Date pattern handling:
            - Supports numeric dates (MM/DD/YYYY).
            - Supports month-based date ranges (e.g., "Aug 24 – Sep 2").
            - Supports month-only labels used as section headers.
            - Splits lines containing multiple date ranges into separate events.

        Event parsing and cleanup:
            - Extracts the date portion and remaining event description.
            - Removes weekday names and extraneous punctuation.
            - Normalizes dash characters and whitespace.
            - Skips empty or non-informational event descriptions.
            - Cleans institutional header text embedded in descriptions.

        Data aggregation and deduplication:
            - Accumulates all parsed calendar events across PDFs into a single list.
            - Converts the results into a pandas DataFrame.
            - Removes duplicate entries based on term, date, and description
            (ignoring source URL differences).
            - Saves the final dataset using `save_df`, keyed by institution ID.

        Args:
            response: Scrapy response object containing links to academic
                    calendar PDF files.

        Returns:
            None
        """
        
        urls = response.xpath('//div[@class="content"]//a/span/parent::a/@href').getall()
        rows = []

        # ---------------- REGEX ----------------
        term_pattern = re.compile(
            r'\b(Fall|Spring|Summer|Winter)\s+(Semester|Term)\s+\d{4}',
            re.IGNORECASE
        )
        INVALID_TERM_WORDS = re.compile(
                r'\b(Deadline|Application|Classes|Examination|Grades|Registration)\b',
                re.IGNORECASE
            )
        numeric_date = re.compile(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b')

        month_range = re.compile(
            r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s*\d{1,2}\s*[‐–-]\s*'
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)?[a-z]*\s*\d{1,2}\b',
            re.IGNORECASE
        )

        month_only = re.compile(
            r'\b(January|February|March|April|May|June|July|August|September|October|November|December|'
            r'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\b',
            re.IGNORECASE
        )

        weekday_cleanup = re.compile(
            r'\b(Mon|Tues?|Wed|Thurs?|Fri|Sat|Sun)\b',
            re.IGNORECASE
        )

        current_term = ""

        for url in urls:
            pdf_response = yield scrapy.Request(url, dont_filter=True)

            with pdfplumber.open(io.BytesIO(pdf_response.body)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue

                    lines = [
                        unicodedata.normalize("NFKD", l).strip()
                        for l in text.split("\n") if l.strip()
                    ]
                    # breakpoint()
                    merged_lines = []
                    buffer = ""

                    for line in lines:
                        # Term header
                        if term_pattern.search(line):
                            if buffer:
                                merged_lines.append(buffer)
                                buffer = ""
                            merged_lines.append(line)
                            continue

                        # Month-only event
                        if month_only.search(line) and not numeric_date.search(line):
                            if buffer:
                                merged_lines.append(buffer)
                                buffer = ""
                            merged_lines.append(line)
                            continue

                        # Line contains date → MERGE with buffer
                        if numeric_date.search(line) or month_range.search(line):
                            if buffer:
                                merged_lines.append(f"{buffer} {line}".strip())
                                buffer = ""
                            else:
                                merged_lines.append(line)
                        else:
                            buffer = f"{buffer} {line}".strip() if buffer else line

                    if buffer:
                        merged_lines.append(buffer)


                    # ----------- SPLIT MULTI-DATE LINES -----------
                    final_lines = []
                    for line in merged_lines:
                        ranges = list(month_range.finditer(line))
                        if len(ranges) <= 1:
                            final_lines.append(line)
                        else:
                            start = 0
                            for r in ranges:
                                chunk = line[start:r.end()].strip()
                                final_lines.append(chunk)
                                start = r.end()
                            if start < len(line):
                                final_lines.append(line[start:].strip())

                    # --------------- PARSE ----------------

                    # breakpoint()
                    for line in final_lines:
                        term_match = term_pattern.search(line)
                        if term_match and not INVALID_TERM_WORDS.search(line):
                            current_term = term_match.group(0).strip()
                            line = line.replace(current_term, "").strip()

                            # if line still has content, DO NOT skip
                            if not line:
                                continue


                        if not current_term:
                            continue

                        term_date = ""
                        desc = ""

                        if numeric_date.search(line):
                            m = numeric_date.search(line)
                            term_date = m.group(0)
                            desc = line.replace(term_date, "").strip()

                        elif month_range.search(line):
                            m = month_range.search(line)
                            term_date = m.group(0)
                            desc = line.replace(term_date, "").strip()

                        elif month_only.search(line):
                            term_date = ""
                            desc = line.strip()

                            # 🔥 REMOVE TRAILING MONTH NAME
                            desc = re.sub(
                                r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\b$',
                                '',
                                desc,
                                flags=re.I
                            ).strip()

                        else:
                            continue

                        desc = weekday_cleanup.sub("", desc)
                        desc = re.sub(r'\s{2,}', ' ', desc).strip(" -‐–")

                        if not desc:
                            continue
                        if "Lincoln University Academic Calendar" in desc:
                            desc = desc.replace('Lincoln University Academic Calendar ','')
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_response.url,
                            "Term Name": current_term,
                            "Term Date": term_date.replace("‐", "-").replace("–", "-"),
                            "Term Date Description": desc.replace("‐", "-").replace("–", "-"),
                        })

        if rows:
            calendar_df = pd.DataFrame(rows)

            # Drop duplicates WITHOUT comparing Source URL
            calendar_df = calendar_df.drop_duplicates(
                subset=[
                    "Cengage Master Institution ID",
                    "Term Name",
                    "Term Date",
                    "Term Date Description"
                ],
                keep="first"
            )

            save_df(calendar_df, self.institution_id, "calendar")

                        
    