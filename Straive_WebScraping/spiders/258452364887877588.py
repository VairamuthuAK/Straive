import re
import os
import scrapy
import pandas as pd
from ..utils import *
from parsel import Selector
from playwright.sync_api import sync_playwright


def normalize_course_number(value, width=3):
    if pd.isna(value):
        return ""

    value_str = str(value).strip()

    # If purely digits → pad
    if value_str.isdigit():
        return value_str.zfill(width)

    # Otherwise return as-is (201L, 101A, etc.)
    return value_str

def extract_instructor_names(text):
    if not text:
        return []

    # Remove term/modality text
    text = re.sub(
        r"\b(1st\s*8\s*weeks|2nd\s*8\s*weeks|traditional|online|hybrid|full\s*term)\b",
        "",
        text,
        flags=re.I
    )

    # Extract valid instructor names: Last, First
    return re.findall(r"[A-Za-z\-']+,\s*[A-Za-z\-']+", text)


def safe_int(value):
    """Convert value to int safely, return None if not possible."""
    try:
        if pd.notna(value):
            return int(value)
        return None
    except (ValueError, TypeError):
        return None

def clean_value(value):
    """Return empty string for NaN, None, 'N/A', or placeholder values."""
    if pd.isna(value):
        return ""
    value = str(value).strip()
    if value.upper() in ["N/A", "-", ""]:
        return ""
    return value


class WmpennSpider(scrapy.Spider):

    name="wmp"
    institution_id = 258452364887877588

    course_url ="https://www.wmpenn.edu/wp-content/uploads/2025/11/SP26.pdf"
    directory_url = "https://www.wmpenn.edu/5905-2/"
    calendar_url ="https://williampenn.us/parents-students/academic-calendar/"

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


    def parse_course(self, response):

        """
        Parse course data from multiple term-specific Excel files and normalize it
        into a unified course dataset.

        This method processes pre-downloaded Excel files corresponding to different
        academic terms (Spring 2026, Fall 2025, Summer 2025). Each file has a distinct
        column layout, so term-specific parsing logic is applied to correctly extract
        course information.

        Core behaviors:
            - Iterates over a configured list of Excel file paths and their source URLs.
            - Skips files that do not exist on disk.
            - Always reads the first worksheet (sheet index 0) from each Excel file.
            - Ignores header, department, and report metadata rows.

        Term-specific parsing logic:
            - Applies different column indexes for course name, instructor, location,
            and enrollment based on the academic term.
            - Normalizes course numbers and safely parses numeric enrollment values.
            - Builds enrollment strings in the format "enrolled/maximum" when possible.

        Course row handling:
            - Detects the start of a new course when required identifying columns
            (course number and department code) are present.
            - Creates a new course record with structured fields including:
                * Course name
                * Section
                * Instructor(s)
                * Enrollment
                * Location
                * Source URL and institution metadata
            - Uses continuation-row inheritance logic when a row lacks identifying
            columns:
                * Appends additional course title text.
                * Merges instructor names without duplication.
                * Fills missing location or enrollment data if present in continuation rows.

        Instructor handling:
            - Supports single or multiple instructors.
            - Normalizes and deduplicates instructor names across continuation rows.
            - Handles instructor data appearing in alternate columns.

        Data aggregation and persistence:
            - Accumulates all parsed course records across all files into a single list.
            - Converts the aggregated data into a pandas DataFrame.
            - Saves the final dataset once using `save_df`, keyed by institution ID.

        Args:
            response: Scrapy response object (not directly used; included for
                    framework compatibility).

        Returns:
            None
        """
        
        file_paths = [
            {
                "path": "GIVE THE FOLLOWING THE EXCEL FILE PATH",
                "source_url": "https://www.wmpenn.edu/wp-content/uploads/2025/11/SP26.pdf",
            },
            {
                "path": "GIVE THE FOLLOWING THE EXCEL FILE PATH",
                "source_url": "https://www.wmpenn.edu/wp-content/uploads/2025/04/FA2025-Course-Schedule.pdf",
            },
            {
                "path": "GIVE THE FOLLOWING THE EXCEL FILE PATH",
                "source_url": "https://www.wmpenn.edu/wp-content/uploads/2025/04/SU2025-Course-schedule.pdf",
            },
        ]

        rows = []

        for cfg in file_paths:
            file_path = cfg["path"]
            source_url = cfg["source_url"]
            if not os.path.exists(file_path):
                continue

            # ✅ ALWAYS read sheet 0
            df = pd.read_excel(file_path, sheet_name=0)
            if "SP26" in file_path :
                previous_course = None

                for _, row in df.iterrows():
                    if "Dept" in row:
                        continue
                    if "Courses in Report" in row:
                        continue
                
                    col0 = row.iloc[0] if len(row) > 0 else None
                    col1 = normalize_course_number(row.iloc[1] if len(row) > 1 else None)
                    col2 = row.iloc[2] if len(row) > 2 else None
                    col6 = row.iloc[6] if len(row) > 6 else None

                    raw_instructor = (
                        clean_value(row.iloc[17]) if len(row) > 17 and clean_value(row.iloc[17])
                        else clean_value(row.iloc[14]) if len(row) > 14
                        else ""
                    )

                    instructor_names = extract_instructor_names(raw_instructor)

                    location = clean_value(row.iloc[21]) if len(row) > 21 else ""

                    maximum = safe_int(row.iloc[34]) if len(row) > 34 else None
                    enrolled = safe_int(row.iloc[36]) if len(row) > 36 else None

                    if enrolled is not None and maximum is not None:
                        enrollment = f"{enrolled}/{maximum}"
                    elif maximum is not None:
                        enrollment = f"0/{maximum}"
                    else:
                        enrollment = ""

                    # 🔹 New course row
                    if pd.notna(col0) and pd.notna(col1):
                        class_number = f"{str(col0).strip()} {str(col1).strip()}"
                        section = str(col2).strip() if pd.notna(col2) else ""
                        course_name = str(col6).strip() if pd.notna(col6) else ""

                        course_data = {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": source_url,
                            "Course Name": f"{class_number} {course_name}".strip(),
                            "Course Description": "",
                            "Class Number": col1,
                            "Section": section,
                            "Instructor": "\n".join(instructor_names),
                            "Enrollment": enrollment,
                            "Course Dates": "",
                            "Location": location,
                            "Textbook/Course Materials": "",
                        }

                        rows.append(course_data)
                        previous_course = course_data

                    # 🔹 Continuation row
                    elif previous_course:
                        if pd.notna(col6):
                            previous_course["Course Name"] += f" {str(col6).strip()}"

                        if instructor_names:
                            existing = (
                                previous_course["Instructor"].split("\n")
                                if previous_course["Instructor"]
                                else []
                            )

                            combined = list(dict.fromkeys(existing + instructor_names))
                            previous_course["Instructor"] = "\n".join(combined)

                        if location and not previous_course["Location"]:
                            previous_course["Location"] = location

                        if enrollment and not previous_course["Enrollment"]:
                            previous_course["Enrollment"] = enrollment

            elif "FA2025" in file_path:
                previous_course = None

                for _, row in df.iterrows():

                    if "Dept" in row:
                        continue
                    if "Courses in Report" in row:
                        continue
                    
                    col0 = row.iloc[0] if len(row) > 0 else None
                    col1 = normalize_course_number(row.iloc[1] if len(row) > 1 else None)
                    col2 = row.iloc[2] if len(row) > 2 else None
                    col6 = row.iloc[5] if len(row) > 5 else None
                    instructor = clean_value(row.iloc[14]) if len(row) > 14 else ""
                    location = clean_value(row.iloc[18]) if len(row) > 18 else ""

                    maximum = safe_int(row.iloc[30]) if len(row) > 30 else None
                    enrolled = safe_int(row.iloc[32]) if len(row) > 32 else None

                    if enrolled is not None and maximum is not None:
                        enrollment = f"{enrolled}/{maximum}"
                    elif maximum is not None:
                        enrollment = f"0/{maximum}"
                    else:
                        enrollment = ""

                    # 🔹 New course row
                    if pd.notna(col0) and pd.notna(col1):
                        class_number = f"{str(col0).strip()} {str(col1).strip()}"
                        section = str(col2).strip() if pd.notna(col2) else ""
                        course_name = str(col6).strip() if pd.notna(col6) else ""

                        course_data = {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": source_url,
                            "Course Name": f"{class_number} {course_name}".strip(),
                            "Course Description": "",
                            "Class Number": col1,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": enrollment,
                            "Course Dates": "",
                            "Location": location,
                            "Textbook/Course Materials": "",
                        }

                        rows.append(course_data)
                        previous_course = course_data

                    # 🔹 Continuation row
                    elif previous_course:
                        if pd.notna(col6):
                            previous_course["Course Name"] += f" {str(col6).strip()}"

                        if instructor:
                            existing = previous_course["Instructor"].split(", ") if previous_course["Instructor"] else []
                            new = instructor.split(", ")
                            previous_course["Instructor"] = ", ".join(dict.fromkeys(existing + new))

                        if location and not previous_course["Location"]:
                            previous_course["Location"] = location

                        if enrollment and not previous_course["Enrollment"]:
                            previous_course["Enrollment"] = enrollment
                            
            elif "SU2025" in file_path:
                for _, row in df.iterrows():

                    if "Dept" in row:
                        continue
                    if "Courses in Report" in row:
                        continue

                    col0 = row.iloc[0] if len(row) > 0 else None
                    col1 = normalize_course_number(row.iloc[1] if len(row) > 1 else None) 
                    col2 = row.iloc[2] if len(row) > 2 else None
                    col6 = row.iloc[5] if len(row) > 5 else None

                    instructor = clean_value(row.iloc[13]) if len(row) > 13 else ""
                    location = clean_value(row.iloc[16]) if len(row) > 16 else ""

                    maximum = safe_int(row.iloc[26]) if len(row) > 26 else None
                    enrolled = safe_int(row.iloc[28]) if len(row) > 28 else None

                    if enrolled is not None and maximum is not None:
                        enrollment = f"{enrolled}/{maximum}"
                    elif maximum is not None:
                        enrollment = f"0/{maximum}"
                    else:
                        enrollment = ""

                    # 🔹 New course row
                    if pd.notna(col0) and pd.notna(col1):
                        class_number = f"{str(col0).strip()} {str(col1).strip()}"
                        section = str(col2).strip() if pd.notna(col2) else ""
                        course_name = str(col6).strip() if pd.notna(col6) else ""

                        course_data = {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": source_url,
                            "Course Name": f"{class_number} {course_name}".strip(),
                            "Course Description": "",
                            "Class Number": col1,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": enrollment,
                            "Course Dates": "",
                            "Location": location,
                            "Textbook/Course Materials": "",
                        }

                        rows.append(course_data)
                        previous_course = course_data

                    # 🔹 Continuation row
                    elif previous_course:
                        if pd.notna(col6):
                            previous_course["Course Name"] += f" {str(col6).strip()}"

                        if instructor:
                            existing = previous_course["Instructor"].split(", ") if previous_course["Instructor"] else []
                            new = instructor.split(", ")
                            previous_course["Instructor"] = ", ".join(dict.fromkeys(existing + new))

                        if location and not previous_course["Location"]:
                            previous_course["Location"] = location

                        if enrollment and not previous_course["Enrollment"]:
                            previous_course["Enrollment"] = enrollment
                        
            # ✅ Save once
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

        total_record = response.xpath('//div[@class="gv-widget-pagination "]/p/text()').get("").split("of")[-1].strip()
        total_pages = round(int(total_record)/25)
        rows=[]
        for total_page in range(1,total_pages+2):
            url = f"https://www.wmpenn.edu/5905-2/?pagenum={total_page}"
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url,  wait_until="networkidle")
                content = page.content()
                browser.close()
                response = Selector(text=content)
                blocks = response.xpath('//table[@class="gv-table-view"]/tbody/tr')
                for block in blocks:
                    first_name = block.xpath('./td[@data-label="First"]/text()').get()
                    last_name = block.xpath('./td[@data-label="Last"]/text()').get()
                    title = block.xpath('./td[@data-label="Title"]/text()').get()
                    dept = block.xpath('./td[@data-label="Department"]/text()').get()
                    email = block.xpath('./td[@data-label="Email"]/a/text()').get()
                    phone = block.xpath('./td[@data-label="Phone"]/a/text()').get()

                    # Clean strings
                    first_name = first_name.strip() if first_name else ""
                    last_name = last_name.strip() if last_name else ""
                    title = title.strip() if title else ""
                    dept = dept.strip() if dept else ""
                    email = email.strip() if email else ""
                    phone = phone.strip() if phone else ""

                    full_name = f"{first_name} {last_name}".strip()
                    full_title = f"{title}, {dept}".strip(", ")

                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": url,
                        "Name": full_name,
                        "Title": full_title,
                        "Email": email,
                        "Phone Number": phone,
                    })

        if rows:
            directory_df = pd.DataFrame(rows)
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

        blogs = response.xpath('//div[@class="wpa-academic-calendar__item"]')
        rows=[]
        for blog in blogs:
            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name":"Academic Calendar",
                "Term Date": blog.xpath('./div[@class="wpa-academic-calendar__item--date"]/text()').get("").strip(),
                "Term Date Description": blog.xpath('./div[@class="wpa-academic-calendar__item--title"]/text()').get("").strip(),

            })
        if rows:
            calendar_df = pd.DataFrame(rows)  # load to dataframe
            save_df(calendar_df, self.institution_id, "calendar") 
              

   