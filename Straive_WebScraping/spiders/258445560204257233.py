import re
import io
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *
from urllib.parse import quote
from datetime import datetime
from inline_requests import inline_requests


def safe_text(sel, xpath):
    return sel.xpath(xpath).get(default="").strip()

class TluSpider(scrapy.Spider):

    name = "tlu"
    institution_id = 258445560204257233

    course_rows = []
    course_url = [
        "https://tlu-edu.files.svdcdn.com/production/images/general/2025-Fall-Compiled-Schedule-9-9-2025.pdf",
        "https://tlu-edu.files.svdcdn.com/production/images/general/2025-Summer-Compiled-Schedule-5-27-2025.pdf",
        "https://tlu-edu.files.svdcdn.com/production/images/general/Spring-2026-Compiled-Schedule-11-13-2025.pdf"]
    course_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'priority': 'u=0, i',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
    }

    directory_url = "https://www.tlu.edu/directory.json"
    directory_headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'priority': 'u=1, i',
        'referer': 'https://www.tlu.edu/directory/category/faculty',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }
  
    calendar_url = "https://www.tlu.edu/events.json?monthYear=January%202025&category=All"
    calendar_headers = {
    'accept': '*/*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'priority': 'u=1, i',
    'referer': 'https://www.tlu.edu/events',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            for url in self.course_url:
                yield scrapy.Request(url,callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        # If only calendar data is required
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            for url in self.course_url:
                yield scrapy.Request(url,callback=self.parse_course, dont_filter=True)  
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            for url in self.course_url:
                yield scrapy.Request(url,callback=self.parse_course, dont_filter=True)             
            yield scrapy.Request(self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

        # All three (default)
        else:
            for url in self.course_url:
                yield scrapy.Request(url,callback=self.parse_course, dont_filter=True) 
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)


    def parse_course(self, response):
        
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

        with pdfplumber.open(io.BytesIO(response.body)) as pdf:
            if "Summer" in response.url:
                for page in pdf.pages[7:]:
                    tables = page.extract_tables(
                        table_settings={
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "intersection_tolerance": 5,
                        }
                    )
                    for table in tables:
                        for row in table:
                            if "Bldg" in row:
                                continue
                            course_number = row[0].split('.')[0]
                            section =row[0].split('.')[-1]
                            title = re.sub(r'\s+', ' ', row[3]).replace("ASY WEB", "").strip()
                            instructor = row[-3].strip()

                            self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Course Name": f"{course_number} {title}",
                                    "Course Description": "",
                                    "Class Number": course_number or "",
                                    "Section": section or "",
                                    "Instructor": instructor,
                                    "Enrollment": "",
                                    "Course Dates": "",
                                    "Location": "",
                                    "Textbook/Course Materials": '',
                                })
                            
            else:
                skip_list = {
                    "Accounting (Masters)", "African American Studies", "Art",
                    "Athletic Training", "Biology", "Business", "Business Analytics",
                    "Business MBA", "Chemistry", "Communication Studies", "Composition",
                    "Computer Science", "Criminal Justice", "Data Science",
                    "Dramatic Media", "Economics", "Education", "English",
                    "Freshman Experience", "Geography", "Greek", "Hebrew", "History",
                    "Information Systems", "Interdisciplinary Leadership",
                    "Interdisciplinary Studies", "International Studies",
                    "Kinesiology", "Mathematics", "Mexican American Studies",
                    "Music", "Music Education", "Nursing", "Philosophy", "Physics",
                    "Political Science", "Psychology",
                    "Social Innovation & Social Ent", "Sociology", "Spanish",
                    "Statistics", "Theology", "Women's & Gender Studies"
                }

                COURSE_RE = re.compile(r"^[A-Z]{3,4}\s+\d{3}")
                TIME_RE = re.compile(r"\d{1,2}:\d{2}[AP]M")

                for page in pdf.pages[7:]:
                    text = page.extract_text()
                    if not text:
                        continue

                    lines = text.split("\n")
                    current_department = ""
                    last_course = None

                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue

                        # Skip global headers
                        if any(
                            h in line
                            for h in (
                                "Course Schedule",
                                "Academic Year",
                                "Course Codes",
                                "face-to-face",
                            )
                        ):
                            continue

                        # Department headers (ONLY real ones)
                        if (
                            not COURSE_RE.match(line)
                            and not any(char.isdigit() for char in line)
                            and line in skip_list
                        ):
                            last_course = None
                            continue

                        # Course row 
                        if COURSE_RE.match(line):
                            parts = line.split()

                            subject = parts[0]
                            course_num = parts[1]
                            section = parts[2]

                            time_idx = next(
                                (i for i, p in enumerate(parts) if TIME_RE.match(p)),
                                None,
                            )

                            if time_idx:
                                title = " ".join(parts[5:time_idx - 2])
                                instructor = " ".join(parts[time_idx + 4:-2])
                                
                                limit = parts[-2]
                                avail = parts[-1]
                            else:
                                title = " ".join(parts[5:-6])
                                section = f"{parts[2]}{parts[3]}"
                                instructor = " ".join(parts[-4:-2]) if len(parts) > 4 else ""
                                limit = parts[-2] if len(parts) > 2 else ""
                                avail = parts[-1] if len(parts) > 1 else ""
                            if "-" in avail:
                                convert_int = int(limit)-int(avail)
                                enrollment =f"{convert_int}/{limit}" if limit and convert_int else ""
                            else:
                                enrollment = f"{avail}/{limit}" if limit and avail else ""
                            course = {
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Course Name": re.sub(
                                    r"\s+",
                                    " ",
                                    f"{subject} {course_num} {title}".replace("F2F", "").replace("ASY", "").replace("SYN", ""),
                                ).strip(),
                                "Course Description": "",
                                "Class Number": f"{subject} {course_num}",
                                "Section": section,
                                "Instructor": instructor,
                                "Enrollment": enrollment,
                                "Course Dates": "",
                                "Location": "",
                                "Textbook/Course Materials": "",
                            }

                            self.course_rows.append(course)
                            last_course = course
                            continue

                        # ---- Title continuation lines (Research, Studies, Meets 10/27–12/21) ----
                        if last_course:
                            if not line.lower().startswith("cross listed"):
                                last_course["Course Name"] = re.sub(
                                    r"\s+",
                                    " ",
                                    f"{last_course['Course Name']} {line}",
                                ).strip()
                                last_course["Course Name"] = re.sub(r"\s*cross\s*listed\s+as.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*Cross\s+listed\s+as.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*Meets\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*Requires\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*requires\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*Asynchronous\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*F2F\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*meets\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*alternative\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*Alternative\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*study\s*abroad;\s+.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = re.sub(r"\s*Fri.*$","",last_course["Course Name"]).strip()
                                last_course["Course Name"] = last_course["Course Name"].replace("By permission only","").strip()

    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
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
        
        json_datas = response.json() 
        rows=[]
        blocks = json_datas.get('data',[])

        for block in blocks:
            catergory = block.get('category',"").strip()
            if catergory=="Faculty" or catergory =="Staff":
                title = (block.get("job_title") or "").strip()
                dept = block.get("department",[])
                dept = ", ".join([d.strip() for d in dept if d.strip()]).strip()
                if dept:
                    main_title = f"{title}, {dept}"
                else:
                    main_title  = title

                rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": block.get('url',"").strip(),
                        "Name": block.get('title',"").strip(),
                        "Title":main_title,
                        "Email":(block.get("email") or "").strip(),
                        "Phone Number": (block.get("phone") or "").strip(),
                    })
                
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")
    

    
    @inline_requests
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

        months_years = []
        start = datetime(2025, 1, 1)
        end = datetime(2026, 12, 1)

        current = start
        while current <= end:
            months_years.append(current.strftime("%B %Y"))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        rows=[]

        for month in months_years:
            month_year = quote(month)
            url = f"https://www.tlu.edu/events.json?monthYear={month_year}&category=All"
            calendar_headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=1, i',
                'referer': 'https://www.tlu.edu/events',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
            response = yield scrapy.Request(url,headers=calendar_headers,dont_filter=True)
            
            json_datas = response.json()
            blocks = json_datas.get('data',[])

            for block in blocks:
                
                term_date = f"{block.get('day','').strip()}-{block.get('month','').strip()}-{block.get('year','').strip()}"
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": block.get('url','').strip(),
                    "Term Name": "",
                    "Term Date": term_date,
                    "Term Date Description": (block.get("title","") or "").strip()
                })

        if rows:
            calendar_df = pd.DataFrame(rows)
            save_df(calendar_df, self.institution_id, "calendar")

    