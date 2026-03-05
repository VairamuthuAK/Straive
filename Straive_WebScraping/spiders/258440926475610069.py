import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO


class CopperSpider(scrapy.Spider):

    name = "copper"
    institution_id = 258440926475610069
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_source_url = "https://www.cmccd.edu/wp-content/uploads/2026/02/Catalog-2025-26-Final-02.04.26.pdf"

    # DIRECTORY CONFIG
    directory_source_url = 'https://www.cmccd.edu/directory/'

    # CALENDAR CONFIG
    calendar_source_url = "https://cmccd.community.diligentoneplatform.com/document/cda99880-1168-43b0-a583-bc8360ead252/"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course datas are availble in the pdf so 
        using pdfplumber for getting data.

        - Directory URL is return proper response so using scrapy
        for collecting data.

        - Calendar datas are availble in the pdf so 
        using pdfplumber for getting data.
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
           yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)
            self.parse_calendar()

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)
            self.parse_calendar()

    # PARSE COURSE
    def parse_course(self):
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
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(self.course_source_url, headers=headers, verify=False)
        
        exclude_keywords = ["Units", "Program", "CMC GE", "Formerly", "Sect", "Meeting", "Deadline", "hrs/wk", "Time"]

        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for i in range(129, 170): 
                if i == 11:
                    continue
                    
                page = pdf.pages[i]
                width = page.width
                # Split the page into left and right halves
                parts = [page.crop((0, 0, width/2, page.height)), page.crop((width/2, 0, width, page.height))]

                for section_crop in parts:
                    text = section_crop.extract_text()
                    if not text: continue
                    
                    lines = text.split('\n') # Split extracted text into lines
                    curr_course, curr_sect, curr_loc = "", "", ""
                    curr_desc = []

                    for line in lines:
                        line = line.strip()
                        if not line: continue

                        course_match = re.match(r'^([A-Z]{1,}\s+[C\d]+[A-Z]*)\s+([A-Z].*)', line)
                        if course_match:
                            curr_course = f"{course_match.group(1)} {course_match.group(2)}"
                            curr_sect, curr_loc, curr_desc = "", "", []
                            continue

                        sect_match = re.match(r'^(\d+[A-Z\d]*)\s+([A-Za-z\-]+(?:\s+[A-Z\d]+)?(?:\s+LMR)?)', line)
                        if sect_match:
                            curr_sect = sect_match.group(1)
                            curr_loc = sect_match.group(2).strip()
                        
                        elif curr_course and not curr_sect:
                            if any(k in line for k in ["Units", "CSU", "UC", "Applicable", "Prerequisite"]):
                                curr_desc.append(line)

                
                        inst_match = re.search(r'([A-Z]\.\s[A-Z][a-z]+(?:/[A-Z]\.\s[A-Z][a-z]+)?|[A-Z][a-z]+/[A-Z][a-z]+|[A-Z]\.\s[A-Z][a-z]+)$', line)
                        
                        if curr_sect and inst_match:
                            instructor_name = inst_match.group(1).strip()
                            
                            if any(word in instructor_name for word in exclude_keywords):
                                continue
                            
                            if 'Online-A' in curr_loc: curr_loc = 'Online-A'
                            if 'TBA' in curr_loc: curr_loc = 'TBA'
                            if 'Bell ' in curr_loc: curr_loc = 'Bell Center 637'
                            if curr_loc == 'Canvas': curr_loc = 'Canvas/Zoom & Online-AS'
                            if curr_loc == 'Dates': curr_loc = ''
                            if curr_loc == 'Online 3': curr_loc = 'Online'
                            if curr_loc == 'Online-HYB 14': curr_loc = 'Online-HYB'

                            if curr_course == 'AE 300A BASIC COLLEGE MATHEMATICS': curr_loc = 'Base 1530-204'
                            if curr_course == 'AE 300B BASIC COLLEGE ENGLISH': curr_loc = 'Base 1530-204'
                            if curr_course == 'AE 300A BASIC COLLEGE MATHEMATICS': curr_loc = 'MB 1530-204'
                            if curr_course == 'AE 301 SUPPLEMENTAL READING': curr_loc = 'AEP West 210'
                            if curr_course == 'CNST 010 BASIC CONSTRUCTION PRINCIPLES' or curr_course == 'CNST 060 PLUMBING FUNDAMENTALS' or curr_course == 'CNST 080 FINISH CARPENTRY': curr_loc = '29HS 115'
                            if curr_course == 'CNST 110 NONCREDIT BASIC CONSTRUCTION PRINCIPLES' or curr_course == 'CNST 160 NONCREDIT PLUMBING FUNDAMENTALS' or curr_course == 'CNST 180 NONCREDIT FINISH CARPENTRY': curr_loc = '29HS 115'
                            if curr_course == 'CNST 110 NONCREDIT BASIC CONSTRUCTION PRINCIPLES' or curr_course == 'CNST 160 NONCREDIT PLUMBING FUNDAMENTALS' or curr_course == 'CNST 180 NONCREDIT FINISH CARPENTRY': curr_loc = '29HS 115'
                            if curr_course == 'AE 302 ENGLISH AS A SECOND LANGUAGE I': curr_loc = 'AEP East'
                            if curr_course == 'AE 305 ENGLISH AS A SECOND LANGUAGE II': curr_loc = 'AEP East'


                            if curr_course == 'CNST 110 NONCREDIT BASIC CONSTRUCTION PRINCIPLES': curr_sect = '20E'
                            if curr_course == 'CNST 160 NONCREDIT PLUMBING FUNDAMENTALS': curr_sect = '20'
                            if curr_course == 'CNST 180 NONCREDIT FINISH CARPENTRY': curr_sect = '30'
                            if curr_course == 'AE 302 ENGLISH AS A SECOND LANGUAGE I': curr_sect = 'TP01'
                            if curr_course == 'AE 302 ENGLISH AS A SECOND LANGUAGE II': curr_sect = 'TP01'
                
                            if curr_course:
                                if curr_sect == '0':
                                    continue
                                c_split = curr_course.split()
            
                                class_num = f'{c_split[0]} {c_split[1]}'
                            

                                self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": self.course_source_url,
                                    "Course Name": curr_course,
                                    "Course Description": '',
                                    "Class Number": class_num,
                                    "Section": curr_sect,
                                    "Instructor": instructor_name,
                                    "Enrollment": '',
                                    "Course Dates": '',
                                    "Location": curr_loc,
                                    "Textbook/Course Materials": '',
                                })

        manual_entries = [
        {"Course Name": "ASC 005A TUTOR TRAINING", "Section": "40", "Location": "TASC 112", "Instructor": "D. Charbonneau", "Date": "", "Description": "0 Units Prerequisite..."},
        {"Course Name": "ASC 100 SUPERVISED TUTORING", "Section": "01", "Location": "TASC 112", "Instructor": "D. Charbonneau", "Date": "", "Description": "0 Units Note..."},
        {"Course Name": "ASC 100 SUPERVISED TUTORING", "Section": "02", "Location": "MESA 111", "Instructor": "S. Rodriguez", "Date": "", "Description": "0 Units Note..."},
        
        # ASC 101A - Library Orientation
        {"Course Name": "ASC 101A LIBRARY WORKSHOP - LIBRARY ORIENTATION", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "02/05", "Description": "0 Units"},
        {"Course Name": "ASC 101A LIBRARY WORKSHOP - LIBRARY ORIENTATION", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "02/11", "Description": "0 Units"},
        {"Course Name": "ASC 101A LIBRARY WORKSHOP - LIBRARY ORIENTATION", "Section": "01", "Location": "Campus LMR", "Instructor": "K. Schott", "Date": "02/19", "Description": "0 Units"},
        {"Course Name": "ASC 101A LIBRARY WORKSHOP - LIBRARY ORIENTATION", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "03/02", "Description": "0 Units"},
        {"Course Name": "ASC 101A LIBRARY WORKSHOP - LIBRARY ORIENTATION", "Section": "01", "Location": "Campus LMR", "Instructor": "A. Pennington", "Date": "04/11", "Description": "0 Units"},
        {"Course Name": "ASC 101A LIBRARY WORKSHOP - LIBRARY ORIENTATION", "Section": "50", "Location": "Online-A", "Instructor": "Monypeny/Schott", "Date": "", "Description": "0 Units"},

        # ASC 101B - Search Strategies
        {"Course Name": "ASC 101B LIBRARY WORKSHOP - SEARCH STRATEGIES", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "02/04", "Description": "0 Units"},
        {"Course Name": "ASC 101B LIBRARY WORKSHOP - SEARCH STRATEGIES", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "02/12", "Description": "0 Units"},
        {"Course Name": "ASC 101B LIBRARY WORKSHOP - SEARCH STRATEGIES", "Section": "01", "Location": "Campus LMR", "Instructor": "K. Schott", "Date": "03/06", "Description": "0 Units"},
        {"Course Name": "ASC 101B LIBRARY WORKSHOP - SEARCH STRATEGIES", "Section": "01", "Location": "Campus LMR", "Instructor": "K. Schott", "Date": "03/19", "Description": "0 Units"},
        {"Course Name": "ASC 101B LIBRARY WORKSHOP - SEARCH STRATEGIES", "Section": "01", "Location": "Campus LMR", "Instructor": "A. Pennington", "Date": "04/18", "Description": "0 Units"},
        {"Course Name": "ASC 101B LIBRARY WORKSHOP - SEARCH STRATEGIES", "Section": "50", "Location": "Online-A", "Instructor": "Monypeny/Schott", "Date": "", "Description": "0 Units"},

        # ASC 101C - Evaluating Sources
        {"Course Name": "ASC 101C LIBRARY WORKSHOP - EVALUATING SOURCES", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "02/20", "Description": "0 Units"},
        {"Course Name": "ASC 101C LIBRARY WORKSHOP - EVALUATING SOURCES", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "03/10", "Description": "0 Units"},
        {"Course Name": "ASC 101C LIBRARY WORKSHOP - EVALUATING SOURCES", "Section": "01", "Location": "Campus LMR", "Instructor": "K. Schott", "Date": "04/09", "Description": "0 Units"},
        {"Course Name": "ASC 101C LIBRARY WORKSHOP - EVALUATING SOURCES", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "04/17", "Description": "0 Units"},
        {"Course Name": "ASC 101C LIBRARY WORKSHOP - EVALUATING SOURCES", "Section": "01", "Location": "Campus LMR", "Instructor": "A. Pennington", "Date": "05/02", "Description": "0 Units"},
        {"Course Name": "ASC 101C LIBRARY WORKSHOP - EVALUATING SOURCES", "Section": "50", "Location": "Online-A", "Instructor": "Monypeny/Schott", "Date": "", "Description": "0 Units"},

        # ASC 101D - APA Format
        {"Course Name": "ASC 101D LIBRARY WORKSHOP - APA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "A. Pennington", "Date": "02/23", "Description": "0 Units"},
        {"Course Name": "ASC 101D LIBRARY WORKSHOP - APA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "A. Pennington", "Date": "03/14", "Description": "0 Units"},
        {"Course Name": "ASC 101D LIBRARY WORKSHOP - APA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "03/24", "Description": "0 Units"},
        {"Course Name": "ASC 101D LIBRARY WORKSHOP - APA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "K. Schott", "Date": "04/16", "Description": "0 Units"},
        {"Course Name": "ASC 101D LIBRARY WORKSHOP - APA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "05/07", "Description": "0 Units"},
        {"Course Name": "ASC 101D LIBRARY WORKSHOP - APA FORMAT", "Section": "50", "Location": "Online-A", "Instructor": "Monypeny/Schott", "Date": "", "Description": "0 Units"},

        # ASC 101E - MLA Format
        {"Course Name": "ASC 101E LIBRARY WORKSHOP - MLA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "02/26", "Description": "0 Units"},
        {"Course Name": "ASC 101E LIBRARY WORKSHOP - MLA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "D. Monypeny", "Date": "03/16", "Description": "0 Units"},
        {"Course Name": "ASC 101E LIBRARY WORKSHOP - MLA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "K. Schott", "Date": "04/08", "Description": "0 Units"},
        {"Course Name": "ASC 101E LIBRARY WORKSHOP - MLA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "K. Schott", "Date": "04/23", "Description": "0 Units"},
        {"Course Name": "ASC 101E LIBRARY WORKSHOP - MLA FORMAT", "Section": "01", "Location": "Campus LMR", "Instructor": "A. Pennington", "Date": "05/09", "Description": "0 Units"},
        {"Course Name": "ASC 101E LIBRARY WORKSHOP - MLA FORMAT", "Section": "50", "Location": "Online-A", "Instructor": "Monypeny/Schott", "Date": "", "Description": "0 Units"},

        # Other Courses
        {"Course Name": "ASC 111 MESA ORIENTATION COURSE", "Section": "50LS", "Location": "Online-A", "Instructor": "Staff", "Date": "02/17", "Description": "0 Units"},
        {"Course Name": "AE 301 SUPPLEMENTAL READING", "Section": "YV01", "Location": "AEP West 210", "Instructor": "L. Cutler", "Date": "", "Description": "0 Units"},
        {"Course Name": "AE 300B BASIC COLLEGE ENGLISH", "Section": "YV01", "Location": "AEP West 210", "Instructor": "J. Hiza-Hong", "Date": "", "Description": "0 Units"},
        {"Course Name": "AE 317 COLLEGE PREPARATORY READING", "Section": "YV01", "Location": "AEP West 210", "Instructor": "J. Hiza-Hong", "Date": "", "Description": "0 Units"},
        {"Course Name": "AE 300B BASIC COLLEGE ENGLISH", "Section": "2901", "Location": "MB 1530-204", "Instructor": "R. Fischer", "Date": "", "Description": "0 Units"},
        {"Course Name": "AE 300B BASIC COLLEGE ENGLISH", "Section": "2902", "Location": "MB 1530-204", "Instructor": "R. Fischer", "Date": "", "Description": "0 Units"}
    ]

        for entry in manual_entries:
            c_name = entry["Course Name"]
            c_split = c_name.split()
            c_num = f'{c_split[0]} {c_split[1]}' if len(c_split) > 1 else c_name
            
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_source_url,
                "Course Name": c_name,
                "Course Description": '',
                "Class Number": c_num,
                "Section": entry["Section"],
                "Instructor": entry["Instructor"],
                "Enrollment": '',
                "Course Dates": '',
                "Location": entry["Location"],
                "Textbook/Course Materials": '',
            })

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
        urls = response.xpath('//h2/a/@href').getall()
        for url in urls:
            yield scrapy.Request(url,callback=self.parse_directory_final)
    
    def parse_directory_final(self,response):
            name = ' '.join(response.xpath('//h2//text()').getall()).strip()
            tit = response.xpath('//span[@class="title notranslate"]/text()').get('').strip()
            dep = response.xpath('//span[@class="organization-unit notranslate"]/text()').get('').strip()

            if tit and dep:
                title = f'{tit}, {dep}'
            elif tit:
                title = tit
            elif dep:
                title = dep
            else:
                title = ''

            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": re.sub(r'\s+',' ',name),
            "Title": re.sub(r'\s+',' ',title),
            "Email": response.xpath('//span[@class="email-address-block"]/span/span[@class="email-address"]/a/text()').get('').strip(),
            "Phone Number": response.xpath('//span[@class="phone-number-block"]/span/span[@class="value"]/text()').get('').replace('VRS','').strip(),
            })
        
    # PARSE CALENDAR
        
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
        def clean_final_desc(text):
            if not text:
                return ""

            # --- Exact hard fixes ---
            exact_fixes = {
                'Holiday, College Closed ollege': 'Holiday, College Closed',
                'Winter Break, College Closed u ce Day // - //': 'Winter Break, College Closed',
                'Winter Session Grades Due ses in e y s May 2025- // - //': 'Winter Session Grades Due',
                'Stafff / Faculty Service Day, e Open College Closed': 'Staff / Faculty Service Day, College Closed',
                'Faculty Service Days, College Open sed,': 'Faculty Service Days, College Open'
            }
            if text in exact_fixes:
                return exact_fixes[text]

            # --- Contains-based fixes ---
            if 'Falll Break' in text:
                return 'Fall Break, No Classes, College Open'

            # --- OCR / spelling fixes ---
            fixes = {
                r"\bebruary\b": "February",
                r"\barch\b": "March",
                r"\bril\b": "April",
                r"\bay\b": "May",
                r"\bune\b": "June",
                r"\buly\b": "July",
                r"\bust\b": "August",
                r"Colege": "College",
                r"Sesion": "Session",
                r"Clases": "Classes",
                r"Staf+": "Staff",
                r"Asigned": "Assigned",
                r"Degre": "Degree",
                r"Fal+": "Fall",
                r"Sumer": "Summer",
                r"Comencement": "Commencement"
            }
            for wrong, right in fixes.items():
                text = re.sub(wrong, right, text, flags=re.IGNORECASE)

            # --- Junk cleanup ---
            junk = ["// - //", "252", "3791", "1398", "endar", "92252"]
            for j in junk:
                text = text.replace(j, "")

            text = re.sub(r'\b\d{1,2}\b', '', text)
            text = re.sub(r'\s+', ' ', text).strip()
            return text

        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(self.calendar_source_url, headers=headers)

        months = [
            "January","February","March","April","May","June",
            "July","August","September","October","November","December"
        ]

        with pdfplumber.open(BytesIO(response.content)) as pdf:
            page = pdf.pages[0]
            W, H = float(page.width), float(page.height)
            # Bounding boxes for each term section
            sections = [
                {"name": "FALL SEMESTER 2025", "bbox": (W*0.44, 40, W*0.72, 415)},
                {"name": "WINTER SESSION 2026", "bbox": (W*0.44, 420, W*0.72, H-15)},
                {"name": "SPRING SEMESTER 2026", "bbox": (W*0.73, 40, W-5, 415)},
                {"name": "SUMMER SESSION 2026", "bbox": (W*0.73, 420, W-5, H-15)}
            ]

            for section in sections:
                crop = page.crop(section["bbox"])
                words = crop.extract_words(x_tolerance=3, y_tolerance=1.5)

                # Group words by Y-position (lines)
                lines = {}
                for w in words:
                    y = round(w['top'], 1)
                    lines.setdefault(y, []).append(w)

                for y in sorted(lines.keys()):
                    line_words = lines[y]
                    line_text = " ".join(w['text'] for w in line_words).strip()
                    first_word = line_words[0]['text'].strip()

                    if first_word in months:
                        date_val = line_words[1]['text'] if len(line_words) > 1 else ""
                        desc = " ".join(w['text'] for w in line_words[2:])
                        if date_val:
                            date = f"{first_word} {date_val}"
                            
                            term_name = section["name"]

                            if 'June' in date:
                                term_name = 'SUMMER SESSION 2026'
                            if 'January' in date:
                                term_name = 'WINTER SESSION 2026'
                            if date == 'June 08':
                                term_name = 'SPRING SEMESTER 2026'

                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": self.calendar_source_url,
                                "Term Name": term_name,
                                "Term Date": date,
                                "Term Date Description": clean_final_desc(desc)
                            })

                        elif self.calendar_rows:
                            if not any(h in line_text.upper() for h in ["SEMESTER", "SESSION"]):
                                self.calendar_rows[-1]["Term Date Description"] += " " + clean_final_desc(line_text)

        # --- DATE BASED OVERRIDES (df.loc replacement) ---
        date_fixes = {
            'October 17': 'Student Services Collaboration Day, Student Services Closed, Classes in Session, College Open',
            'March 20': 'Student Services Collaboration Day, Student Services Closed, College Open, Classes in Session',
            'July 08': 'Last day to withdraw with grade of a “W”',
            'July 16': 'Last day of 5 wk Summer Session',
            'July 27': '5 wk Grades Due',
            'July 30': 'Last day of 7 wk Summer Session'
        }

        for row in self.calendar_rows:
            if row["Term Date"] in date_fixes:
                row["Term Date Description"] = date_fixes[row["Term Date"]]

        # --- Manual final row ---
        self.calendar_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.calendar_source_url,
            "Term Name": "SUMMER SESSION 2026",
            "Term Date": "August 07",
            "Term Date Description": "7 wk Grades Due"
        })

    
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

        