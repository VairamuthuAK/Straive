import re
import io
import json
import scrapy
import requests
import pdfplumber
import pandas as pd
from parsel import Selector
from ..utils import save_df

def course_parse_clean_bldg_field(text):
    """Aggressively strips times and credit numbers from the building string."""
    # Remove times (00:00 AM/PM)
    text = re.sub(r'\d{1,2}:\d{2}\s?(?:AM|PM|am|pm)?', '', text)
    # Remove credit values (1.00, 0.50, .30)
    text = re.sub(r'\d?\.\d{2}', '', text)
    return text.strip().strip(',')

class CoeSpider(scrapy.Spider):
    name = "coe"

    # Unique institution identifier used across all datasets
    institution_id = 258447495951050705

    # Base URLs

    course_urls = [
        "https://www.coe.edu/application/files/9317/3219/9089/Spring_2025_Cours_accessible_11.21.2024.pdf",
        "https://www.coe.edu/application/files/8417/4379/6224/FA_2025_accessible_4.4.2025.pdf",
        "https://www.coe.edu/application/files/3217/6099/0815/SP_2026_10.20.2025.pdf"
    ]
    directory_headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://www.coe.edu/why-coe/faculty-staff-directory',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }
    
    calendar_urls = [
        'https://www.coe.edu/application/files/3017/5760/8086/25-26_academic_calendar_accessible_9.11.25.pdf',
        "https://www.coe.edu/application/files/9217/6782/4541/Academic_Calendar_26-27_accessible_1.7.26.pdf"
    ]
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage for scraped data
        self.course_rows = []       # Stores all course data
        self.directory_rows = []    # Stores all directory (faculty/staff) data
        self.calendar_rows = []     # Stores all calendar events data

    def start_requests(self):
        """
        Entry point for the spider.
        Scrape mode can be controlled using SCRAPE_MODE setting.
        """
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')  # Determine mode

        if mode == 'course':
            self.parse_course()

        elif mode == 'directory':
            for page in range(1,19):
                directory_url = f"https://www.coe.edu/why-coe/faculty-staff-directory/get_pages/46909?ccm_paging_p={page}"
                yield scrapy.Request(
                    directory_url,
                    headers=self.directory_headers,
                    callback=self.parse_directory,
                    dont_filter=True
                )

        elif mode == 'calendar':
           self.parse_calendar()

        else:
            # Default: scrape course, directory, and calendar
            self.parse_course()
            for page in range(1,19):
                directory_url = f"https://www.coe.edu/why-coe/faculty-staff-directory/get_pages/46909?ccm_paging_p={page}"
                yield scrapy.Request(
                    directory_url,
                    headers=self.directory_headers,
                    callback=self.parse_directory,
                    dont_filter=True
                )
            self.parse_calendar()


    def parse_course(self):

        SPLIT_DELIMITERS = [
            r'\bMWF\b', r'\bTR\b', r'\bTTh\b', r'\bMTWF\b', r'\bMW\b', r'\bWF\b',
            r'\d{2}:\d{2}',  # Matches times like 09:00 or 00:00
            r'\bRemote\b', r'\bArranged\b', r'\bTBA\b'
            ]
        DELIM_PATTERN = re.compile('|'.join(SPLIT_DELIMITERS))

        weekdays_pattern = r'\b(M|T|W|R|F|MWF|TR|MW|WF|MTW|TWR|MTR|MF|WR|MTWR|TWF|MR|TW|MWR)\b'
        weekdays_pattern_course = r'\s+\b(M|T|W|R|F|MWF|TR|MW|WF|MTW|TWR|MTR|MF|WR|MTWR|TWF|MR|TW|MWR|MTWF)\b$'

        
        for pdf_url in self.course_urls:
            response = requests.get(pdf_url)
        
            # Matches 'Staff' or 'Initial Lastname' (e.g., 'C Melcher')
            instr_pattern = re.compile(r'(Staff|[A-Z]\s[A-Z][a-zA-Z-]+)')

            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text: continue
                    
                    for line in text.split('\n'):
                        # 1. Identify Course Number (e.g., ACC 171 01)
                        match = re.match(r'^([A-Z]{2,4}\s\d{3}[A-Z]?\s\d{2}(?:\s+WE)?)', line)
                        if not match: continue
                        
                        course_num = match.group(1)
                        remaining = line[len(course_num):].strip().lstrip(',')
                        
                        # 2. Find the anchor (Day/Time/Location start)
                        split_point = DELIM_PATTERN.search(remaining)
                        
                        if split_point:
                            # Identity info is BEFORE the split
                            id_chunk = remaining[:split_point.start()].strip().rstrip(',')
                            # Location/Time is AFTER the split
                            loc_chunk = remaining[split_point.start():].strip()
                            
                            # 3. Extract Instructor from the id_chunk
                            instr_search = list(instr_pattern.finditer(id_chunk))
                            if instr_search:
                                # Take the LAST instructor found in that chunk
                                instructor = instr_search[-1].group(0)
                                title = id_chunk[:instr_search[-1].start()].strip().rstrip(',')
                            else:
                                # Special check for staff names in the location chunk
                                title = id_chunk
                                instructor = ""
                            
                            bldg = course_parse_clean_bldg_field(loc_chunk)
                        else:
                            title = remaining
                            instructor = ""
                            bldg = ""
                    
                        bldgs = re.sub(weekdays_pattern, '', bldg).strip()
                        bldgs = re.sub(r'\b[MTWRF]{1,5}\b', '', bldgs).strip()
                        bldgs = bldgs.replace('Stuart Hall 4','Stuart Hall').replace('Arranged A1','Arranged A').replace('Struve Comm Ctr 4','Struve Comm Ctr').replace('Arranged A0','Arranged').replace('Arranged A2','Arranged A').replace('Arranged A3','Arranged A').replace('Athletics and RecreaW 0','Athletics and Recrea').strip()
                        course_number = course_num.split(' ')[:2]
                        course_number = ' '.join(course_number)
                        course_name = course_number + ' ' + title
                        section = course_num.split(' ')[2:]
                        section = section[0]
                        course_name = course_name.replace('1.00','').replace('0.25','').replace('0.30','').replace('0.50','')
                        course_name = re.sub(weekdays_pattern_course, '', course_name).strip()
                        
                        # Append course data to list
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Course Name":course_name,
                            "Course Description": '',
                            "Class Number": course_number,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": '',
                            "Course Dates": '',
                            "Location": '',
                            "Textbook/Course Materials": '',
                        })

        # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")


    def parse_directory(self, response):
        data = json.loads(response.text)
        for html in data.get("pages", []):
            selector = Selector(text=html)
            name = selector.xpath('//h2/text()').get(default='').strip()
            title1 = selector.xpath('//p[@class="title"]//text()').getall()
            title1 = " ".join([t.strip() for t in title1 if t.strip()])
            title2 = selector.xpath('//p[@class="department"]//text()').get(default='').strip()
            title = f"{title1}, {title2}" if title2 else title1
            title_parts = [t.strip() for t in title.split(",")]
            title = ", ".join(dict.fromkeys(title_parts))
            # breakpoint()
            phone = selector.xpath('//p[@class="phone"]/a/text()').get(default='').strip()
            email = selector.xpath('//p[@class="email"]/a/text()').get(default='').strip()
        
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": 'https://www.coe.edu/why-coe/faculty-staff-directory',
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })

        # # Save updated DataFrame
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")


    def parse_calendar(self):
        
        current_term = ""
        is_in_date_section = False
        
        # Regex for standard date formats found in the text (e.g., 21-Aug, 22-25 Aug, 1-Sep)
        date_pattern = re.compile(r'^(\d{1,2}(?:-\d{1,2})?\s?-\s?[A-Za-z]{3}|\d{1,2}(?:-\d{1,2})?\s?[A-Za-z]{3})\s+(.*)')
        for calendar_url in self.calendar_urls:
            response = requests.get(calendar_url)
            response.raise_for_status()
            pdf_file = io.BytesIO(response.content)
            with pdfplumber.open(pdf_file) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue
                        
                    lines = text.split('\n')
                    for line in lines:
                        line = line.strip()
                        
                        # Identify Term Header (e.g., Fall 2026 Academic Calendar)
                        if "Academic Calendar" in line and "Dates" not in line:
                            current_term = line
                            is_in_date_section = False # Reset for new page grid
                            continue
                        
                        # Flag to start extraction: Skip the table grid and wait for "Dates" section
                        if "Academic Calendar Dates" in line or "May 2027 Academic Calendar Dates" in line:
                            is_in_date_section = True
                            continue
                        
                        # Extract if we are in the correct section and the line looks like a date/event
                        if is_in_date_section:
                            match = date_pattern.match(line)
                            if match:
                                term_date = match.group(1).strip()
                                description = match.group(2).strip()
                                self.calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Term Name": current_term,
                                    "Term Date": term_date,
                                    "Term Date Description": description,
                                })

        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")



