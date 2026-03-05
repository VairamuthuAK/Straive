import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import save_df

class OzarksSpider(scrapy.Spider):
    """
    Scrapy spider for scraping data from
    University of the Ozarks.

    Website: https://ozarks.edu/

    This spider collects:
    - Course catalog data (PDF)
    - Faculty directory data
    - Academic calendar data (PDF)

    SCRAPE_MODE options:
        - "course"
        - "directory"
        - "calendar"
        - "all" (default)
    """

    name = "ozarks"

    # Unique institution identifier used across all datasets
    institution_id = 258418071683753944

    # Base URLs
    course_url = "https://eaglenet.ozarks.edu/includes/pdffiles/catalogs/catalog.pdf"
    directory_url = "https://ozarks.edu/about/personnel-directory/?pg=1"

    calendar_url = "https://ozarks.edu/wp-content/uploads/Five-Year-Academic-Calendar-rev-7-2025.pdf"
    calendar_headers = {"User-Agent": "Mozilla/5.0"}

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
            for page in range(1,21):
                directory_url = f"https://ozarks.edu/about/personnel-directory/?pg={page}"
                directory_headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'referer': directory_url,
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
                yield scrapy.Request(directory_url,headers=directory_headers, callback=self.parse_directory, dont_filter=True)
        
        elif mode == 'calendar':
            self.parse_calendar()

        else:
            # Default: scrape course, directory, and calendar
            self.parse_course()
            for page in range(1,21):
                directory_url = f"https://ozarks.edu/about/personnel-directory/?pg={page}"
                directory_headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'referer': directory_url,
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
                yield scrapy.Request(directory_url,headers=directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()


    def parse_course(self):
        """
        Extract course catalog data from PDF.

        Logic:
        - Identify course codes using regex.
        - Split title and description using starter keywords.
        - Clean extracted text.
        """

        # Description starting keywords to split title from description
        desc_starters = [
        "An introduction", "A study", "A course", "This course", "Development of",
        "Introduction to", "The study", "Basic", "Fundamental", 
        r"(?<!Financial )Analysis", # Special case for ACC 1003
        "Preparation for", "A historical", "In this", "Exploration of", "Further exploration", "A consideration", "The conclusion",
        "An introductory", "An intermediate", "Introduction of" , "Further investigation", "Further study" , "A survey", "A continuation of Advanced"
        " A continuation of", "Surveys" , "A continuation of Advanced" , "An advanced", "A continuation of" ,"Students will"
        "This is a two-course","This advanced course", "The culmination", "Students will", "A general" ,
        "In Human Physiology", "This is a", "The Capstone", "Continuation of", "An in-depth", "This survey course", "An historical", "Small group study", "A project-oriented",
        "Study of", "As the introductory", "This variable", "A research", "Environment, Natural", "A two-semester" ,
        "Religion is", "Employing biblical", "Set within", "Orientation to", "An exploration", "An examination", "Principles of",
        "Provides an", "A comprehensive", " Examination of", "As a seminar", "An upper level", "An area course",
        "Examination of", "two-semester", "Hermeneutics is", "Over time", "A critical", "The course", "This comparative", "Provides exploration",
        "The basic", "An ensemble", "Advanced private", "Through placement", "An SSAA","The student", "These courses", "Unlike MUS", "Group instruction", "An independent","Limits, continuity", "Selected topics", "Matrices and matrix",
        "This required", "Pre-service", "This continuation", "The emphasis", "Discrete and continuous", "STEAM is the", "This culminating", "Each student", "This portion", "Fluctuations in", "Environmentalism has", "The relationship",
        "A modern development", "Concepts covered", "Application of", "Analytic geometry", "Circular functions", "This capstone", "An overview",
        "A weekly", "This one-hour", "An applied", "Investigates various", "Provides a detailed", "Provides a","Examines the", "Introduces students", "The aim is", "Evaluation is", "This is an introductory", "This seminar", "This senior",
        ]
    
        desc_regex = r'\s(' + '|'.join(desc_starters) + r')\b'

        print("Processing catalog pages...")
        
        try:
            response = requests.get(self.course_url)
            response.raise_for_status()
            pdf_file = io.BytesIO(response.content)
        except Exception as e:
            print(f"Error: {e}")
            return

        courses = []
        current_course = None

        with pdfplumber.open(pdf_file) as pdf:
            # Focusing on pages 46-77 (0-indexed 45-76)
            for i in range(45, 77):
                page = pdf.pages[i]
                text = page.extract_text()
                if not text: continue

                lines = text.split('\n')
                for line in lines:
                    line = line.strip()

                    # Skip footers and headers
                    if not line or "UNIVERSITY OF THE OZARKS" in line or "PAGE |" in line: 
                        continue

                    # 2. Extract Course Code and the remaining blob
                    code_match = re.match(r'^([A-Z]{3,4}\s\d{4}(?:-\d{4})?)\s+(.*)$', line)
                    
                    if code_match:
                        if current_course:
                            courses.append(current_course)
                        
                        code = code_match.group(1)
                        title_desc_blob = code_match.group(2)
                        
                        # 3. Use search to find the split point
                        split_match = re.search(desc_regex, " " + title_desc_blob)
                        
                        if split_match:
                            # Capture the exact index of the starter word
                            idx = split_match.start() 
                            title = title_desc_blob[:idx].strip().rstrip('.')
                            description = title_desc_blob[idx:].strip()
                        else:
                            title = title_desc_blob
                            description = ""
       
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.course_url,
                            "Course Name": code + ' ' + title,
                            "Course Description": re.sub(r'\s+', ' ', description),
                            "Class Number": code,
                            "Section": '',
                            "Instructor": '',
                            "Enrollment": '',
                            "Course Dates": '',
                            "Location": '',
                            "Textbook/Course Materials": '',
                        })

        # # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    def parse_directory(self, response):
        """
        Extract profile links from directory listing page.
        """
        links = response.xpath('//article[@class="personnel-directory__wrapper"]//div[@class="h3 title"]/a/@href').getall()
        for link in links:
            yield scrapy.Request(link,headers=self.directory_headers,callback=self.parse_directory_details)

    def parse_directory_details(self,response):
        """
        Extract individual faculty profile details.
        """
        name = response.xpath('normalize-space(//h1)').get('')

        # Combine job title + department
        title1 = response.xpath('normalize-space(//h2)').get('')
        title1 = title1.replace('•', ',').strip()
        title2_list = response.xpath('//div[@class="faculty-staff__details-department cell medium-auto"]//a/text()').getall()
        title2_list = [t.strip() for t in title2_list if t.strip()]
        title2 = ', '.join(title2_list)
        title = ', '.join([t for t in [title1, title2] if t])
        title = re.sub(r'\s+,', ',', title)
     
        email = response.xpath('//a[starts-with(@href, "mailto:")]/text()').get('')
        phone = response.xpath('//div[contains(@class,"faculty-staff__details-contact")]//p[not(.//a)]/text()').get('').strip()

        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    def parse_calendar(self):
        """
        Extract academic calendar from PDF.

        Logic:
        - Iterate through extracted table rows.
        - Map columns to academic year ranges.
        - Assign season dynamically based on record position.
        """

        response = requests.get(self.calendar_url, headers=self.calendar_headers)
        response.raise_for_status()
        
        all_rows = []
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    all_rows.extend(table)

        year_map = {
            3: '2025-2026',
            7: '2026-2027',
            10: '2027-2028',
            13: '2028-2029',
            16: '2029-2030'
        }

        self.calendar_rows = []
        # This counter tracks every single line added to your final output
        total_record_count = 0 

        for row in all_rows:
            if not row or not row[0]:
                continue
                
            desc = str(row[0]).strip().replace('\n', ' ')
            
            # Skip garbage rows
            skip_list = ['FIVE YEAR', 'Calendar Event', 'Revised', 'Date', 'Event']
            if any(skip in desc for skip in skip_list) or len(desc) < 3:
                continue
                
            if desc.lower() in ['fall', 'spring', 'summer', 'winter']:
                continue

            for col_idx, year_range in year_map.items():
                if col_idx < len(row):
                    date_val = row[col_idx]
                    
                    if date_val and str(date_val).strip():
                        # INCREMENT HERE: Every time we find a valid date, it's one record
                        total_record_count += 1
                        
                        # Apply your specific ranges to the total records
                        if total_record_count <= 105:
                            season = "Fall"
                        elif total_record_count <= 110:
                            season = "Winter"
                        elif total_record_count <= 200:
                            season = "Spring"
                        else:
                            season = "Summer"

                        clean_date = str(date_val).strip().replace('\n', ' ')
                        
                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": f"{season} {year_range}",
                            "Term Date": clean_date,
                            "Term Date Description": desc
                        })
        if self.calendar_rows:
            save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")
    