import re
import io
import scrapy
import requests
import pandas as pd
from ..utils import save_df
from pypdf import PdfReader


def course_parse_extract_class_number(title):
    """
    Extract course class number from course title.

    Example:
        "ENG 101 Introduction to Writing"
        → "ENG 101"
    """

    match = re.match(r'^([A-Z]{2,4}\s\d{3}(?:-\d{3})?)', title)
    return match.group(1) if match else ''

def parse_course_clean_description(text):
    """
    Clean course description text by:
        - Removing section headers
        - Removing credit hour patterns
        - Removing numeric codes in parentheses
        - Normalizing whitespace
    """

    headers = [
        'Africana Women’s Studies Courses',
        'Art Courses',
        'Business Administration Courses',
        'Chemistry Courses',
        'Computer Science Courses',
        'Economics Courses',
        'Education Courses',
        'English Courses',
        'Entrepreneurship Courses',
        'Finance Courses',
        'Foreign Language Courses',
        'Global Studies Courses',
        'Health Education Courses',
        'History Courses',
        'Interdisciplinary Studies Courses',
        'International Affairs',
        'Journalism and Media Studies Courses',
        'Mathematics Courses',
        'Music Courses',
        'Philosophy and Religion Courses',
        'Physical Education Courses',
        'Physics Courses',
        'Political Science Courses',
        'Psychology Courses',
        'Science Courses',
        'Social Work Courses',
        'Sociology Courses',
        'Special Education Courses',
        'Theatre Courses',
        'Speech Courses'
    ]

    # Remove header names from text
    for h in headers:
        text = text.replace(h, '')

    # Remove credit hours formats
    text = re.sub(
        r'\(?\b\d+(?:\.\d+)?(?:\s*[-–]\s*\d+(?:\.\d+)?)?\b\s*credit\s*hours?\)?',
        '',
        text,
        flags=re.IGNORECASE
    )

    # Remove standalone numbers in parentheses (e.g., (123))
    text = re.sub(r'\(\d+\)', '', text)

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

class BennettSpider(scrapy.Spider):
    """
    Spider to scrape Course, Directory, and Academic Calendar data
    from Bennett College.

    Modes supported via SCRAPE_MODE setting:
        - course
        - directory
        - calendar
        - combinations (e.g., course_directory)
        - all (default)
    """

    name = "bennett"
    institution_id = 258443019257145301

    # Data storage lists
    course_rows = []
    directory_row = []
    calendar_rows = []

    course_url = "https://www.bennett.edu/wp-content/uploads/2025/07/bennett-college-academic-catalog-2025-2026.pdf"

    directory_url = "https://www.bennett.edu/faculty/?_page=1&sort=field_department"
    directory_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }

    calendar_url = "https://www.bennett.edu/wp-content/uploads/2024/07/Bennett-College-Spring-2025-ACADEMIC-CALENDAR-FACULTY.pdf"

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            self.parse_course()
            
        elif mode == 'directory':
            for page in range(1, 3): 
                directory_url = f"https://www.bennett.edu/faculty/?_page={page}&sort=field_department"
                yield scrapy.Request(url=directory_url, headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            
        elif mode == 'calendar':
            self.parse_calendar()
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()

            for page in range(1, 3): 
                directory_url = f"https://www.bennett.edu/faculty/?_page={page}&sort=field_department"
                yield scrapy.Request(url=directory_url, headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            self.parse_calendar()

            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            for page in range(1, 3): 
                directory_url = f"https://www.bennett.edu/faculty/?_page={page}&sort=field_department"
                yield scrapy.Request(url=directory_url, headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            
            self.parse_calendar()
        
        # All three (default)
        else:
            self.parse_course()
            for page in range(1, 3): 
                directory_url = f"https://www.bennett.edu/faculty/?_page={page}&sort=field_department"
                yield scrapy.Request(url=directory_url, headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

    def parse_course(self):
        """
        Download and parse academic catalog PDF.
        Extract courses based on page ranges and title patterns.
        """

        response = requests.get(self.course_url)
        response.raise_for_status()
        reader = PdfReader(io.BytesIO(response.content))

        # Page ranges containing courses (human-readable → zero-based index)
        PAGE_RANGES = [(119, 164), (166, 190)]
        pages = []
        for start, end in PAGE_RANGES:
            pages.extend(range(start - 1, end))

        current_title = None
        current_description = []

        # Loop through selected pages
        for page_index in pages:
            if page_index >= len(reader.pages):
                continue

            text = reader.pages[page_index].extract_text()
            if not text:
                continue

            for line in text.split('\n'):
                line = line.strip()

                # Course title line
                if re.match(r'^[A-Z]{2,4}\s\d{3}\b', line):

                    if current_title:
                        class_number = course_parse_extract_class_number(current_title)
                        description = parse_course_clean_description(" ".join(current_description))
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Course Name": re.sub(r'\(\d+\)', '', current_title).strip(),
                            "Course Description": description,
                            "Class Number": class_number.strip(),
                            "Section": '',
                            "Instructor": '',
                            "Enrollment": '',
                            "Course Dates": '',
                            "Location": '',
                            "Textbook/Course Materials": '',
                        })
                    # Start new course
                    current_title = line
                    current_description = []

                # Append description lines
                elif current_title and line and not line.isdigit():
                    current_description.append(line)
        
        # Save final course
        if current_title:
            class_number = course_parse_extract_class_number(current_title)
            description = parse_course_clean_description(" ".join(current_description))
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": re.sub(r'\(\d+\)', '', current_title).strip(),
                "Course Description": description,
                "Class Number": class_number.strip(),
                "Section": '',
                "Instructor": '',
                "Enrollment": '',
                "Course Dates": '',
                "Location": '',
                "Textbook/Course Materials": '',
            })

        # Save all extracted courses
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")
      
    def parse_directory(self, response):
        """
        Extract faculty profile URLs from directory listing page.
        """

        blocks = response.xpath('//div[@data-name="entity_field_post_title"]//a/@href').getall()
        for block in blocks:
            yield scrapy.Request(url=block, headers=self.directory_headers, callback=self.parse_directory_person, dont_filter=True)

    def parse_directory_person(self, response):
        """
        Extract individual faculty member details.
        """

        name = response.xpath('//div[@data-name="entity_field_post_title"]/text()').get("").strip()
        title1 = response.xpath('//div[@data-name="entity_field_directory_category"]/text()').get('').strip()
        title2 = response.xpath('//div[@data-name="entity_field_field_job_title"]/text()').get('').strip()

        # Combine title fields if both exist
        title = title1 + ', ' + title2 if title1 and title2 else title1 or title2
        phone = response.xpath('//div[@data-name="entity_field_field_phone"]/a/text()').get('')

        self.directory_row.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": '',
            "Phone Number": phone
        })

        df = pd.DataFrame(self.directory_row)
        save_df(df, self.institution_id, "campus")

    def parse_calendar(self):
        """
        Download and parse academic calendar PDF.
        Extract date-based events using regex pattern detection.
        """

        response = requests.get(self.calendar_url)
        response.raise_for_status()
        
        # 2. Read PDF from byte stream
        pdf_file = io.BytesIO(response.content)
        reader = PdfReader(pdf_file)
        
        # regex to identify lines that start with a date (e.g., "August 25: Monday")
        # or contain a date range (e.g., "August 8-10:")
        date_pattern = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+')

        current_description = ""

        for page_num, page in enumerate(reader.pages):
            text = page.extract_text()
            if not text:
                continue
                
            lines = text.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line or "ACADEMIC CALENDAR" in line.upper() or "Page" in line:
                    continue

                # Check if this line contains a date pattern
                match = date_pattern.search(line)
                
                if match:
                    # If there's a date, the part before the date is the description
                    # (plus any description carried over from previous lines)
                    date_start_idx = match.start()
                    desc_part = line[:date_start_idx].strip()
                    date_part = line[date_start_idx:].strip()
                    
                    full_description = f"{current_description} {desc_part}".strip()
                    
                    # Clean up multiple spaces
                    full_description = ' '.join(full_description.split())
                    
                    if full_description:
                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": "ACADEMIC CALENDAR SPRING 2025",
                            "Term Date": date_part,
                            "Term Date Description": full_description
                        })
                    
                    # Reset buffer
                    current_description = ""
                else:
                    # If no date is found, this line is likely a continuation 
                    # of a description (e.g., "Without Penalty from Minimester 1 classes")
                    current_description += " " + line
        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")
