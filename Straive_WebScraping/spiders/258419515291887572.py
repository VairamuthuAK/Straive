import io
import re
import html
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from parsel import Selector


class EtbuSpider(scrapy.Spider):

    name = "etbu"
    institution_id = 258419515291887572

    # In-memory storage
    course_rows = []
    calendar_rows = []
    directory_rows = []
     
    #Course Schedule Endpoint
    course_url = "https://intranet.etbu.edu/php/academics/schedule/iframe.php"

    course_headers  = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://www.etbu.edu/',
        'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }   

    
    # Directory Endpoint
    directory_url = "https://www.etbu.edu/about-etbu/faculty-and-staff/gallery/"
    
    # Academic calendar PDFs
    calendar_URL = "https://www.etbu.edu/sites/default/files/downloads/2025-2026%20Calendar_0.pdf"

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':

            # Letters used by directory pagination
            letters = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","v","w","y"]
            for letter in letters:
                directory_urls = f"https://www.etbu.edu/about-etbu/faculty-and-staff/gallery/{letter}"
                yield scrapy.Request(url=directory_urls,callback=self.parse_directory,dont_filter=True)
        
        elif mode == 'calendar':
            self.parse_calendar()
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)

            # Letters used by directory pagination
            letters = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","v","w","y"]
            for letter in letters:
                directory_urls = f"https://www.etbu.edu/about-etbu/faculty-and-staff/gallery/{letter}"
                yield scrapy.Request(url=directory_urls,callback=self.parse_directory,dont_filter=True)

            
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_calendar()

        elif mode in ['directory_calendar', 'calendar_directory']:
            self.parse_calendar()

            # Letters used by directory pagination
            letters = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","v","w","y"]
            for letter in letters:
                directory_urls = f"https://www.etbu.edu/about-etbu/faculty-and-staff/gallery/{letter}"
                yield scrapy.Request(url=directory_urls,callback=self.parse_directory,dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)

            letters = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","v","w","y"]
            for letter in letters:
                directory_urls = f"https://www.etbu.edu/about-etbu/faculty-and-staff/gallery/{letter}"
                yield scrapy.Request(url=directory_urls,callback=self.parse_directory,dont_filter=True)
            
            self.parse_calendar()
       

    # Parse methods UNCHANGED from your original
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

        """
        Parse course Excel files and normalize rows.
        """

        # Get all term codes from dropdown
        term_lists = response.xpath('//select[@name="term"]//option/@value').getall()
        for term_list in term_lists:
            course_url =  "https://intranet.etbu.edu/php/academics/schedule/iframe.php"
            
            # Headers for POST
            course_post_headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'max-age=0',
                'content-type': 'application/x-www-form-urlencoded',
                'origin': 'https://intranet.etbu.edu',
                'priority': 'u=0, i',
                'referer': 'https://intranet.etbu.edu/php/academics/schedule/iframe.php',
                'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
            
            # Build POST body
            payload = f'term={term_list}&dept=ALL'
            res = requests.request("POST", course_url, headers=course_post_headers, data=payload)
            coures_response = Selector(text=res.text)

            # Select both active and expired courses
            tablesss = coures_response.xpath('//table//tr[@class="course_data "]|//table//tr[@class="course_data expired"]')
            for row  in tablesss:
                course_id = row.xpath('.//td[@class="course_id"]//text()').get('')
                course_title = row.xpath('.//td[@class="course_title"]/text()').get('').strip()
                instructor = row.xpath('.//td[@class="instructor"]/a/text()').getall()

                # Combine instructors or use staff
                if instructor != []:
                    if len(instructor) >1:
                        instructor = ', '.join(instructor)
                    else:
                        instructor = instructor[0]
                else:
                    instructor = 'staff'
                location = row.xpath('.//td[@class="location"]/text()').get('').strip()
                start_date = row.xpath('.//td[@class="startdate"]/text()').get('').strip()
                end_date = row.xpath('.//td[@class="enddate"]/text()').get('').strip()
                course_dates = start_date + '- ' + end_date
                seats = row.xpath('.//td[@class="seats_available"]//text()').get('').strip()
                
                section = course_id.split(' ')[-1]
                courses = course_id.split(' ')[0]
                course_name = courses + ' ' + course_title

                # Build final row dictionary
                self.course_rows.append(
                    {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_url,
                    "Course Name": course_name,
                    "Course Description": '',
                    "Class Number": courses,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": seats,
                    "Course Dates": course_dates,
                    "Location": location,
                    "Textbook/Course Materials": "",
                    }
                )

        #  SAVE OUTPUT CSV
        course_df = pd.DataFrame(self.course_rows)
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

        """
        Parse employee directory profiles and extract emails via hCaptcha.
        """
        
        blocks = response.xpath('//div[@class="faculty-gallery"]/article')
        for block in blocks:
            name = block.xpath('.//h2/span/text()').get('')
            title = block.xpath('.//div[@class="faculty-teaser__position"]//text()').getall()
            title = ', '.join(title)
            phone = block.xpath('.//div[@class="number"]//text()').get('')
           
            self.directory_rows.append(
                {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": '',
                "Phone Number": phone,
                }
            )
    
        
    def parse_calendar(self):
        response = requests.get(self.calendar_URL, timeout=30)
        response.raise_for_status()
        pdf_bytes = io.BytesIO(response.content)

        #Extract all text lines from PDF
        lines = []
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    for line in text.split("\n"):
                        line = line.strip()
                        if line:
                            lines.append(line)

        
        #Regex to detect term header
        TERM_PATTERN = re.compile(
            r"^(SUMMER|FALL|SPRING|WINTER|AUGUST|MAY|DECEMBER|JANUARY|FULL|EXTENDED)"
            r".*(TERM|MINI|FLEX).*?\d{4}",
            re.IGNORECASE
        )

        EMBEDDED_TERM_PATTERN = re.compile(
            r"(SUMMER|FALL|SPRING|WINTER|AUGUST|MAY|DECEMBER|JANUARY|FULL|EXTENDED)"
            r".*?(TERM|MINI|FLEX).*?\d{4}(?:\s*\(.*?\))?",
            re.IGNORECASE
        )

        # Regex to detect date line
        DATE_PATTERN = re.compile(
            r"^(JAN|FEB|MAR|APR|MAY|JUNE|JULY|AUG|AUGUST|SEP|SEPT|OCT|NOV|DEC)"
            r"\s+\d{1,2}(\s*-\s*\d{1,2})?",
            re.IGNORECASE
        )

        # Weekday names to remove from term_description
        WEEKDAY_PATTERN = re.compile(
            r"\b(MON|TUE|WED|THU|FRI|SAT|SUN)(DAY)?\b",
            re.IGNORECASE
        )

        DOT_LEADER_PATTERN = re.compile(r"\.{3,}")

        MONTH_START_PATTERN = re.compile(
            r"^(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)",
            re.IGNORECASE
        )

        #Helper functions
        def normalize_text(text):
            return (
                html.unescape(text)
                .replace("â€“", "-")
                .replace("â€™", "'")
                .replace("â€œ", '"')
                .replace("â€", '"')
                .replace("â€", "")
                .strip()
            )

        def is_term_header(line):
            if DOT_LEADER_PATTERN.search(line):
                return False
            if MONTH_START_PATTERN.match(line):
                return False
            return bool(TERM_PATTERN.search(line))

        def is_date_line(line):
            return bool(DATE_PATTERN.match(line))

        # Parse calendar into structured rows
        rows = []
        current_term = None
        last_row = None

        for raw_line in lines:
            line = normalize_text(raw_line)

            #TERM HEADER
            if is_term_header(line):
                current_term = line
                last_row = None
                continue

            #DATE LINE
            date_match = DATE_PATTERN.match(line)
            if date_match and current_term:
                month = date_match.group(1).upper()
                day_part = date_match.group(0).split(maxsplit=1)[1]
                term_date = f"{month} {day_part}"

                desc = line[date_match.end():]

                # Remove weekday names from description
                desc = WEEKDAY_PATTERN.sub("", desc)
                desc = re.sub(r"\.+", " ", desc)
                desc = re.sub(r"\s+", " ", desc).strip(" -")

                # Check for embedded term
                embedded = EMBEDDED_TERM_PATTERN.search(desc)
                if embedded:
                    desc = desc[:embedded.start()].strip(" .-")
                    current_term = embedded.group(0).strip()

                last_row = {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_URL,
                    "Term Name": current_term,
                    "Term Date": term_date,
                    "Term Date Description": desc.replace('SUNDAY','').replace('MONDAY','').replace('TUESDAY','').replace('WEDNESDAY','').replace('THURSDAY','').replace('FRIDAY','').replace('SATURDAY','').replace('THURS','')
                }
                rows.append(last_row)
                continue

            #CONTINUATION LINE
            if last_row and not is_date_line(line) and not is_term_header(line):
                embedded = EMBEDDED_TERM_PATTERN.search(line)
                if embedded:
                    current_term = embedded.group(0).strip()
                    last_row = None
                    continue

                clean = re.sub(r"\s+", " ", line).strip()
                clean = WEEKDAY_PATTERN.sub("", clean)  # remove weekday names
                if clean:
                    if not last_row["Term Date Description"].endswith("."):
                        last_row["Term Date Description"] += "."
                    last_row["Term Date Description"] += " " + clean
    
        calendar_df = pd.DataFrame(rows)
        save_df(calendar_df, self.institution_id, "calendar")

      
    def closed(self, reason):

        """
        Final cleanup and persistence.

        Saves:
        - Directory dataset
        - Calendar dataset
        - Closes all file handles
        """

        if self.directory_rows:
            save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

        if self.calendar_rows:
            save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")




 