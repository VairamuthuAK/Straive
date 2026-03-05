import re
import json
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from parsel import Selector
from inline_requests import inline_requests


def normalize_dashes(text):
    return (
        text.replace("–", "-")
            .replace("—", "-")
            .replace("−", "-")
    )

def is_valid_event(text):
    if not text:
        return False
    skip_phrases = [
        "academic calendar",
    ]
    text_lower = text.lower()
    return not any(p in text_lower for p in skip_phrases)


def normalize_same_month_range(date_text):
    """
    February 3 - February 7  -> February 3 - 7
    March 30 - April 4       -> March 30 - April 4
    """
    if not date_text:
        return date_text

    pattern = re.compile(
        r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*-\s*"
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})$",
        re.I
    )

    m = pattern.match(date_text.strip())
    if not m:
        return date_text

    m1, d1, m2, d2 = m.groups()

    if m1.lower() == m2.lower():
        return f"{m1} {d1} - {d2}"

    return date_text


class VuuSpider(scrapy.Spider):
    name = "vuu"
    
    course_rows = []
    institution_id = 258435885509404624
    
    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://my.vuu.edu/ICS/Helpful_Resources.jnz?portlet=Course_Search"
    directory_source_url = "https://www.vuu.edu/directory/"
    calendar_url = "https://www.vuu.edu/"
   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)

    # Parse methods UNCHANGED from your original
    @inline_requests
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
        
        # Extract available term IDs and term names from dropdown
        term_ids = response.xpath('//select[@id="pg0_V_TermDropDownList"]/option/@value').getall()
        terms = response.xpath('//select[@id="pg0_V_TermDropDownList"]/option/text()').getall()
        
        # Iterate through terms and process only 2025 / 2026 terms
        for term_id, term in zip(term_ids[1:], terms[1:]):
            if "2025" in term or "2026" in term:
                print(term)
                self.parse_course_post_response(term_id)
       
        # Convert collected course rows into DataFrame and save
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df,    self.institution_id, "course")    
            
    def parse_course_post_response(self, term_id):
        
        """
        Submit course search form for a given term
        and parse individual course detail pages.
        """
        
        # Maintain session to preserve cookies/state
        session = requests.Session()
        
        # Headers required by the ASP.NET application
        course_headers = {
            'Origin': 'https://my.vuu.edu',
            'Referer': 'https://my.vuu.edu/ICS/Helpful_Resources.jnz?portlet=Course_Search',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            }
        
        url = 'https://my.vuu.edu/ICS/Helpful_Resources.jnz?portlet=Course_Search'

        # Initial GET request to retrieve fresh VIEWSTATE values
        response = session.get(url)

        # Check if the request was successful
        if response.status_code == 200:

            # Parse updated hidden form values
            res_xpath = Selector(text=response.text)
            VIEWSTATE_text_main  = res_xpath.xpath('//input[@id="__VIEWSTATE"]/@value').get('').strip()
            VIEWSTATEGENERATOR_text_main  = res_xpath.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip()
            BrowserRefresh_text_main  = res_xpath.xpath('//input[@id="___BrowserRefresh"]/@value').get('').strip()
            files=[

            ]
            # Build POST payload for course search
            payload = {'_scriptManager_HiddenField': '',
                    '__EVENTTARGET': '',
                    '__EVENTARGUMENT': '',
                    '__VIEWSTATE': f'{VIEWSTATE_text_main}',
                    '__VIEWSTATEGENERATOR': f'{VIEWSTATEGENERATOR_text_main}',
                    '___BrowserRefresh': f'{BrowserRefresh_text_main}',
                    'userName': '',
                    'password': '',
                    'siteNavBar$searchBox$tbSearch': '',
                    'pg0$V$CourseTitleTextBox': '',
                    'pg0$V$CourseCodeTextBox': '',
                    'pg0$V$FacultyLastNameTextBox': '',
                    'pg0$V$TermDropDownList': f'{str(term_id)}',
                    'pg0$V$CourseDescriptionTextBox': '',
                    'pg0$V$CourseSearchButton': 'Search'}
            
            # Submit search form
            post_response = session.post(url, headers=course_headers, data=payload, files=files)
            
            post_xpath = Selector(text=post_response.text)
            
            # Extract course session URLs from search results table
            session_urls = post_xpath.xpath('//table[@id="CourseSearchResultsTable"]/tbody/tr/td/a/@href').getall()
            
            # Visit each course detail page
            for i,session_url in enumerate(session_urls, start=1):
                session_url = f"https://my.vuu.edu{session_url}"
                print(f"{session_url}---->>{i}")
                headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
                'referer': 'https://my.vuu.edu/ICS/Helpful_Resources.jnz?portlet=Course_Search',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
                
                session_response = session.get(session_url, headers=headers)
                session_xpath = Selector(text=session_response.text)

                # Extract course name and class number
                course_name = session_xpath.xpath('//div[@id="PageBar_pageTitle"]/h2/a/text()').get('').strip()
                if re.search(r'\d+\s*(\d+)\s*\-', course_name):
                    class_no = re.findall(r'\d+\s*(\d+)\s*\-', course_name)[0]
                else:
                    class_no = ''
                if class_no:
                    course_name = course_name.replace(class_no,'')
                    
                # Extract schedule date and location
                date_location = session_xpath.xpath('//div[@id="pg0_V_Schedule"]/p/text()').get('').strip()
                if "Location:" in date_location:
                    location = date_location.split('Location:')[-1].strip()
                else:
                    location = ''
                if re.search(r'\(([\w\W]*?)\)', date_location):
                    date = re.findall(r'\(([\w\W]*?)\)', date_location)[0]
                else:
                    date = ''
                
                # Extract instructor names
                names = session_xpath.xpath('//div[@id="Faculty"]//div[@class="row course-info-row-container"]//div[@class="row"][not(@id)]/text()').getall()
                if not names:
                    names = list(set(session_xpath.xpath('//div[@id="Faculty"]//table//tr[2]/td/text()').getall()))
                names = ", ".join(n.strip() for n in names if n.strip())
                
                # Append course record
                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": session_url,
                    "Course Name": re.sub('\s+',' ',course_name),
                    "Course Description": re.sub('\s+',' ',session_xpath.xpath('//div[@id="CourseDescription"]/p/text()').get('')).strip(),
                    "Class Number": class_no,
                    "Section": '',
                    "Instructor": names,
                    "Enrollment": '',
                    "Course Dates":date,
                    "Location": location,   
                    "Textbook/Course Materials": '',
                })
            
            
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

        # List to store extracted staff records
        directory_rows = []

        if re.search(r'layoutID\s*\=\s*(\d+)\;', response.text):
            layoutid = re.findall(r'layoutID\s*\=\s*(\d+)\;', response.text)[0]
        else:
            layoutid = ''
        if re.search(r'BlockID\"\s*value\=\"(\d+)\"', response.text):
            Blockid = re.findall(r'BlockID\"\s*value\=\"(\d+)\"', response.text)[0]
        else:
            Blockid = ''
        url = "https://www.vuu.edu/Page/UpdateLiveSearch/"
        for count in range(1,100,1):
            payload = f"BlockID={str(Blockid)}&LayoutID={str(layoutid)}&Filters=%5B%5D&URL=%2Fdirectory%2F&pageNumber={str(count)}&itemsPerPage=50"
            headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://www.vuu.edu',
            'Referer': 'https://www.vuu.edu/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            }

            response1 = requests.request("POST", url, headers=headers, data=payload)
            json_data = json.loads(response1.text)
            results = json_data.get('result','')
            xpath_obj = Selector(text=results)
        
            blocks = xpath_obj.xpath('//tbody/tr')
            if blocks:
                for block in blocks:
                    last_name = block.xpath('./td[1]//text()').get('').strip()
                    first_name = block.xpath('./td[2]//text()').get('').strip()
                    
                    if first_name:
                        
                        # Append extracted staff data to the results list
                        directory_rows.append(
                            {
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": self.directory_source_url,
                                "Name": f"{first_name} {last_name}",
                                "Title": block.xpath('./td[3]//text()').get('').strip(),
                                "Email": block.xpath('./td[6]//text()').get('').strip(),
                                "Phone Number": block.xpath('./td[4]//text()').get('').strip(),
                            }
                        )
            else:
                break
        # Convert collected records into a DataFrame
        directory_df = pd.DataFrame(directory_rows)
        
        # Save the DataFrame using a custom helper function
        save_df(directory_df, self.institution_id, "campus")
        
        
    @inline_requests
    def parse_calendar(self, response):
        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        calendar_rows = []
        page = "1,2"
        page1 = "5-8"
        cal_url = response.xpath('//a[contains(text(),"Academic Calendar")]/@href').get('').strip()
        cal_url = f"https://www.vuu.edu{cal_url}"
        
        # Download PDF from URL
        response1 = requests.get(cal_url)
        response1.raise_for_status()
        if page1:
            current_term = None
            current_subterm = None
            current_date = None
            current_event = None

            # TERM HEADER
            term_pattern = re.compile(r"^(FALL|SPRING|SUMMER)\s+\d{4}", re.I)

            # SUB-TERM HEADER
            subterm_pattern = re.compile(r"Sub[-\s]?term\s*(\d+)", re.I)

            date_anywhere_pattern = re.compile(
                r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+"
                r"(\d{1,2})"
                r"(?:\s*[,–-]\s*"
                r"(January|February|March|April|May|June|July|August|September|October|November|December)?\s*"
                r"(\d{1,2}))?",
                re.I
            )

            with pdfplumber.open(BytesIO(response1.content)) as pdf:
                for page_num in range(4, 8):
                    text = pdf.pages[page_num].extract_text()
                    if not text:
                        continue

                    for line in text.split("\n"):
                        line = line.strip()
                        if not line:
                            continue

                        # 🔹 TERM HEADER (flush previous event first)
                        term_match = term_pattern.match(line)
                        if term_match:
                            if current_event and current_date:
                                if not re.match(r"^\d+", current_event):
                                    calendar_rows.append({
                                        "Cengage Master Institution ID": self.institution_id,
                                        "Source URL": cal_url,
                                        "Term Name": f"{current_term} Sub-term {current_subterm}" if current_subterm else current_term,
                                        "Term Date": normalize_same_month_range(current_date),
                                        "Term Date Description": current_event.strip(),
                                    })
                            current_term = line.upper()
                            current_subterm = None
                            current_event = None
                            current_date = None
                            continue

                        if not current_term:
                            continue

                        # 🔹 SUB-TERM DETECTION
                        subterm_match = subterm_pattern.search(line)
                        if subterm_match:
                            current_subterm = subterm_match.group(1)

                        # 🔹 DATE DETECTION
                        date_match = date_anywhere_pattern.search(line)
                        if date_match:
                            if current_event and current_date:
                                # if not re.match(r"^\d{2}\s*Academic Calendar", current_event):
                                if not re.match(r"^\d+", current_event):
                                    calendar_rows.append({
                                        "Cengage Master Institution ID": self.institution_id,
                                        "Source URL": cal_url,
                                        "Term Name": f"{current_term} Sub-term {current_subterm}" if current_subterm else current_term,
                                        "Term Date": normalize_same_month_range(current_date),
                                        "Term Date Description": current_event.replace('Virginia Union University','').strip(),
                                    })

                            m1, d1, m2, d2 = date_match.groups()

                            if d2:
                                if not m2:
                                    m2 = m1
                                current_date = f"{m1} {d1} - {m2} {d2}"
                            else:
                                current_date = f"{m1} {d1}"

                            current_event = line[date_match.end():].strip()

                        else:
                            # wrapped line
                            if current_event:
                                current_event += " " + line

            # 🔹 SAVE LAST EVENT
            if current_event and current_date:
                if not re.match(r"^\d+", current_event):
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": cal_url,
                        "Term Name": f"{current_term} Sub-term {current_subterm}" if current_subterm else current_term,
                        "Term Date": normalize_same_month_range(current_date),
                        "Term Date Description": current_event.replace('Virginia Union University','').strip(),
                    })
    
        if page:
            # Regex patterns
            TERM_PATTERN = re.compile(
                r"(Fall|Spring|Summer)\s+\d{4}\s+Key Dates",
                re.IGNORECASE
            )

            DATE_PATTERN = re.compile(
                r"""
                ^
                (
                    TBA
                    |
                    # Cross-month range (December 22 - January 2)
                    [A-Za-z]+\s+\d{1,2}\s*-\s*[A-Za-z]+\s+\d{1,2}
                    |
                    # Same-month date or range
                    [A-Za-z]+\s+\d{1,2}
                    (?:\s*-\s*\d{1,2})?
                    (?:,\s*[A-Za-z]+(?:-\s*[A-Za-z]+)?)?
                )
                """,
                re.VERBOSE
            )
            current_term = None
            with pdfplumber.open(BytesIO(response1.content)) as pdf:
                for page in pdf.pages[:2]:  # pages 5–8 (0-based index)
                    
                    text = page.extract_text()
            
                    if not text:
                        continue

                    for raw_line in text.split("\n"):
                        line = re.sub(r"\s+", " ", raw_line).strip()

                        if not line:
                            continue

                        # Detect TERM NAME
                        term_match = TERM_PATTERN.search(line)
                        if term_match:
                            current_term = term_match.group(0)
                            continue

                        # Skip noise
                        if any(x in line.lower() for x in [
                            "academic calendar",
                            "university calendar",
                            "office of the registrar",
                            "deadlines",
                            "grades due"
                        ]):
                            continue

                        line = normalize_dashes(line)

                        date_match = DATE_PATTERN.match(line)

                        if date_match:
                            event_date = date_match.group(1).replace('University','').strip()
                            event_name = line[len(event_date):].strip(" ,-")
                            event_name = " ".join(dict.fromkeys(event_name.split()))
                            if not re.match(r"^\d{4}", event_name):
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_url,
                                    "Term Name": current_term,
                                    "Term Date": event_date,
                                    "Term Date Description": event_name,
                                })            
                                    
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")