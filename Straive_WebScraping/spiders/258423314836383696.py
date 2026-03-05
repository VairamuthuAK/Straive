import re
import scrapy
import os
import io
import csv
import time
import json
import requests
import pdfplumber
import pandas as pd
from parsel import Selector
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from ..utils import save_df


class HerkimerSpider(scrapy.Spider):
    """
    Scrapy spider for Herkimer County Community College.

    This spider extracts:
    1. Course information
    2. Academic calendar (from PDF)
    3. Faculty/staff directory

    Data is normalized and saved using save_df().
    """

    name = "herkimer"
    process_file_name = f"herkimer_001.csv"
    institution_id = 258423314836383696

    # Base website URL
    base_url = "http://www.herkimer.edu"

    # Calendar PDF URL
    calendar_url = "https://www.herkimer.edu/assets/Documents/Summer-2025-Spring-2026-Abridged-Calendar-2.pdf"

    # Course schedule page
    course_url = "https://www.herkimer.edu/academics/course-schedule/"

    # Faculty & staff directory page
    directory_url = "https://www.herkimer.edu/directory/facultystaff"

    # HTTP headers to pretend we are a browser
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
    
    def parse_course_safe_get(self, session, url, level, failed_urls):
        """
        Safely fetches a URL using requests.

        - Retries on failure
        - Waits between retries
        - Logs errors
        - Never crashes the spider
        """

        # Try 3 times
        for attempt in range(1, 4):
            try:
                # Send GET request
                r = session.get(url, timeout=80)

                # Raise error if status code is 4xx or 5xx
                r.raise_for_status()

                # If success, return response
                return r

            except Exception as e:
                # If this is the final attempt
                if attempt == 3:
                    # Store failure details
                    failed_urls.append({
                        "level": level,
                        "url": url,
                        "error": str(e)
                    })

                    # Give up
                    return None

                # Wait before retrying
                time.sleep(5)
    
    def parse_course(self, response):
        """
        Parse course data using request response.

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
        
        processed_urls = set()
        if os.path.exists(self.process_file_name):
            try:
                df_resume = pd.read_csv(self.process_file_name, usecols=["url"])
                processed_urls = set(df_resume["url"].dropna().str.strip())
                print(f":arrows_anticlockwise: Loaded {len(processed_urls)} processed URLs")
            except Exception as e:
                processed_urls = set()

        if not os.path.exists(self.process_file_name):
            with open(self.process_file_name, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["url"])

        course_data = []
        failed_urls = []
        
        
        """
        Why use a session?
        - Keeps connection alive (faster)
        - Shares headers
        - Applies retry logic
        - More control than Scrapy for some sites
        """

        # Create a new session object 
        session = requests.Session()

        # Define retry behavior
        retries = Retry(
            total=5,                  # Try up to 5 times
            backoff_factor=2,         # Wait time increases after each failure
            status_force_list=[429, 500, 502, 503, 504]  # Retry for these HTTP codes
        )

        # Create an adapter with retry logic
        adapter = HTTPAdapter(max_retries=retries)
        
        # Apply retry adapter to HTTPS and HTTP
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Set default headers for all requests from this session
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })

        course_terms = response.xpath('.//main//div/ul/li/a/@href').getall()

        for term in course_terms:
            term_url = response.urljoin(term)

            term_res = self.parse_course_safe_get(session, term_url, "campus", failed_urls)
            
            if not term_res:
                continue

            term_sel = Selector(text=term_res.text)
            subject_links = term_sel.xpath('.//ul[contains(@class,"grid")]/li/a/@href | .//div/h3/a/@href').getall()

            time.sleep(3)

            for sub in subject_links:
                sub_url = urljoin(term_res.url, sub)
                sub_res = self.parse_course_safe_get(session, sub_url, "subject", failed_urls)

                if sub_res and sub_res.status_code == 200:
                    tablesSel = Selector(text=sub_res.text)

                    rows = tablesSel.xpath('//table//tbody/tr')
                    
                    if not rows:
                        break

                    # Save processed URL
                    with open(self.process_file_name, "a", encoding="utf-8", newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([sub_url])
                    
                    for row in rows:
                        course_code = row.xpath('.//td[1]//text()').get()
                        title = row.xpath('.//td[4]//text()').get()
                        classNumber = row.xpath('.//td[3]//text()').get()
                        instructor = row.xpath('.//td[7]//text()').get()

                        if course_code:
                            course_code = course_code.strip()
                        if title:
                            title = title.strip()
                        if classNumber:
                            classNumber = classNumber.strip()

                        if course_code and "-" in course_code:
                            code, sec = course_code.split("-", 1)
                        else:
                            code = course_code
                            sec = ""

                        course_code_url = row.xpath('.//td[1]//a/@href').get()

                        seats_available = seats_total = None
                        start_date = end_date = None
                        course_desc = ""
                        bookurl = ""
                        
                        # Use the detail URL as source URL
                        source_url = sub_url  # Default to subject page
                        
                        if course_code_url:
                            desc_url = urljoin(sub_res.url, course_code_url)
                            source_url = desc_url  # Update to detail page URL
                            
                            for attempt in range(1, 4):
                                try:
                                    desc_res = session.get(desc_url, timeout=80)
                                    desc_res.raise_for_status()
                                    
                                    if desc_res.status_code == 200:
                                        desc_sel = Selector(text=desc_res.text)

                                        # Get course description
                                        desc_parts = desc_sel.xpath('//p[4]//text()[normalize-space()]').getall()
                                        
                                        if desc_parts:
                                            course_desc = desc_parts[0].strip()
                                        
                                        # Get book URL
                                        bookurl_list = desc_sel.xpath('.//p//a/@href').getall()
                                        
                                        if bookurl_list:
                                            bookurl = bookurl_list[0]

                                        # Get seats information - try multiple methods
                                        # Method 1: Look for elements containing "Seats:"
                                        info_line = desc_sel.xpath('//*[contains(text(),"Seats:")]//text()').getall()
                                        info_text = " ".join(t.strip() for t in info_line if t.strip())
                                        
                                        # Method 2: If not found, search entire page content
                                        if not info_text or "Seats:" not in info_text:
                                            all_text = desc_sel.xpath('//body//text()').getall()
                                            info_text = " ".join(t.strip() for t in all_text if t.strip())
                                        
                                        # Parse seats with more flexible regex
                                        # Handles: "Seats: 16 / 28", "Seats: / 150", "Seats: 0 / 20"
                                        seat_match = re.search(r'Seats:\s*(\d*)\s*/\s*(\d+)', info_text)
                                        
                                        if seat_match:
                                            # Group 1 might be empty string if no available seats shown
                                            seats_available = seat_match.group(1) if seat_match.group(1) else None
                                            seats_total = seat_match.group(2)

                                        # Get date information
                                        date_text = desc_sel.xpath('//p[contains(text(),"runs from")]//text()').get()
                                        
                                        if date_text:
                                            date_match = re.search(r"from\s+(.*?)\s+to\s+(.*?)(?:\s+on|\s*$)", date_text)
                                            
                                            if date_match:
                                                start_date = date_match.group(1).strip()
                                                end_date = date_match.group(2).strip()
                                        
                                        # Successfully fetched, break the retry loop
                                        break
                                        
                                except Exception as e:
                                    if attempt == 3:
                                        
                                        # On final failure, log it
                                        failed_urls.append({
                                            "level": "course_desc",
                                            "url": desc_url,
                                            "error": f"desc fetch failed: {e}"
                                        })
                                        self.logger.error(f"Failed to fetch {desc_url} after 3 attempts: {e}")
                                    
                                    else:
                                        time.sleep(2)

                        # Format enrollment and course dates
                        # If seats_available is None or empty, default to "0"
                        if seats_total:
                            available = seats_available if seats_available else "0"
                            enrollment = f"{available}/{seats_total}"
                        
                        else:
                            enrollment = ""
                        
                        course_date = f"{start_date} to {end_date}" if start_date and end_date else ""
                        
                        course_data.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": source_url,
                            "Course Name": f'{code} {title}' if code and title else code or title or "",
                            "Course Description": re.sub(r'\s+', ' ', course_desc).strip() if course_desc else "",
                            "Class Number": classNumber or "",
                            "Section": sec,
                            "Instructor": instructor or "",
                            "Enrollment": enrollment,
                            "Course Dates": course_date,
                            "Location": "",
                            "Textbook/Course Materials": bookurl
                        })

                    time.sleep(1.5)

        df = pd.DataFrame(course_data)
        save_df(df, self.institution_id, "course")

        # Save failed URLs
        if failed_urls:
            
            failed_df = pd.DataFrame(failed_urls)
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            file_path = os.path.join(output_dir, f"{self.institution_id}_course_failed_urls.csv")
            failed_df.to_csv(file_path, index=False, encoding="utf-8-sig")
    
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

        directory_data = []

        # Extract JSON text embedded inside a <script> tag with id="directory-data"
        json_text = response.xpath('//script[@id="directory-data"]/text()').get()
        
        # If JSON data exists
        if json_text:

            # Convert JSON string into Python dictionary
            data = json.loads(json_text)

            # Get the list of people from the JSON
            items = data.get("items", [])

            # Loop through each person record
            for item in items:

                # Extract and clean all data
                name = (item.get("name") or "").strip()
                title = (item.get("job_title") or "").strip()
                phone = (item.get("phone") or "").strip()
                email = (item.get("email") or "").strip()

                # Append the record into our list
                directory_data.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.directory_url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone
                })

        df = pd.DataFrame(directory_data)
        save_df(df, self.institution_id, "campus")

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

        calendar_data = []  # This list will store all calendar rows

        # Get the raw PDF bytes from the response
        pdf_bytes = response.body

        # Open the PDF using pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            full_text = ""  # Will store all text from all pages

            # Loop through each page in the PDF
            for page in pdf.pages:
                # Extract text from the page and add it
                full_text += page.extract_text() + "\n"

        # Split the extracted text into individual lines
        lines = [line.strip() for line in full_text.split("\n") if line.strip()]

        current_term = None  # Will store the current term (e.g., Fall 2025)

        # Loop through each line from the PDF
        for line in lines:

            # Detect headers like: SUMMER 2025, FALL 2025, WINTER 2026
            if re.match(r"^(SPRING|SUMMER|FALL|WINTER)\s+\d{4}", line, re.I):
                current_term = line.title()
                continue

            # Skip junk headers
            if line.lower().startswith("abridged"):
                continue

            # Split description and date
            match = re.search(r"(.*?)(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(\s*–\s*\w+\s+\d{1,2})?", line)

            # If a valid date line is found and we have a term
            if match and current_term:
                desc = match.group(1).strip(" -–")
                date = line.replace(desc, "").strip()

                calendar_data.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": current_term,
                    "Term Date": date,
                    "Term Date Description": desc
                })
        calendar_df = pd.DataFrame(calendar_data)
        save_df(calendar_df, self.institution_id, "calendar")

