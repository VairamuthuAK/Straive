import re
import string
import scrapy
import os
import csv
import time
import requests
import pandas as pd
from parsel import Selector
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from inline_requests import inline_requests
from playwright.sync_api import sync_playwright
from ..utils import save_df


class PCCSpider(scrapy.Spider):
    """
    Scrapy spider for Portland Community College (PCC).

    This spider supports scraping:
    1. Course schedule & seat availability
    2. Staff directory
    3. Academic calendar
    
    """
    # Scrapy spider name (used when running spider)
    name = "pcc"

    # File to track processed course URLs (resume support)
    process_file_name = "pcc_001.csv"

    # Unique institution identifier
    institution_id = 258427885197486037

    # Base site URLs
    base_url = "http://www.pcc.edu"
    calendar_url = "https://www.pcc.edu/enroll/registration/academic-calendar/"
    course_url = "https://www.pcc.edu/schedule/winter/credit/"
    directory_search_url = "https://www.pcc.edu/staff/directory/?all="
    directory_url = "https://www.pcc.edu/staff/directory/"
    
    # Headers for PCC seat availability API (AJAX)
    headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': base_url,
            'Referer': base_url,
            'User-Agent': 'Mozilla/5.0',
            'X-Requested-With': 'XMLHttpRequest'
        }

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            self.parse_calendar(self.calendar_url)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_calendar(self.calendar_url)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar(self.calendar_url)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar(self.calendar_url)
    
    def parse_course_safe_get(self, session, url, level, failed_urls):
        """
        Safely fetches a URL using requests.Session with retries.

        Prevents the spider from crashing due to:
        - Network errors
        - Timeouts
        - Server 5xx responses

        Args:
            session (requests.Session): Reusable HTTP session
            url (str): URL to fetch
            level (str): Logical level (subject/course) for error reporting
            failed_urls (list): Shared list to store failed URL metadata

        Returns:
            requests.Response | None
        """
        for attempt in range(1, 4):
            try:
                r = session.get(url, timeout=80)
                r.raise_for_status()
                return r
            except Exception as e:
                if attempt == 3:
                    failed_urls.append({
                        "level": level,
                        "url": url,
                        "error": str(e)
                    })
                    return None
                time.sleep(5)

    def parse_course_fetch_seats_with_retry(self, session, term, crns, max_retries=5, delay=3):
        """
        Fetches seat & waitlist availability for multiple CRNs.

        PCC exposes seat data via an unstable AJAX endpoint.
        This method retries until valid JSON is returned.

        Args:
            session (requests.Session): HTTP session
            term (str): Academic term code (e.g. 202601)
            crns (list[str]): List of class CRNs
            max_retries (int): Retry limit
            delay (int): Seconds between retries

        Returns:
            dict: {
                "12345": {
                    "seat": [filled, capacity],
                    "wait": [filled, capacity]
                }
            }
        """
        
        url = "https://www.pcc.edu/schedule/capacity/"
        payload = {"term": str(term), "crn": ",".join(crns)}

        for attempt in range(1, max_retries + 1):
            try:                
                r = session.post(url, data=payload, headers=self.headers, timeout=30)
                
                r.raise_for_status()
                
                # Try to parse JSON
                seat_data = r.json()
                
                # Check if we got valid data
                if seat_data and isinstance(seat_data, dict) and len(seat_data) > 0:
                    return seat_data
                else:
                    self.logger.warning(f"Empty or invalid seat data received: {seat_data}")
                    
            except Exception as e:
                self.logger.error(f"Unexpected error on attempt {attempt}: {type(e).__name__} - {e}")
                
        return {}

    def parse_course(self, response):
        """
        Parse PCC course listings and extract detailed class-level data.

        Responsibilities:
        - Resume scraping using a CSV checkpoint file
        - Crawl subject → course pages
        - Extract course title, description, sections
        - Fetch live seat & waitlist data via PCC AJAX API
        - Normalize rows where CRN/instructor/seat data is repeated
        - Persist final dataset and failed URLs

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
        
        Args:
            response (scrapy.http.Response): Initial course listing page
        """
        
        # Resume support: load previously processed course URLs
        processed_urls = set()
        if os.path.exists(self.process_file_name):
            try:
                df_resume = pd.read_csv(self.process_file_name, usecols=["url"])
                processed_urls = set(df_resume["url"].dropna().str.strip())
                print(f":arrows_anticlockwise: Loaded {len(processed_urls)} processed URLs")
            except Exception as e:
                print(":warning: gwu_course_1.csv invalid — starting fresh")
                processed_urls = set()
                
        # Create resume file with header if it does not exist
        if not os.path.exists(self.process_file_name):
            with open(self.process_file_name, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["url"])

        # Initialize data containers
        course_data = []
        failed_urls = []
        term = "202601"  
        
        # Requests session with retry adapter
        session = requests.Session()

        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504]
        )

        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })
        
        # Extract subject links
        links = response.xpath('.//div[@class="indexlist"]//ul/li/a/@href').getall()

        # loop all subject links
        for link in links:
            subject_url = urljoin(response.url, link)

            # Safe fetch with retry + failure logging
            subject_html = self.parse_course_safe_get(session, subject_url, "subject", failed_urls)
            
            if not subject_html:
                continue

            subjectSel = Selector(text=subject_html.text)
            
            # Each subject page links to multiple course pages
            sub_links = subjectSel.xpath('.//main//ul//a/@href').getall()

            for subLink in sub_links:
                course_url = urljoin(response.url, subLink)

                course_html = self.parse_course_safe_get(session, course_url, "course", failed_urls)
                if not course_html:
                    continue

                courseSel = Selector(text=course_html.text)

                h3 = courseSel.xpath('//main//h3[re:test(normalize-space(), "^[A-Z]{2,}\\d+")]',
                                    namespaces={"re": "http://exslt.org/regular-expressions"})

                title = h3.xpath('normalize-space(.)').get()

                desc = h3.xpath('following-sibling::p[not(@class)][1]//text()[not(ancestor::a)]').getall()
                desc = " ".join(d.strip() for d in desc if d.strip())
                desc = desc.replace('\xa0', ' ').strip()
                desc = re.sub(r'\(See.*?\)', '', desc).strip()

                rows = courseSel.xpath('//table[contains(@class,"classes-table")]//tbody//tr[contains(@class,"data-row")]')
                if not rows:
                    continue

                # Save processed URL
                with open(self.process_file_name, "a", encoding="utf-8", newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow([subLink])

                # Collect CRNs
                crns = []
                for row in rows:
                    crn = row.xpath('.//th/text()').get()
                    if crn:
                        crn = crn.strip()
                        # Only add if it's a number (CRN is always numeric)
                        if crn.isdigit():
                            crns.append(crn)

                # Fetch seats with retry logic
                seat_data = self.parse_course_fetch_seats_with_retry(session, term, crns, max_retries=5, delay=3)
                
                # Track previous values (PCC repeats rows visually)
                prev_crn = ""
                prev_seats = ""
                prev_wait = ""
                prev_instructor = ""
                prev_book = ""

                # Parse each section row
                for row in rows:
                    crn = row.xpath('.//th/text()').get()
                    crn = crn.strip() if crn else ""

                    # If CRN missing, reuse previous
                    if not crn:
                        crn = prev_crn

                    # Seat & waitlist info
                    seat_info = seat_data.get(crn, {})
                    seat = seat_info.get("seat", [])
                    wait = seat_info.get("wait", [])

                    formatted_seats = f"{seat[0]}/{seat[1]}" if len(seat) == 2 else prev_seats
                    formatted_wait = f"{wait[0]}/{wait[1]}" if len(wait) == 2 else prev_wait

                    faculty = row.xpath('./td[6]//a/text()').get()
                    if not faculty:
                        faculty = prev_instructor

                    # Dates (can differ per row)
                    dates = row.xpath('./td[4]//*[not(contains(@class,"visually-hide"))]/text()').getall()
                    dates = " ".join(d.strip() for d in dates if d.strip())

                    # Location (can differ)
                    location = " ".join(row.xpath('./td[2]//*[not(contains(@class,"visually-hide"))]//text()').getall())
                    location = " ".join(location.split())
                    
                    # Books link may be shown only once
                    books_link = row.xpath('.//a[contains(text(),"Books")]/@href').get()
                    
                    if not books_link:
                        books_link = prev_book
                    
                    # Normalize placeholder locations
                    if location in ['—', '-', '–', 'Not applicable']:
                        location = ""

                    # Save current as previous for next row
                    prev_crn = crn
                    prev_seats = formatted_seats
                    prev_wait = formatted_wait
                    prev_instructor = faculty
                    prev_book = books_link

                    course_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": course_url,
                        "Course Name": title or "",
                        "Course Description": desc,
                        "Class Number": crn,
                        "Section": "",
                        "Instructor": faculty or "",
                        "Enrollment": formatted_seats,
                        "Course Dates": dates,
                        "Location": location,
                        "Textbook/Course Materials": books_link

                    })

        df = pd.DataFrame(course_data)
        save_df(df, self.institution_id, "course")

        # Persist failed URLs for debugging / re-run
        if failed_urls:
            failed_df = pd.DataFrame(failed_urls)
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            file_path = os.path.join(output_dir, f"{self.institution_id}_course_failed_urls.csv")
            failed_df.to_csv(file_path, index=False, encoding="utf-8-sig")
            self.logger.info(f"Failed URLs saved → {file_path}")


    @inline_requests
    def parse_directory(self, response):
        """
        Parse PCC staff directory.

        Uses alphabet-pair search (aa → zz) because PCC directory
        does not expose a full listing endpoint.

        Responsibilities:
        - Iterate through all alphabet combinations
        - Handle 404 responses safely
        - Extract staff name, title, department, phone, email
        - Decode obfuscated email addresses embedded in JavaScript
        - Output a normalized campus directory dataset
        
        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        
        """
        directory_data = []   # Final staff records
        seen = set()          # Used for deduplication (name + email)

        # PCC directory requires two-letter prefix search (aa → zz)
        for a in string.ascii_lowercase:
            for b in string.ascii_lowercase:
                key = a + b
                fullUrl = f"{self.directory_search_url}{key}"

                # inline_requests allows yielding a Request and receiving Response
                res = yield scrapy.Request(url=fullUrl,  dont_filter=True, meta={"handle_httpstatus_all": True})

                # Some combinations return 404 → safely skip
                if res.status == 404:
                    print("404 skipped:", fullUrl)
                    continue
                sel = Selector(text=res.text)
                
                # Extract directory rows
                rows = sel.xpath('.//table[contains(@class,"directory-search-results")]//tbody/tr')
                
                # loop directory details
                for row in rows:
                    
                    # extract all details in directory
                    name = row.xpath('./td[1]/strong/text()').get(default='').strip()
                    title = row.xpath('./td[1]/text()[normalize-space()]').get(default='').strip()
                    department = row.xpath('./td[2]/text()[normalize-space()]').get(default='').strip()
                    phone = row.xpath('./td[3]/text()[normalize-space()]').get(default='').strip()
                    email = row.xpath('./td[3]//a/text()').get(default='').strip()

                    # Decode JavaScript-obfuscated email if needed
                    if not email:
                        script_text = " ".join(row.xpath('./td[3]//script//text()').getall())
                        user = re.search(r"m_user\s*=\s*'([^']+)'", script_text)
                        dom = re.search(r"m_dom\s*=\s*'([^']+)'", script_text)
                        if user and dom:
                            email = f"{user.group(1)}@{dom.group(1)}"
                            
                    # Deduplication key
                    unique_key = name + email
                    if unique_key in seen:
                        continue
                    seen.add(unique_key)
                    
                    # Final staff record
                    directory_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_url,
                        "Name": name,
                        "Title": f"{title}, {department}",
                        "Email": email,
                        "Phone Number": phone
                    })

        df = pd.DataFrame(directory_data)
        save_df(df, self.institution_id, "campus")


    def parse_calendar(self, calenderURL):
        """
        Parse PCC academic calendar using Playwright.

        Playwright is required because calendar content is rendered
        dynamically and not available in initial HTML.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        calendar_data = []

        # Use Playwright to render JavaScript-driven content
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Wait until all network requests are completed
            page.goto(calenderURL, wait_until="networkidle", timeout=120000)

            html = page.content()   # ✅ Get rendered HTML
            
            # Now create Selector from Playwright HTML
            sel = Selector(text=html)

            # PCC does NOT use tabs-content — this is why it was empty
            panels = sel.xpath('.//div[contains(@class,"tabs-panel")]')

            for panel in panels:
                term_name = panel.xpath('.//h4/text()').get(default='').strip()
                term_name = re.sub(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec).*$', '', term_name).strip()
                rows = panel.xpath('.//table//tr')

                for row in rows:
                    date = row.xpath('./th//text()').get(default='').strip()
                    desc = "".join(row.xpath('./td//text()').getall()).strip()

                    if not date or not desc:
                        continue

                    calendar_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_url,
                        "Term Name": term_name,
                        "Term Date": date,
                        "Term Date Description": desc
                    })
            browser.close()
            
            calendar_df = pd.DataFrame(calendar_data)
            save_df(calendar_df, self.institution_id, "calendar")
