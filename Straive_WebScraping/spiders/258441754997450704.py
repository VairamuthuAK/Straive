import scrapy
import os
import csv
import requests
import time
import pandas as pd
from parsel import Selector
from ..utils import save_df
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.sync_api import sync_playwright

class GwuEduSpider(scrapy.Spider):
    """
    Scrapy spider for George Washington University (GSEHD).

    Scrapes:
    1. Course listings from GWU PWS system
    2. Faculty & staff directory
    3. Academic calendar (dates & deadlines)

    """
    name = "gwu"
    process_file_name = f"gwu_course_001.csv"
    institution_id = 258441754997450704

    # URLs
    course_url = "https://my.gwu.edu/mod/pws/"
    directory_url = "https://gsehd.gwu.edu/directory"
    calendar_url = "https://gsehd.gwu.edu/student-success/dates-deadlines"

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            self.parse_course()
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            self.parse_calendar(self.calendar_url)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            self.parse_calendar(self.calendar_url)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar(self.calendar_url)
        
        # All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar(self.calendar_url)

    def parse_course_safe_get(self, session, url, level, failed_urls):
        """
        Safely fetch a URL using requests.Session with retries.

        Prevents spider failure due to transient network or server issues.

        Args:
            session (requests.Session): Active HTTP session
            url (str): URL to fetch
            level (str): Logical level for error reporting
            failed_urls (list): Shared failure log

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

    def parse_course(self):
        """
        Parse course data using request session response.

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
                print(":warning: gwu_course_1.csv invalid — starting fresh")
                processed_urls = set()

        if not os.path.exists(self.process_file_name):
            with open(self.process_file_name, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["url"])
        
        course_data = []
        failed_urls = []
        
        # Configure HTTP session with retries
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


        # Main landing page
        resp = self.parse_course_safe_get(session, self.course_url, "main_page", failed_urls)
        if not resp:
            return  # nothing else possible

        sel = Selector(text=resp.text)
        
        # Extract campus-level links (excluding older terms)
        campus_links = sel.xpath(
            "//div[@class='scheduleMain']//div[@class='field']"
            "[.//div[@class='tableHeaderFont'][not(contains(., '2024'))]]"
            "//div[@class='scheduleBox']//ul//li//a/@href"
        ).getall()

        for camp in campus_links:
            campus_url = resp.url + camp
            
            campus_res = self.parse_course_safe_get(session, campus_url, "campus", failed_urls)
            if not campus_res:
                continue

            sel1 = Selector(text=campus_res.text)

            subject_links = sel1.xpath(
                "//div[@class='subjectsMain']//li/a/@href"
            ).getall()
            subject_links = list(dict.fromkeys(subject_links))
            
            time.sleep(3)

            for subject in subject_links:
                for page_num in range(1, 51):

                    subject_url = f"{resp.url}{subject}&pageNum={page_num}"

                    if subject_url in processed_urls:
                        continue

                    r = self.parse_course_safe_get(session, subject_url, "subject", failed_urls)
                    if not r:
                        break
                    
                    if r.status_code == 200:

                        tablesSel = Selector(text=r.text)
                        
                        tables = tablesSel.css("table.courseListing")
                        if not tables:
                            break
                        
                        # Save processed URL
                        with open(self.process_file_name, "a", encoding="utf-8", newline='') as f:
                            writer = csv.writer(f)
                            writer.writerow([subject_url])
                        
                        for table in tables:
                                subject_code = table.xpath(
                                    "normalize-space(.//tr[1]/td[3])"
                                ).get()
                                course = table.xpath(
                                    "normalize-space(.//tr[1]/td[5])"
                                ).get()

                                descUrl = table.xpath(
                                    ".//tr[1]/td[3]//a/@href"
                                ).get()

                                find_books_href = table.xpath(
                                    ".//a[contains(., 'Find Books')]/@href"
                                ).get(default="")

                                # Course description page
                                course_desc = ""
                                if descUrl:
                                    for attempt in range(1, 4):
                                        try:
                                            desc_res = session.get(descUrl, timeout=80)
                                            if desc_res.status_code == 200:
                                                desc_sel = Selector(text=desc_res.text)
                                                parts = desc_sel.xpath("//p[contains(@class,'courseblockdesc')]//text()").getall()
                                                course_desc = " ".join(p.strip() for p in parts if p.strip())
                                        except Exception as e:
                                            if attempt == 3:
                                                course_desc = descUrl
                                                failed_urls.append({
                                                    "level": "course_desc",
                                                    "url": descUrl,
                                                    "error": f"desc fetch failed {e}"
                                                })

                                row = {
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": r.url,
                                    "Course Name": f"{subject_code} {course}",
                                    "Course Description": course_desc,
                                    "Class Number": table.xpath(
                                        "normalize-space(.//tr[1]/td[2])"
                                    ).get(),
                                    "Section": table.xpath(
                                        "normalize-space(.//tr[1]/td[4])"
                                    ).get(),
                                    "Instructor": table.xpath(
                                        "normalize-space(.//tr[1]/td[7])"
                                    ).get(),
                                    "Enrollment": "",
                                    "Course Dates": table.xpath(
                                        "normalize-space(.//tr[1]/td[10])"
                                    ).get(),
                                    "Location": "",
                                    "Textbook/Course Materials": find_books_href
                                }

                                course_data.append(row)
                                
                    time.sleep(1.5)
            
        df = pd.DataFrame(course_data)
        save_df(df, self.institution_id, "course")
        
        # ---------- SAVE FAILED URLS ----------
        if failed_urls:
            failed_df = pd.DataFrame(failed_urls)
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            file_path = os.path.join(output_dir, f"{self.institution_id}course_failed_urls.csv")

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
        
        # List to store all directory records
        directory_data = []
        
        # Select all staff/faculty cards
        # Excludes the section "Faculty & Staff in the News"
        cards = response.xpath('//div[contains(@class,"card-group")][not(ancestor::article[.//h3[normalize-space()="Faculty & Staff in the News"]])]')
        
        # Loop through each staff card
        for card in cards:
            # Extract person name, title, email and phone
            name = card.xpath('.//span[contains(@class,"card-title")]/text()').get('')
            title = card.xpath('.//p[contains(@class,"card-person-role")]/text()').get('')
            email_href = card.xpath('.//div[@class="gw-email d-flex"]//a/@href').get()
            phone = card.xpath('.//div[contains(@class,"gw-phone")]//span/text()' ).get() 
            
            # Clean email by removing "mailto:"
            email = email_href.replace("mailto:", "") if email_href else ""
            
            # Store extracted values in dictionary
            directory_data.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email":email,
                "Phone Number": phone,
            })
        
        directory_df = pd.DataFrame(directory_data)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self, calenderURL):
        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        
        # This list will store all calendar rows (final output)
        calender_data = []
        
        # Start Playwright (opens a real browser engine)
        with sync_playwright() as p:
            
            # Launch Chromium browser in headless mode (no UI)
            browser = p.chromium.launch(headless=True)
            
            # Open a new browser tab/page
            page = browser.new_page()

            # Open the calendar URL
            # wait_until="networkidle" means:
            # → wait until all JS, APIs, images are fully loaded
            page.goto(calenderURL, wait_until="networkidle", timeout=120000)

            # Find accordion container that holds all terms
            accordions = page.query_selector_all("div.ckeditor-accordion-container")

            # Each term (Fall / Spring / Summer)
            dts = accordions[0].query_selector_all("dt")

            # Loop through each academic term
            for dt in dts:
                
                # Read visible text of the term name
                term_name = dt.inner_text().strip()

                # Expand accordion if collapsed
                dt.click()
                
                # Wait a bit so content loads after click
                page.wait_for_timeout(300)

                # After <dt>, the next HTML element is <dd>
                dd = dt.evaluate_handle("el => el.nextElementSibling")
                
                # Get all table rows inside this term
                rows = dd.query_selector_all("table tr")
                
                # Loop through each row in the calendar table
                for row in rows:
                    
                    cols = row.query_selector_all("td")
                    
                    # Skip rows that don’t have enough columns
                    if len(cols) < 2:
                        continue
                    
                    # First column = event description
                    event = cols[0].inner_text().strip()
                    # Second column = date
                    date = cols[1].inner_text().strip()
                    
                    # Save extracted data into list
                    calender_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": calenderURL,
                        "Term Name": term_name,
                        "Term Date": date,
                        "Term Date Description": event,
                    })
            # Close the browser to free memory
            browser.close()
            
            calendar_df = pd.DataFrame(calender_data)
            save_df(calendar_df, self.institution_id, "calendar")

