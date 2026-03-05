import re
import time
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from ..utils import save_df
from playwright.sync_api import sync_playwright


class SouthernSpider(scrapy.Spider):
    name = "southern"

    institution_id = 258461178982852574
    calendar_rows = []

    # Academic terms used for course scraping
    course_url = "https://www.sscc.edu/academics/class-schedules.shtml"
    course_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
    }
    
    # Employee directory URL
    directory_url = "https://www.sscc.edu/facultystaff/staff-directory.shtml"
    
    # Academic calendar page URL
    calendar_url = 'https://www.sscc.edu/academics/academic-calendar.shtml'
    calendar_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }
    
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
           self.parse_directory()
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            self.parse_directory()
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

    # Parse methods UNCHANGED from your original
    def parse_course(self, response):
        """
        Parse course data using Scrapy response and SSCC PDF.
        Outputs:
            - Cengage Master Institution ID
            - Source URL
            - Course Name
            - Course Description
            - Class Number
            - Section
            - Instructor
            - Enrollment
            - Course Dates
            - Location
            - Textbook/Course Materials
        """


        course_rows = []

        # Get PDF URL from page
        spring_semester_url = response.urljoin(
            response.xpath('//ul[@class="list-default-no-style-no-padding"]/li/a/@href').get()
        )
       
        pdf_response = requests.get(spring_semester_url,headers=self.course_headers, timeout=60)
        pdf_bytes = BytesIO(pdf_response.content)

        # Regex patterns
        course_re = re.compile(r"([A-Z]{3,4}\s*\d{4})\s*([A-Z0-9]{1,3})")
        credits_re = re.compile(r"\b(\d+\.\d{2})\b")
        date_re = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b|\b(\d{2}-\d{2}-\d{4})\b")
        time_re = re.compile(r"\d{1,2}:\d{2}\s?(AM|PM)", re.I)

        records = []
        current = None

        # Parse PDF
        with pdfplumber.open(pdf_bytes) as pdf:
            total_pages = len(pdf.pages)
            print(f"📄 Total pages in PDF: {total_pages}")

            # Pages 2–24 → index 1 to 23
            for page_num in range(1, min(24, total_pages)):
                page = pdf.pages[page_num]
                text = page.extract_text()

                if not text:
                    print(f"⚠️ Skipped empty page: {page_num+1}")
                    continue

                lines = text.split("\n")
                for idx, line in enumerate(lines):
                    line_strip = line.strip()
                    if not line_strip:
                        continue

                    # Relaxed header/footer filter
                    if line_strip.lower().startswith((
                        "spring fees", "course note", "credits location", "days time",
                        "spring semester", "transfer module start date end date location room days time instructor"
                    )):
                        previous_line = line_strip
                        continue

                    # NEW COURSE LINE
                    course_match = course_re.search(line_strip)
                    if course_match:
                        if current:
                            records.append(current)

                        course = course_match.group(1)
                        transfer_module = course_match.group(2)

                        credits = None
                        cm = credits_re.search(line_strip)
                        if cm:
                            credits = cm.group(1)

                        title_part = line_strip.split(transfer_module, 1)[-1]
                        if credits:
                            title_part = title_part.replace(credits, "")

                        course_title = title_part.strip()
                        if 'Course Note' in course_title:
                            course_title = course_title.split('Course Note')[0].strip()
                            
                        current = {
                            "course": course,
                            "transfer_module": transfer_module,
                            "course_title": course_title,
                            "credits": credits,
                            "start_date": None,
                            "end_date": None,
                            "location": None,
                            "instructor": None
                        }

                    # CONTINUATION LINE
                    if current:
                        # Dates
                        dates = date_re.findall(line_strip)
                        if len(dates) >= 2:
                            flat_dates = [d[0] or d[1] for d in dates]
                            current["start_date"] = flat_dates[0]
                            current["end_date"] = flat_dates[1]

                        # Location
                        loc = line_strip.lower()
                        if "online" in loc:
                            current["location"] = "Online"
                        elif "off-campus" in loc:
                            current["location"] = "Off-Campus"
                        elif "campus" in loc:
                            current["location"] = "Campus"

                        # Instructor handling
                        instructor_candidate = None
                        times = time_re.findall(line_strip)

                        if times:
                            last_time_idx = max([line_strip.rfind(t[0]) for t in times])
                            instructor_candidate = line_strip[last_time_idx+len(times[-1][0]):].strip()
                        else:
                            # Check if current line is a likely instructor
                            words = line_strip.split()
                            if 0 < len(words) <= 3 and all(c.isalpha() or c in "-." for c in line_strip.replace(" ", "")):
                                instructor_candidate = line_strip.strip()
                            else:
                                # Possibly after location keyword
                                for loc_word in ["Online", "Campus", "Off-Campus"]:
                                    if loc_word in line_strip:
                                        candidate = line_strip.split(loc_word)[-1].strip()
                                        # Only short words are likely instructor
                                        if 0 < len(candidate.split()) <= 3:
                                            instructor_candidate = candidate
                                        break

                        # Assign instructor if valid
                        if instructor_candidate:
                            invalids = ["", "m", "transfer module start date end date location room days time instructor"]
                            if instructor_candidate.lower() not in invalids:
                                current["instructor"] = " ".join(instructor_candidate.split()[:3])
                            elif idx+1 < len(lines):
                                # Look at next line if current invalid
                                next_line = lines[idx+1].strip()
                                next_words = next_line.split()
                                if 0 < len(next_words) <= 3 and all(c.isalpha() or c in "-." for c in next_line.replace(" ", "")):
                                    current["instructor"] = next_line

        if current:
            records.append(current)

        # Map PDF records to Scrapy output
        for rec in records:
            course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": spring_semester_url,
                "Course Name": f"{rec['course']} {rec['transfer_module']} {rec['course_title']}",
                "Course Description": "",  # PDF does not have description
                "Class Number": f"{rec['course']} {rec['transfer_module']}",
                "Section": "",
                "Instructor": rec['instructor'] or "",
                "Enrollment": "",
                "Course Date": f"{rec['start_date']} - {rec['end_date']}" if rec['start_date'] else "",
                "Location": rec['location'] or "",
                "Textbook/Course Materials": ""
            })

        # Optional: save to CSV for debugging
        course_df = pd.DataFrame(course_rows)
        save_df(course_df, self.institution_id, "campus")
        print(f"✅ Parsed {len(course_rows)} courses from PDF")

            
    def parse_directory(self):
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
        campus_row = []

        # Launch Playwright browser session
        with sync_playwright() as p:
            # Launch Chromium (set headless=True for production)
            browser = p.chromium.launch(headless=False)  

            page = browser.new_page()

            # Navigate to staff directory page
            page.goto(
                "https://www.sscc.edu/facultystaff/staff-directory.shtml",
                wait_until="load"
            )

            # Allow page and iframe content to fully load
            time.sleep(2)  

            # Locate the embedded iframe containing the staff table
            staff_frame = None
            for frame in page.frames:
                if "organimi.com" in frame.url:
                    staff_frame = frame
                    break

            # Fail early if iframe is not found
            if not staff_frame:
                raise Exception("Staff iframe not found")

            # Wait for the staff table body to be present
            staff_frame.wait_for_selector("//tbody[contains(@class,'MuiTableBody-root')]", timeout=30000)

            # Open the pagination dropdown
            staff_frame.locator("//div[contains(@class,'MuiTablePagination-select')]").first.click()

            # Select "100" rows per page to load all records
            staff_frame.locator("//li[text()='100']").click()
            time.sleep(3)
            rows = staff_frame.locator("//tbody[contains(@class,'MuiTableBody-root')]//tr")
            row_count = rows.count()
            print(f"Total rows found: {row_count}")

            # Iterate over each staff row
            for i in range(row_count):
                # Use CSS selector for all <td> in this row
                cells = rows.nth(i).locator("td")  # <-- fixed

                # Skip malformed or incomplete rows
                if cells.count() < 7:
                    continue

                first_name = cells.nth(2).inner_text().strip()
                last_name = cells.nth(3).inner_text().strip()
                name = f"{first_name} {last_name}"
                title = cells.nth(4).inner_text().strip()
                phone = cells.nth(5).inner_text().strip().replace('x', '').replace('X', '').strip()
                email = cells.nth(6).inner_text().strip()

                campus_row.append(
                        {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_url,
                        "Name": name,
                        "Title": title,
                        "Email": email,
                        "Phone Number": phone,
                        }
                    )
        # Convert records into DataFrame
        directory_df = pd.DataFrame(campus_row)

        # Persist data using shared save utility
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):
        # Select all term panels (e.g., Spring, Summer, Fall)
        main_blocks = response.xpath('//div[@class="panel"]')
        for main_block in main_blocks:

            # Extract individual list items under each term
            li_items = main_block.xpath('.//ul//li')
            term_name = main_block.xpath('.//h3/text()').get('')
            for li in li_items:
                term_date = li.xpath('.//text()').get('').strip()
                term_description = li.xpath('./strong/following-sibling::text()').get('').replace('—','').strip()
                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,  
                    "Term Date": term_date,
                    "Term Date Description": term_description,
                })

        calendar_df = pd.DataFrame(self.calendar_rows)
        # Persist calendar data
        save_df(calendar_df, self.institution_id, "calendar")


 