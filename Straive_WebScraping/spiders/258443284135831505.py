import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from parsel import Selector
from ..utils import save_df
from datetime import datetime
from playwright.sync_api import sync_playwright



def normalize_date(text, current_year):
    if not text:
        return "", current_year

    match = re.search(
        r'(?:\d+\s*event[s]*|No events),\s*'
        r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s*'
        r'(\d{1,2})\s+([A-Za-z]+)',
        text
    )

    if not match:
        return "", current_year

    weekday, day, month = match.groups()
    dt = datetime.strptime(f"{day} {month} {current_year}", "%d %B %Y")
    formatted = dt.strftime("%d %b, %a").upper()
    return formatted, current_year

class NorthwestSpider(scrapy.Spider):

    """
    Scrapy spider for John Wood Community College (JWCC).

    This spider supports scraping:
    1. Course catalog (PDF-based)
    2. Employee directory (AJAX endpoint)
    3. Academic calendar (HTML pages with pagination)

    Scrape behavior is controlled by the SCRAPE_MODE setting.
    """

    name = "northwest"

    # Unique institution identifier used by downstream pipelines
    institution_id = 258443284135831505

    calendar_rows = []

    course_url = "https://nwbanxe.utoledo.edu/StudentRegistrationSsb/ssb/term/termSelection?mode=search"
    directory_url = "https://northweststate.edu/wp-content/uploads/2025/11/2025-2026-Faculty-Handbook.pdf"
    calendar_url = "https://northweststate.edu/calendars/"
    
    def start_requests(self):
        """
        Entry point for the spider.

        The SCRAPE_MODE setting determines which sections are executed.
        Supported values:
            - course
            - directory
            - calendar
            - any combination (course_directory, course_calendar, etc.)
            - default: all three
        """

        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      
        # Single functions
        if mode == 'course':
            self.parse_course()
            
        elif mode == 'directory':
           self.parse_directory()
           
        elif mode == 'calendar':
            self.parse_calendar()
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            self.parse_directory()


        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            self.parse_calendar()

        elif mode in ['directory_calendar', 'calendar_directory']:
            self.parse_calendar()
            self.parse_directory()
        
        # All three (default)
        else:
            self.parse_course()
            self.parse_directory()
            self.parse_calendar()
       

    # Parse methods UNCHANGED from your original
    def parse_course(self):

        course_rows = []  
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            page.goto("https://nwbanxe.utoledo.edu/StudentRegistrationSsb/ssb/term/termSelection?mode=search")
            page.wait_for_timeout(6000)

            #OPEN TERM DROPDOWN
            page.click('//b[@role="presentation"]')
            page.wait_for_selector('li[role="presentation"]')

            term_count = page.locator('li[role="presentation"]').count()
            for t in range(1, 2):  # adjust range as needed
                print(f"\nProcessing term {t + 1}/{term_count}")

                # reopen dropdown every iteration
                item = page.locator('li[role="presentation"]').nth(t)
                item.scroll_into_view_if_needed()
                item.click()
                page.wait_for_timeout(1000)
                page.click('//button[@id="term-go"]')
                page.wait_for_selector('//button[@id="search-go"]')
                page.click('//button[@id="search-go"]')
                page.wait_for_selector('//table[@role="grid"]//tr')

                #PAGINATION
                while True:
                    rows = page.locator('//table[@role="grid"]//tr')
                    row_count = rows.count()

                    print("Rows on page:", row_count)

                    for i in range(1, row_count):
                        row = rows.nth(i)
                        course_name = row.locator('xpath=.//td[1]//a').text_content().strip()
                        course_number = row.locator('xpath=.//td[3]').text_content().strip()
                        section = row.locator('xpath=.//td[@data-content="Section"]').text_content().strip()
                        instrs = row.locator('xpath=.//td[8]//a[@class="email"]')
                        instructors = instrs.all_text_contents()
                        instructor = ", ".join(i.strip() for i in instructors)

                        section = row.locator('xpath=.//td[4]').text_content().strip()
                        location = row.locator('xpath=.//td[10]').text_content().strip()
                        td_text = row.locator('xpath=.//td[9]').inner_text()
                        start_match = re.search(r"Start Date:\s*([0-9/]+)", td_text)
                        end_match   = re.search(r"End Date:\s*([0-9/]+)", td_text)

                        startdate = start_match.group(1) if start_match else ""
                        enddate   = end_match.group(1) if end_match else ""
                    
                        class_number = row.locator('xpath=.//td[6]').text_content().strip()

                        enrollment = row.locator('xpath=.//td[11]').inner_text().strip()
                        # Remove space after FULL:
                        enrollment = re.sub(r'FULL:\s+', 'FULL:', enrollment)

                        #Remove spaces after periods
                        enrollment = re.sub(r'\.\s+', '.', enrollment)

                        #Remove spaces immediately before numbers
                        enrollment = re.sub(r'\s+(\d+)', r'\1', enrollment)

                        #DESCRIPTION
                        description = ""

                        rows = page.locator('//table[@role="grid"]//tr')
                        row = rows.nth(i)
                        try:
                            first_link = row.locator('xpath=.//a').first
                            first_link.scroll_into_view_if_needed()
                            first_link.click()

                            page.click('//h3[@id="courseDescription"]//a')
                            page.wait_for_selector('//section[@aria-labelledby="courseDescription"]')

                            desc_html = page.content()
                            desc_sel = Selector(text=desc_html)
                            description = desc_sel.xpath('//section[@aria-labelledby="courseDescription"]//text()').getall()
                            description = " ".join(" ".join(description).split())

                            # close modal safely
                            close_btn = page.locator('//button[@class="ui-dialog-titlebar-close"]')
                            if close_btn.count() > 0:
                                close_btn.click()
                                page.wait_for_timeout(300)
                        except:
                            pass
                        row_data = {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": page.url,
                            "Course Name": f"{course_number} {course_name}",
                            "Course Description": description,
                            "Class Number": class_number,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": enrollment,
                            "Course Dates": f"{startdate} - {enddate}",
                            "Location": location,
                            "Textbook/Course Materials": "",
                        }

                        course_rows.append(row_data)
                        print("Saved:", row_data["Course Name"])

                    next_btn = page.locator('//button[@title="Next" and not(@disabled)]')
                    if next_btn.count() == 0:
                        break

                    next_btn.click()
                    page.wait_for_timeout(8000)

            browser.close()

        #SAVE CSV
        course_df = pd.DataFrame(course_rows)
        save_df(course_df, self.institution_id, "course")
            
    def parse_directory(self):
        rows = []
        resp = requests.get(self.directory_url)
        resp.raise_for_status()
        pdf_bytes = io.BytesIO(resp.content)

        #EXTRACT PDF TEXT
        text = ""
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"

        # Name must start with capital
        # Must contain EXT.1234
        record_pattern = re.compile(r"^([A-Z][A-Za-z.,\s]+?)\s{1,}(.+?)\s+EXT\.?(\d+)$")

        for line in text.splitlines():
            line = line.strip()

            match = record_pattern.search(line)
            if not match:
                continue

            name = match.group(1).strip()
            title = match.group(2).strip()
            ext = match.group(3).strip()

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.directory_url,
                "Name": name,
                "Title": title,
                "Email": "",
                "Phone Number": f"EXT.{ext}"
            })


            directory_df = pd.DataFrame(rows)
            save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self):
        
        all_rows = []
        MONTHS_TO_SCRAPE = 2
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            page.goto(self.calendar_url, wait_until="networkidle")

            # Enable Academic Calendar
            page.wait_for_selector("span.dsm-switch-inner", timeout=20000)
            page.click("span.dsm-switch-inner")

            # View Calendar
            page.wait_for_selector("text=VIEW CALENDAR", timeout=20000)
            page.click("text=VIEW CALENDAR")
            page.wait_for_timeout(5000)

            # Find Google Calendar iframe
            calendar_frame = None
            for frame in page.frames:
                if "calendar.google.com" in frame.url:
                    calendar_frame = frame
                    break

            if not calendar_frame:
                raise Exception("Google Calendar iframe not found")
            html = calendar_frame.content()
            sel = Selector(text=html)
            # Loop months
            current_year = 2026
            for _ in range(MONTHS_TO_SCRAPE):
                calendar_frame.wait_for_selector('//div[@role="gridcell"]', timeout=30000)
                
                sel = Selector(text=calendar_frame.content())
                cells = sel.xpath('//div[@role="gridcell"]')

                for cell in cells:
                    raw_date = cell.xpath('.//h2/text()').get()
                    term_date, current_year = normalize_date(raw_date, current_year)

                    events = cell.xpath('.//span[@class="XuJrye"]/text()').getall()

                    for event in events:
                        parts = [p.strip() for p in event.split(',')]
                        term_description = parts[1] if len(parts) > 1 else ""
                        term_name = sel.xpath('//span[@class="XuJrye"]/text()').get('')
                        all_rows.append({
                            "Cengage Master Institution ID": 258443284135831505,
                            "Source URL": self.calendar_url,
                            "Term Name": term_name,
                            "Term Date": term_date,
                            "Term Date Description": term_description,
                        })

                # Next month
                calendar_frame.click('button[aria-label="Next month"]')
                calendar_frame.wait_for_timeout(3000)

            browser.close()
        # Save only once after last page
        calendar_df = pd.DataFrame(all_rows)
        save_df(calendar_df, self.institution_id, "calendar")





 