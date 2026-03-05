import scrapy
import pandas as pd
from ..utils import *
from playwright.sync_api import sync_playwright


class PennHighlandsSpider(scrapy.Spider):
    name = "highlands"
    institution_id = 258418749210650593

    # Course Page URL
    course_url = "https://my.pennhighlands.edu/ICS/Semester_Schedules.jnz?portlet=Schedules_Portlet"
    
    # Academic calendar page URL
    calendar_url = "https://www.pennhighlands.edu/admissions/registration/academic-calendar/" 

    # Directory URL
    directory_url="https://www.pennhighlands.edu/about/college-directory/"

    # calendar_headers 
    calendar_headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    }

    # directory_headers
    directory_headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
   }


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar,dont_filter=True)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar,dont_filter=True)
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar,dont_filter=True)
       

    def parse_course(self, response):
        """
        Parse course schedule using Playwright

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Course Name"                   : str
        - "Course Description"            : str
        - "Class Number"                  : str
        - "Section"                       : str
        - "Instructor"                    : str
        - "Enrollment"                    : str
        - "Course Dates"                  : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        """
        Used Playwright to scrape 

        - Launch browser and load the main course schedule page
        - Collect all academic term links
        - Reset page state and open each term
        - Traverse paginated course listings
        - Open course detail popup windows
        - Extract course-level and schedule-level data
        - Save all collected data once at the end
        """

        # Helper to safely extract visible text
        def safe_text(locator):
            try:
                return locator.first.inner_text().strip()
            except:
                return ""

        rows = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            # Load MAIN page
            page.goto(self.course_url, wait_until="domcontentloaded")
            page.wait_for_selector("#pg0_V_grdTerms", timeout=300000)
            # locate all term links
            term_links = page.locator("#pg0_V_grdTerms a")
            term_count = term_links.count()

            # LOOP THROUGH TERMS
            for term_index in range(term_count):
                # Reload page to reset ASP.NET postback state
                page.goto(self.course_url, wait_until="domcontentloaded")
                page.wait_for_selector("#pg0_V_grdTerms", timeout=300000)

                term_links = page.locator("#pg0_V_grdTerms a")
                term_link = term_links.nth(term_index)
                term_name = term_link.inner_text().strip()
                
                # Click term and wait for navigation
                with page.expect_navigation(wait_until="domcontentloaded"):
                    term_link.click()

                page.wait_for_selector("#pg0_V_grdCourses", timeout=300000)
                page_no = 1

                # PAGINATION
                while True:
                    self.logger.info(f"[{term_name}] Parsing page {page_no}")
                    # Locate course rows on current page
                    course_rows = page.locator(
                        "#pg0_V_grdCourses tr.RowStyle, "
                        "#pg0_V_grdCourses tr.AlternatingRowStyle"
                    )

                    # Loop through each course row
                    for i in range(course_rows.count()):
                        row = course_rows.nth(i)
                        course_link = row.locator("a").first

                        if not course_link.count():
                            continue

                        # OPEN DETAIL POPUP
                        with context.expect_page() as popup_info:
                            course_link.click()

                        popup = popup_info.value
                        popup.wait_for_load_state("domcontentloaded")

                        # Extract course-level data
                        course_title = safe_text(
                            popup.locator("#fvCrsDetails1_lblCrsTitle")
                        )

                        course_code_raw = safe_text(
                            popup.locator("#fvCrsDetails1_lblCrsCde")
                        )

                        parts = course_code_raw.split()
                        subject = parts[0] if len(parts) > 0 else ""
                        course_number = parts[1] if len(parts) > 1 else ""
                        section = parts[2] if len(parts) > 2 else ""
                        class_number = parts[3] if len(parts) > 3 else ""

                        course_name = f"{subject} {course_number} {course_title}".strip()
                        seats_raw = safe_text(
                            popup.locator("#fvCrsDetails1_lblSeats")
                        )
                        enrollment = seats_raw.replace("/", " of ") if seats_raw else ""

                        course_description = safe_text(
                            popup.locator("#fvCrsDetails1_lblCourseDesc")
                        )
                        # Textbook link
                        textbook_link = ""

                        if popup.locator("#fvCrsDetails1_hypLinkBookNow").count():
                            textbook_link = popup.locator(
                                "#fvCrsDetails1_hypLinkBookNow"
                            ).get_attribute("href")

                        # Schedule table (one row per schedule)
                        schedule_rows = popup.locator(
                            "table.subTable tr"
                        ).filter(has_not=popup.locator("strong"))

                        # Loop through each schedule row
                        for j in range(schedule_rows.count()):
                            tds = schedule_rows.nth(j).locator("td")

                            if tds.count() < 5:
                                continue

                            instructor = safe_text(tds.nth(0))
                            course_dates = safe_text(tds.nth(2))
                            location = safe_text(tds.nth(3))

                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Course Name": course_name,
                                "Course Description": course_description,
                                "Class Number": class_number,
                                "Section": section,
                                "Instructor": instructor,
                                "Enrollment": enrollment,
                                "Course Dates": course_dates,
                                "Location": location,
                                "Textbook/Course Materials": textbook_link,
                            })

                        popup.close()

                    # NEXT PAGE
                    next_btn = page.locator("a:has-text('Next')")

                    if next_btn.count() and next_btn.first.is_visible():
                        with page.expect_navigation(wait_until="domcontentloaded"):
                            next_btn.first.click()
                        page_no += 1
                    else:
                        self.logger.info(f"[{term_name}] No more pages")
                        break

            browser.close()

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "course")


    def parse_directory(self, response):
        """
        Parse directory using Scrapy response.

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Name
        - Title
        - Email
        - Phone Number
        """

        rows = []
        
        # select each person entry in the directory list
        people = response.xpath('//ul[contains(@class,"directoryListing")]/li')
        
        # loop through each directory record
        for person in people:
            full_text = person.xpath('.//span[@class="directoryName"]/text()').get()
            phone = person.xpath('.//span[@class="directoryPhone"]/text()').get()

            if not full_text:
                continue

            # Split name and title safely
            parts = [p.strip() for p in full_text.split(",")]

            name = ", ".join(parts[:2]) if len(parts) >= 2 else parts[0]
            title = ", ".join(parts[2:]) if len(parts) > 2 else ""

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": "",  
                "Phone Number": phone.strip() if phone else ""
            })

        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):
        """
        Parse academic calendar.

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Term Name
        - Term Date
        - Term Date Description
        """

        calendar_rows = []

        # Loop through each term block (Fall 2025, Spring 2026, Summer 2026)
        term_blocks = response.xpath('//ul[contains(@class,"accordionGroup")]')

        for term in term_blocks:
            # Extract term name
            term_name = term.xpath('.//h3/a/text()').get()

            if not term_name:
                continue
            term_name = term_name.strip()

            # Loop through each event row
            event_rows = term.xpath('.//div[contains(@class,"accordionContent")]//p')

            for event in event_rows:
                # Extract date
                term_date = event.xpath('.//strong/text()').get()

                if not term_date:
                    continue
                term_date = term_date.strip()
                term_date = term_date.replace("\u2013", "-").replace("\u2014", "-")
                # Extract description (everything except the <strong> date)
                description_parts = event.xpath('.//text()[not(parent::strong)]').getall()
                term_description = " ".join(description_parts).strip()
                # FIX NON-BREAKING SPACE ONLY
                term_description = term_description.replace("\u00A0", " ")

                if not term_description:
                    continue

                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": term_date,
                    "Term Date Description": term_description
                })

        # SAVE OUTPUT
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
