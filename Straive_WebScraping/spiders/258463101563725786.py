import re
import io
import json
import scrapy
import PyPDF2
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from parsel import Selector
from playwright.sync_api import sync_playwright


class CentralSpider(scrapy.Spider):
    """
    Spider to scrape:
    - Course schedule data (dynamic content using Playwright)
    - Faculty / staff directory
    - Academic calendar from PDF
    """

    name = "cacc"

    # Constant institution identifier used in all outputs
    institution_id = 258463101563725786

    # URL for dynamic course search page
    course_url = "https://reg-prod.ec.accs.edu/StudentRegistrationSsb/ssb/term/termSelection?mode=search&mepCode=CACC"

    # URL for faculty / staff directory
    directory_url = "https://www.cacc.edu/about/faculty-staff-directory"

    # URL for academic calendar PDF
    calendar_url = "https://www.cacc.edu/content/userfiles/files/Academic%20Calendar/2025-2026%20Academic%20Calendar.pdf"


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # List to store faculty / staff directory records
        self.directory_rows = []

        # List to store academic calendar records
        self.calendar_rows = []

        # List to store course records
        self.course_rows = []


    def start_requests(self):
        """
        Controls which section to scrape based on SCRAPE_MODE setting
        """

        # Read SCRAPE_MODE from settings, default is "all"
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        # If only course data is required
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        # If only directory data is required
        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        # If only calendar data is required
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # If both course and directory are required
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        # If both course and calendar are required
        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # If both directory and calendar are required
        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        # Default case: scrape all sections
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)


    def clean(self, value):
        # Safely return stripped text, even if value is None
        return (value or "").strip()


    def parse_course(self, response):
        """
        Scrapes course data using Playwright
        """

        # Launch Playwright browser
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()

            # Open course search page
            page.goto(self.course_url)

            # Click term dropdown
            page.click('//b[@role="presentation"]')
            page.wait_for_selector('li[role="presentation"]')

            # Count available terms
            term_count = page.locator('li[role="presentation"]').count()

            # Loop through selected term(s)
            for t in range(3, 4):

                # Select the specific term
                item = page.locator('li[role="presentation"]').nth(t)
                item.scroll_into_view_if_needed()
                item.click()

                # Wait for term selection to apply
                page.wait_for_timeout(1000)

                # Submit term selection
                page.click('//button[@id="term-go"]')
                page.wait_for_selector('//button[@id="search-go"]')

                # Click search button
                page.click('//button[@id="search-go"]')

                # Wait until course table loads
                page.wait_for_selector('//table[@role="grid"]//tr')

                # Loop through paginated result pages
                while True:

                    # Get all course rows
                    rows = page.locator('//table[@role="grid"]//tr')
                    row_count = rows.count()

                    # Loop through each course row (skip header row)
                    for i in range(1, row_count):
                        row = rows.nth(i)

                        # Extract course name
                        course_name = row.locator('xpath=.//td[1]//a').text_content().strip()

                        # Extract course number
                        course_number = row.locator('xpath=.//td[3]').text_content().strip()

                        # Extract section value
                        section = row.locator('xpath=.//td[4]').text_content().strip()

                        # Extract class number
                        class_number = row.locator('xpath=.//td[6]').text_content().strip()

                        # Extract location
                        location = row.locator('xpath=.//td[10]').text_content().strip()

                        # Extract enrollment count
                        enrollment = row.locator('xpath=.//td[11]').inner_text().strip()

                        # Extract instructor names
                        instructors = row.locator(
                            'xpath=.//td[8]//a[@class="email"]'
                        ).all_text_contents()

                        # Join multiple instructors into a single string
                        instructor = ", ".join(i.strip() for i in instructors)

                        # Extract start and end date text
                        td_text = row.locator('xpath=.//td[9]').inner_text()

                        # Find start date using regex
                        start_match = re.search(r"Start Date:\s*([0-9/]+)", td_text)

                        # Find end date using regex
                        end_match = re.search(r"End Date:\s*([0-9/]+)", td_text)

                        # Assign dates if found
                        startdate = start_match.group(1) if start_match else ""
                        enddate = end_match.group(1) if end_match else ""

                        # Open course description modal
                        first_link = row.locator('xpath=.//a').first
                        first_link.scroll_into_view_if_needed()
                        first_link.click()

                        # Click description tab
                        page.click('//h3[@id="courseDescription"]')
                        page.wait_for_selector('//section[@aria-labelledby="courseDescription"]')

                        # Parse modal HTML using Selector
                        desc_sel = Selector(text=page.content())

                        # Extract description text
                        description = desc_sel.xpath(
                            '//section[@aria-labelledby="courseDescription"]//text()'
                        ).getall()

                        # Clean and normalize description text
                        description = " ".join(" ".join(description).split())

                        # Close the modal popup
                        page.click('//button[@class="ui-dialog-titlebar-close"]')
                        page.wait_for_timeout(300)

                        # Append extracted course data
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": "https://reg-prod.ec.accs.edu/StudentRegistrationSsb/ssb/classSearch/classSearch",
                            "Course Name": f"{course_number} {course_name}",
                            "Course Description": description,
                            "Class Number": class_number,
                            "Section": "",
                            "Instructor": instructor,
                            "Enrollment": enrollment,
                            "Course Dates": f"{startdate} - {enddate}",
                            "Location": location,
                            "Textbook/Course Materials": "",
                        })

                    # Save course data after each page
                    course_df = pd.DataFrame(self.course_rows)
                    save_df(course_df, self.institution_id, "course")

                    # Click next page button if available
                    next_btn = page.locator('//button[@title="Next" and not(@disabled)]')
                    next_btn.click()

                    # Wait for next page to load
                    page.wait_for_timeout(10000)


    def parse_directory(self, response):
        """
        Extracts faculty / staff profile links
        """

        # Get all profile links from directory table
        links = response.xpath(
            '//table[@class="table table-bordered table-hover"]//tr/td/a/@href'
        ).getall()

        # Loop through each profile link
        for link in links:
            yield scrapy.Request(
                response.urljoin(link),
                callback=self.parse_directory_details
            )


    def parse_directory_details(self, response):
        """
        Extracts individual faculty / staff details
        """

        # Extract faculty/staff name
        name = response.xpath("//h2/text()").get(default="").strip()

        # Extract email address
        email = response.xpath('//div[@class="mb-2"]/a/@href').get("").replace("mailto:", "").strip()

        # Extract title
        title = response.xpath("//h3/text()").get(default="").strip()

        # If title is missing, try alternate selector
        if not title:
            title = response.xpath('//p[@class="h4 mb-1"]//text()').get("")

        # Extract phone number href
        phone_href = response.xpath('//a[starts-with(@href,"tel:")]/@href').get("")

        # Keep only digits from phone number
        digits = re.sub(r"\D", "", phone_href.replace("tel:", ""))

        # Format phone number if valid
        formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else ""

        # Extract phone extension text
        ext_raw = response.xpath(
            "//a[span[text()='Phone number']]/following-sibling::text()"
        ).get("").strip()

        # Combine phone number and extension
        phone = f"{formatted} {ext_raw}".strip() if formatted else ""

        # Append directory record
        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

        # Save directory data
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):
        """
        Parses academic calendar PDF and extracts term-based dates
        """

        # List to store extracted calendar data
        calendar_data = []

        # Convert PDF response body to BytesIO
        pdf_bytes = io.BytesIO(response.body)

        # Variable to track current academic term
        current_term = None

        # Open PDF using pdfplumber for text and table extraction
        with pdfplumber.open(pdf_bytes) as pdf:

            # Loop through each page of the PDF
            for page in pdf.pages:

                # Extract text content from page
                text = page.extract_text() or ""
                text_lower = text.lower()

                # Identify term based on keywords and year
                if "fall" in text_lower and "2025" in text_lower:
                    current_term = "Fall 2025"
                elif "spring" in text_lower and "2026" in text_lower:
                    current_term = "Spring 2026"
                elif "summer" in text_lower and "2026" in text_lower:
                    current_term = "Summer 2026"

                # Skip page if term is not identified or summary page
                if not current_term or "summary" in text_lower:
                    continue

                # Loop through tables in the page
                for table in page.extract_tables():

                    # Loop through each row in the table
                    for row in table:

                        # Skip empty or invalid rows
                        if not row or len(row) < 2:
                            continue

                        # Extract date cell
                        date_cell = (row[0] or "").strip()

                        # Combine remaining cells as description
                        desc_cell = " ".join(c for c in row[1:] if c).strip()

                        # Skip if date or description is missing
                        if not date_cell or not desc_cell:
                            continue

                        # Remove weekday name from date
                        date_cell = re.sub(
                            r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*",
                            "",
                            date_cell,
                            flags=re.I
                        )

                        # Validate date format (Month Day, Year)
                        if not re.match(
                            r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
                            date_cell,
                            re.I
                        ):
                            continue

                        # Append calendar record
                        calendar_data.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": current_term,
                            "Term Date": date_cell,
                            "Term Date Description": desc_cell,
                        })

        # Save calendar data
        calendar_df = pd.DataFrame(calendar_data)
        save_df(calendar_df, self.institution_id, "calendar")
