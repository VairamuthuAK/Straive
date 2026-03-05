import re
import time
import scrapy
import pandas as pd
import PyPDF2
from io import BytesIO
from playwright.sync_api import sync_playwright
from parsel import Selector
from ..utils import save_df   # Project utility function


class InfoStudentSpider(scrapy.Spider):
    """
    Spider to scrape:
    1. Course schedule (Playwright)
    2. Employee directory
    3. Academic calendar (PDF + XLSX)
    """

    name = "infostudent"
    institution_id = 258454055125280731

    course_url = "https://infostudent.fvsu.edu/Schedule.aspx"
    directory_url = "https://www.fvsu.edu/directory"
    calendar_url = "https://www.fvsu.edu/about-fvsu/academic-calendar"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # START REQUESTS BASED ON MODE
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower()

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default: run all
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE SCRAPING (Playwright)
    def parse_course(self, response):
        """
        Load course page using Playwright and scrape course table
        """

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Load schedule page
            page.goto(self.course_url, wait_until="domcontentloaded", timeout=120000)

            # Select all terms
            page.wait_for_selector("#ddlTerms")
            page.select_option("#ddlTerms", value="-+-----+- Select All Terms -+-----+-")

            # Click search
            page.click("#btnSearch")

            # Wait for results table
            page.wait_for_selector("table[id^='dlSchedInfo_tblSchedInfo']", timeout=180000)

            html = page.content()
            response = Selector(text=html)

        # Extract course rows
        rows = response.xpath("//table[starts-with(@id,'dlSchedInfo_tblSchedInfo')]//tr[@valign='top']")

        previous_data = []
        for row in rows[1:]:
            data = row.xpath('.//td[starts-with(@id,"tdPOT")]//text()').getall()
            data = [d.strip() for d in data if d.strip()]

            # Some rows reuse previous row data
            if not data:
                data = previous_data
            else:
                previous_data = data

            # Extract date range
            date_result = ""
            if len(previous_data) > 2:
                match = re.search(r"Starts:\s*(.*?)\s*---\s*Ends:\s*(.*)", previous_data[2])
                if match:
                    date_result = f"{match.group(1)} - {match.group(2)}"

            course_name = f"{row.xpath('./td[2]/text()').get('').strip()} {row.xpath('./td[3]/text()').get('').strip()} - {row.xpath('./td[6]/text()').get('').strip()}"
            if not course_name.strip():
                continue

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_url,
                "Course Name": course_name,
                "Course Description": "",
                "Class Number": row.xpath("./td[5]/text()").get("").strip(),
                "Section": row.xpath("./td[4]/text()").get("").strip(),
                "Instructor": row.xpath("./td[14]/text()").get("").strip(),
                "Enrollment": f"{row.xpath('./td[9]/text()').get('').strip()}/{row.xpath('./td[10]/text()').get('').strip()}",
                "Course Dates": date_result,
                "Location": row.xpath("./td[13]/text()").get("").strip(),
                "Textbook/Course Materials": "",
            })

        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY LIST PAGES
    def parse_directory(self, response):
        """
        Crawl directory pagination pages
        """

        # Page 1
        yield from self.parse_directory_page(response)

        # Remaining pages
        for page in range(2, 131):
            url = f"https://www.fvsu.edu/directory/pg/{page}"
            yield scrapy.Request(url, callback=self.parse_directory_page)

    def parse_directory_page(self, response):
        """
        Extract profile links from listing page
        """

        links = response.xpath('//div[@class="border-top-yellow py-4"]//a/@href').getall()

        for link in links:
            if "/directory/profile/" not in link:
                continue

            yield response.follow(link, callback=self.directory_details)

    # DIRECTORY PROFILE PAGE
    def directory_details(self, response):
        """
        Extract staff profile details
        """

        name = response.xpath('//h2[@class="h3 mb-2"]/text()').get(default="").strip()

        texts = [
            t.strip()
            for t in response.xpath(
                '//p[@class="h6 mb-2"]//text() | //p[@class="text-concourse mb-4 text-large"]//text()'
            ).getall()
            if t.strip()
        ]

        title = " | ".join(texts)

        # Phone
        phone = ""
        raw_texts = response.xpath('//div[@class="row mb-4"]//li//text()').getall()
        for txt in raw_texts:
            digits = re.sub(r"\D", "", txt)
            if len(digits) >= 10:
                phone = digits
                break

        # Email
        email = response.xpath('//a[starts-with(@href,"mailto:")]/@href').re_first(r"mailto:(.*)")

        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

    # CALENDAR PAGE
    def parse_calendar(self, response):
        links = response.xpath('//div[@class="tab-pane fade cms-tab-content-1 active show"]//p/a/@href').getall()
        for link in links:
            file_url = response.urljoin(link)
            lower_url = file_url.lower()

            if lower_url.endswith(".pdf"):
                yield scrapy.Request(url=file_url,callback=self.parse_calendar_pdf)

            elif lower_url.endswith(".xlsx"):
                yield scrapy.Request(url=file_url,callback=self.parse_calendar_xlsx)

    def parse_calendar_pdf(self, response):
        source_url = response.url

        content_type = response.headers.get(
            "Content-Type", b""
        ).decode().lower()

        if "pdf" not in content_type or not response.body:
            return

        try:
            reader = PyPDF2.PdfReader(BytesIO(response.body))
        except Exception:
            return

        full_text = ""
        for page in reader.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"

        term_match = re.search(
            r'(Spring|Summer|Fall|Winter)\s+20\d{2}',
            full_text,
            re.IGNORECASE
        )
        term = term_match.group(0).title() if term_match else None

        date_pattern = re.compile(
                r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+(\d{1,2}(?:\s*&\s*\d{1,2})?)\s+(.*)',
                re.IGNORECASE
            )

        for line in full_text.split("\n"):
            line = line.strip()
            match = date_pattern.match(line)
            if not match:
                continue

            month, day, description = match.groups()

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": source_url,
                "Term Name": term,
                "Term Date": f"{month} {day}",
                "Term Date Description": description.strip(),
            })

    # ---------- XLSX PARSER ----------
    def parse_calendar_xlsx(self, response):
        source_url = response.url

        df = pd.read_excel(
            BytesIO(response.body),
            engine="openpyxl",
            header=None
        )

        df = df.fillna("").astype(str)

        full_text = " ".join(df.values.flatten())
        term_match = re.search(
            r'(Spring|Summer|Fall|Winter)\s+20\d{2}',
            full_text,
            re.IGNORECASE
        )
        term = term_match.group(0).title() if term_match else None

        month_names = {
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        }

        current_month = None
        date_pattern = re.compile(
            r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)'
        )

        for _, row in df.iterrows():
            cells = [c.strip() for c in row if c.strip()]
            if not cells:
                continue

            row_text = " ".join(cells)

            if row_text in month_names:
                current_month = row_text
                continue

            if "accredited" in row_text.lower():
                break

            if date_pattern.match(cells[0]):
                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": source_url,
                    "Term Name": term,
                    "Term Date": cells[0],
                    "Term Date Description": cells[1] if len(cells) > 1 else "",
                })

    # SAVE DATA ON SPIDER CLOSE
    def closed(self, reason):
        """
        Save directory & calendar data when spider finishes
        """

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")

        self.logger.info("Spider finished successfully")
