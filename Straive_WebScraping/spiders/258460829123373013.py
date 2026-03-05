import re
import scrapy
import pandas as pd
from ..utils import *
import pdfplumber
import io
from inline_requests import inline_requests
from lxml import html
from urllib.parse import urlencode


class MohawkSpider(scrapy.Spider):
    name = "mohawk"
    institution_id = 258460829123373013

    # Course catalog base URL and API endpoints
    course_url = "https://www2.mvcc.edu/courses/"
    course_api_url = "https://www2.mvcc.edu/courses/index.cfm"
    course_detail_api_url = "https://www2.mvcc.edu/courses/ajax/coursedetail.cfm"
    
    # Employee directory API endpoint
    directory_api_url = "https://mvcc.edu/directory/"
    
     # Academic calendar page URL
    calendar_url = "https://www.mvcc.edu/records-registration/academic-calendar.php" 


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

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
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        rows = []
        # Term codes to scrape
        TERM_CODES = ["202601", "202605", "202508"]
        # Pagination size for grid API
        PAGE_SIZE = 50
        # DataTables echo counter
        s_echo = 1
        # Headers required for AJAX grid requests
        ajax_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
        # Iterate through each academic term
        for term_code in TERM_CODES:
            # Initialize pagination offset
            offset = 0

            # Build mandatory initialization URL
            init_url = self.course_url + "?" + urlencode({
                "termcode": term_code,
                "campus": "",
                "subject": "",
                "monday": "1",
                "tuesday": "1",
                "wednesday": "1",
                "thursday": "1",
                "friday": "1",
                "saturday": "1",
                "starttime1": "",
                "starttime2": "",
                "instructor": "",
                "open": "",
                "partterm": "",
                "imethod": "",
                "action": "results",
            })
            # Send initialization request to set session cookies
            yield scrapy.Request(
                url=init_url,
                headers={"User-Agent": ajax_headers["User-Agent"]},
                meta={"cookiejar": term_code},
                dont_filter=True,
            )

            # Paginate through course grid results
            while True:
                # Build grid API URL
                grid_url = (
                    f"{self.course_api_url}"
                    f"?action=getGrid"
                    f"&termcode={term_code}"
                    f"&campus=&subject="
                    f"&monday=1&tuesday=1&wednesday=1&thursday=1&friday=1&saturday=1"
                    f"&starttime1=&starttime2=&instructor=&open=&partterm=&imethod="
                )
                # Request paginated grid data
                grid_response = yield scrapy.FormRequest(
                    url=grid_url,
                    method="POST",
                    headers=ajax_headers,
                    formdata={
                        "sEcho": str(s_echo),
                        "iColumns": "14",
                        "sColumns": "",
                        "iDisplayStart": str(offset),
                        "iDisplayLength": str(PAGE_SIZE),
                        "sSearch": "",
                        "bEscapeRegex": "true",
                    },
                    meta={"cookiejar": term_code},
                    dont_filter=True,
                )

                # Increment DataTables echo value
                s_echo += 1
                # Parse JSON grid response
                data = grid_response.json()
                grid_rows = data.get("aaData", [])
                total = int(data.get("iTotalDisplayRecords", 0))
                if not grid_rows:
                    break

                # Process each course row in the grid
                for row in grid_rows:

                    if len(row) < 13:
                        continue
                    
                    # Extract course code and section ID
                    link = html.fromstring(row[0])
                    course_code = link.text_content().strip()
                    section_id = link.attrib.get("id", "").strip()

                    crn = row[1].strip()
                    course_name = html.fromstring(row[3]).text_content().strip()
                    full_course_name = f"{course_code} {course_name}"

                    enrollment_raw = row[8].strip()
                    course_dates = row[9].strip()
                    instructor = row[10].strip()
                    campus = row[11].strip()

                    
                    # Request detailed course information
                    detail_response = yield scrapy.FormRequest(
                        url=self.course_detail_api_url,
                        method="POST",
                        formdata={
                            "term": term_code,
                            "course": course_code,
                            "section": section_id,
                        },
                        meta={"cookiejar": term_code},
                        dont_filter=True,
                    )
                    # Parse course detail HTML
                    tree = html.fromstring(detail_response.text)
                    # Extract course description
                    desc_nodes = tree.xpath(
                        "//td[normalize-space()='Description']/following-sibling::td//text()"
                    )
                    description = " ".join(
                        t.strip() for t in desc_nodes if t.strip()
                    )
                    # Normalize enrollment 
                    enrollment_raw = enrollment_raw.strip()
                    enrollment = enrollment_raw if enrollment_raw else ""

                    # Extract textbook information
                    textbook_url = ""
                    onclick = tree.xpath(
                        "//td[normalize-space()='Text Book Information']"
                        "/following-sibling::td//a/@onclick"
                    )
                    # Parse textbook URL from onclick JavaScript
                    if onclick:
                        args = (
                            onclick[0]
                            .split("(")[1]
                            .split(")")[0]
                            .replace("'", "")
                            .split(",")
                        )

                        subject = args[1].strip()
                        course_full = args[2].strip()
                        section_no = args[3].strip()
                        course_number = "".join(c for c in course_full if c.isdigit())

                        textbook_url = (
                            "https://mvcc.bncollege.com/course-material/course-finder"
                            f"?campusId=mvcc"
                            f"&subject={subject}"
                            f"&course={course_number}"
                            f"&section={section_no}"
                        )

                    rows.append({
                        "Cengage Master Institution ID": int(self.institution_id),
                        "Source URL": self.course_url,
                        "Course Name": full_course_name,
                        "Course Description": description,
                        "Class Number": crn,
                        "Section": section_id,
                        "Instructor": instructor,
                        "Enrollment": enrollment,
                        "Course Dates": course_dates,
                        "Location": campus,
                        "Textbook/Course Materials": textbook_url,
                    })

                # Stop pagination when all records are processed
                if offset + PAGE_SIZE >= total:
                    break

                offset += PAGE_SIZE

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "course")


    def parse_directory(self, response):
        """
        Parse faculty & staff directory.

        Columns:
        - Cengage Master Institution ID
        - Source URL
        - Name
        - Title (Title + Department)
        - Email
        - Phone Number
        """

        rows = []

        # Loop through each directory table row
        for tr in response.xpath('//table[@id="profiles"]/tbody/tr'):
            name = tr.xpath('./td[1]//text()').getall()
            name = " ".join(t.strip() for t in name if t.strip())

            raw_title = tr.xpath('./td[2]//text()').getall()
            title = " ".join(t.strip() for t in raw_title if t.strip())

            raw_department = tr.xpath('./td[3]//text()').getall()
            department = " ".join(t.strip() for t in raw_department if t.strip())

            # Merge title + department
            if department:
                title = f"{title}, {department}"
            
            # Extract and clean phone number
            phone = tr.xpath('./td[4]//text()').get()
            phone = phone.strip() if phone else ""
            
            # Extract email from mailto link
            email = tr.xpath('./td[6]/a/@href').get()
            if email and email.startswith("mailto:"):
                email = email.replace("mailto:", "").strip()
            else:
                email = ""

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })

        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")

 
    @inline_requests
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
        rows = []

        # Matches all supported month names in short and long form
        MONTHS = (
            r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|'
            r'Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|'
            r'Nov(?:ember)?|Dec(?:ember)?)'
        )

        # Matches a date with optional range and optional description on the same line
        DATE_RE = re.compile(
            rf'^\s*(?P<date>'
            rf'{MONTHS}\s+\d{{1,2}}(?:\s*[-–—]\s*\d{{1,2}})?'
            rf'(?:\s*[-–—]\s*{MONTHS}\s*\d{{1,2}}(?:,\s*\d{{4}})?)?'
            rf'(?:,\s*\d{{4}})?'
            rf')\s*(?P<desc>.*)?$',
            re.IGNORECASE
        )

        # Used to detect whether a line begins with a month
        MONTH_START_RE = re.compile(rf'^\s*{MONTHS}\b', re.IGNORECASE)
        # Detects term headers
        TERM_HEADER_RE = re.compile(r'^(FALL|SPRING|SUMMER)\s+(\d{4})', re.IGNORECASE)
        # Detects subheaders that should not produce data rows
        TERM_SUBHEADER_RE = re.compile(r'^\s*Term\s*\d+\b.*\d{4}', re.IGNORECASE)

        # Normalizes month-to-month ranges like "Dec - Jan"
        MONTH_HYPHEN_RE = re.compile(
            rf'(?i)\b({MONTHS})\b\s*-\s*\b({MONTHS})\b'
        )

        # Collect academic calendar PDF links
        calendar_section = response.xpath('//div[@class="interior-content"]')
        pdf_links = calendar_section.xpath(
            './/h2[normalize-space()="Current academic calendars"]/following-sibling::ul[1]/li/a | '
            './/h3[normalize-space()="Future calendars"]/following-sibling::ul[1]/li/a'
        )

        pdf_urls = [
            response.urljoin(a.xpath('@href').get())
            for a in pdf_links
            if "Academic Year" in (a.xpath('normalize-space(text())').get() or "")
        ]

        # Process each academic calendar PDF
        for pdf_url in pdf_urls:
            self.logger.info(f"Processing PDF: {pdf_url}")
            pdf_response = yield scrapy.Request(pdf_url, dont_filter=True)
            
            # Extract all visible text lines from all pages
            with pdfplumber.open(io.BytesIO(pdf_response.body)) as pdf:
                lines = [
                    ln.strip()
                    for page in pdf.pages
                    for ln in (page.extract_text() or "").splitlines()
                    if ln.strip()
                ]

            current_term = None
            pending_date = None
            pending_desc = None
            
            # Iterate line-by-line to preserve ordering and context
            i = 0
            while i < len(lines):
                raw = lines[i]
                ln = raw.replace('–', '-').replace('—', '-').strip()

                # Ignore internal subheaders that do not represent events
                if TERM_SUBHEADER_RE.match(ln):
                    pending_date = pending_desc = None
                    i += 1
                    continue

                # Detect term header
                th = TERM_HEADER_RE.match(ln)
                if th:
                    current_term = f"{th.group(1).title()} {th.group(2)}"
                    pending_date = pending_desc = None
                    i += 1
                    continue
                
                # Skip irrelevant lines
                if not current_term or ln.lower().startswith("last updated") or ln.startswith("*"):
                    i += 1
                    continue

                # Attach description to a pending date
                if pending_date and not MONTH_START_RE.match(ln):
                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": pdf_url,
                        "Term Name": current_term,
                        "Term Date": MONTH_HYPHEN_RE.sub(r'\1 – \2', pending_date),
                        "Term Date Description": ln.lstrip("- ").strip()
                    })
                    pending_date = None
                    i += 1
                    continue

                # If a description appears before a date, store it temporarily
                if not MONTH_START_RE.match(ln):
                    next_ln = lines[i + 1] if i + 1 < len(lines) else ""
                    if next_ln and MONTH_START_RE.match(next_ln.replace('–', '-')):
                        pending_desc = ln
                        i += 1
                        continue

                # Parse date line
                m = DATE_RE.match(ln)
                if m:
                    date_part = m.group("date").strip()
                    desc_part = (m.group("desc") or "").strip()

                    date_part = MONTH_HYPHEN_RE.sub(r'\1 – \2', date_part)

                    # Description was on previous line
                    if pending_desc and not desc_part:
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Term Name": current_term,
                            "Term Date": date_part,
                            "Term Date Description": pending_desc
                        })
                        pending_desc = None
                        i += 1
                        continue

                    # Date + description on same line
                    if desc_part:
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Term Name": current_term,
                            "Term Date": date_part,
                            "Term Date Description": desc_part.lstrip("- ").strip()
                        })
                    else:
                        pending_date = date_part

                    i += 1
                    continue

                i += 1

            # Save any leftover date
            if pending_date:
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": pdf_url,
                    "Term Name": current_term or "",
                    "Term Date": MONTH_HYPHEN_RE.sub(r'\1 – \2', pending_date),
                    "Term Date Description": ""
                })
        
        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "calendar")
