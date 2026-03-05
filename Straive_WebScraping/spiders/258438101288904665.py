
import re
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from parsel import Selector


def parse_directory_clean_text(text):
    """
    Normalize directory field text.

    - Collapses multiple whitespaces into a single space
    - Removes leading/trailing whitespace
    - Safely handles None values

    Args:
        text (str | None): Raw extracted text

    Returns:
        str: Cleaned string
    """
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

class RichmondSpider(scrapy.Spider):
    name = "richmond"

    institution_id = 258438101288904665

    # In-memory storage for scraped records
    calendar_rows = []
    directory_rows = []
    course_rows = []

    #Course schedule PDFs URLs
    course_urls = ["https://richmondcc.edu/sites/default/files/public/PDFs/class_schedule_2026sp_all.pdf","https://richmondcc.edu/sites/default/files/public/PDFs/class_schedule_2026su_all.pdf"]
    
    # Directory AJAX endpoint
    directory_url = "https://richmondcc.edu/views/ajax"

    # POST payload template for paginated directory requests
    directory_payload_template = (
    "view_name=directory&view_display_id=block_1&view_args=&view_path=node%2F67"
    "&view_base_path=contact-us%2Fdirectory"
    "&view_dom_id=0472aa8c1234e4eefc5ae15db26db795"
    "&pager_element=0&page={page}"
    "&ajax_html_ids%5B%5D=async-buttons"
    "&ajax_page_state%5Btheme%5D=richmond"
        )

    # Required headers for AJAX directory requests
    directory_headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://richmondcc.edu',
        'referer': 'https://richmondcc.edu/contact-us/directory',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }
    
    # Academic calendar PDFs URLS
    calendar_urls = [
        "https://richmondcc.edu/sites/default/files/public/PDFs/2026_spring_academic_calendar_9-17-25.pdf",
        "https://richmondcc.edu/sites/default/files/public/PDFs/2026_summer_academic_calendar_9-23-25.pdf",
        "https://richmondcc.edu/sites/default/files/public/PDFs/2026_fall_academic_calendar_1-14-26.pdf",
        "https://richmondcc.edu/sites/default/files/public/PDFs/2027_spring_academic_calendar_9-16-25.pdf",
        "https://richmondcc.edu/sites/default/files/public/PDFs/2027_summer_academic_calendar_9-17-25.pdf",
        "https://richmondcc.edu/sites/default/files/public/PDFs/2025_fall_academic_calendar_10-27-25.pdf",
        "https://richmondcc.edu/sites/default/files/public/PDFs/summer_2025_academic_calendar_10-17-24.pdf"
    ]

    
     # Spider Entry Point
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

       # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
           # Pages 1–14 → page=0–13
            for page in range(0, 14):
                payload = self.directory_payload_template.format(page=page)
                yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,body=payload,method="POST", callback=self.parse_directory, dont_filter=True)
        
        elif mode == 'calendar':
            for calendar_url in self.calendar_urls:
                yield scrapy.Request(url=calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)

            for page in range(0, 14):
                payload = self.directory_payload_template.format(page=page)
                yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,body=payload,method="POST", callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)
            
            for calendar_url in self.calendar_urls:
                yield scrapy.Request(url=calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            for calendar_url in self.calendar_urls:
                yield scrapy.Request(url=calendar_url, callback=self.parse_calendar)
            
            for page in range(0, 14):
                payload = self.directory_payload_template.format(page=page)
                yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,body=payload,method="POST", callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)
            
            for page in range(0, 14):
                payload = self.directory_payload_template.format(page=page)
                yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,body=payload,method="POST", callback=self.parse_directory, dont_filter=True)

            for calendar_url in self.calendar_urls:
                yield scrapy.Request(url=calendar_url, callback=self.parse_calendar)
                
    
    def parse_course(self, response):

        def clean(text):
            """Normalize cell text."""
            if not text:
                return ""
            return re.sub(r"\s+", " ", text.replace("\n", " ")).strip()

        def fix_course_date(date_text):
            """
            Normalize course date ranges.

            - Single date → returned as-is
            - Multiple dates → first and last date joined
            """
            if not date_text:
                return ""
            dates = re.findall(r"\d{1,2}/\d{1,2}/\d{2}", date_text)
            if not dates:
                return date_text.strip()
            if len(dates) == 1:
                return dates[0]
            return f"{dates[0]} - {dates[-1]}"

        pdf_bytes = response.body 
        pdf_url = response.url    

        # Open PDF in-memory
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for table in page.extract_tables():
                    
                    # Skip empty or malformed tables
                    if not table or len(table) < 2:
                        continue

                    headers = [clean(h).lower() for h in table[0]]

                    def find_col(*keywords):
                        """Locate column index by keyword match."""
                        for i, h in enumerate(headers):
                            if any(k in h for k in keywords):
                                return i
                        return None
                    
                    # Identify column indices
                    idx_syn = find_col("synon", "synonym")
                    idx_loc = find_col("location")
                    idx_sec = find_col("section")
                    idx_title = find_col("title")
                    idx_fac = find_col("faculty", "instructor")
                    idx_cap = find_col("cap")
                    idx_seats = find_col("seat")
                    idx_start = find_col("start")
                    idx_end = find_col("end")

                    # Required fields
                    if idx_syn is None or idx_title is None:
                        continue

                    for row in table[1:]:
                        if len(row) < len(headers):
                            continue

                        synonym = clean(row[idx_syn])
                        title = clean(row[idx_title])

                        if not synonym or not title:
                            continue

                        section = clean(row[idx_sec]) if idx_sec is not None else ""
                        section1 = section.split('-')[-1].strip()
                        section2 = '-'.join(section.split('-')[:2])

                        self.logger.info(f"Added course: {synonym} | {title}")

                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Course Name": f"{section2} {title}".strip(),
                            "Course Description": "",
                            "Class Number": synonym,
                            "Section": section1,
                            "Instructor": clean(row[idx_fac]) if idx_fac is not None else "",
                            "Enrollment": "",
                            "Course Dates": fix_course_date(
                                f"{clean(row[idx_start])} - {clean(row[idx_end])}"
                            ),
                            "Location": clean(row[idx_loc]) if idx_loc is not None else "",
                            "Textbook/Course Materials": ""
                        })
            
    def parse_directory(self,response):
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
        
        json_datas = response.json()
        json_data = json_datas[2]['data']
        response1 = Selector(text=json_data)
        blocks = response1.xpath('//div[@class="faculty-name"]')
        for block in blocks:
            product_link = block.xpath('./a/@href').get('')
            product_link = response.urljoin(product_link)

            # Fetch profile page
            res = requests.get(product_link)
            product_response = Selector(text=res.text)
            name = parse_directory_clean_text(product_response.xpath("//h1/text()").get(''))
            title = parse_directory_clean_text(product_response.xpath('//div[contains(text(),"Position")]/following::div[1]/div/text()').get(''))
            phone = parse_directory_clean_text(product_response.xpath('//div[contains(text(),"Phone")]/following::div[1]/div/text()').get(''))
            email = parse_directory_clean_text(product_response.xpath('//div[contains(text(),"Email")]/following::div[1]/div/a/text()|//div[contains(text(),"Email")]/following::div[1]/div/text()').get(''))
            
            self.directory_rows.append(
                    {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": product_link,
                    "Name": name.strip(),
                    "Title": title.strip(),
                    "Email": email.strip(),
                    "Phone Number": phone.strip(),
                    }
                )


    def parse_calendar(self, response):
        
        DATE_RE = re.compile(r"[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}")

        def clean(text):
            return re.sub(r"\s+", " ", text).strip() if text else ""
        
        def clean_session(text):
            """Remove date ranges from session header text."""
            return re.sub(r"\s*\d{1,2}/\d{1,2}/\d{2}\s*-\s*\d{1,2}/\d{1,2}/\d{2}", "", text).strip()
        
        pdf_url = response.url

        parts = pdf_url.split('/')[-1].split('_')
        term_name = f"{parts[1]} {parts[0]}"  # spring 2026, summer 2026, etc.

        response_pdf = requests.get(pdf_url)
        response_pdf.raise_for_status()

        with pdfplumber.open(BytesIO(response_pdf.content)) as pdf:

            # Page 1: Tabular calendar
            page1 = pdf.pages[0]
            tables = page1.extract_tables()

            for table in tables:
                if len(table) < 3:
                    continue

                header_row = None
                for row in table:
                    if row and any(cell and "Session" in cell for cell in row):
                        header_row = row
                        break

                if not header_row:
                    continue

                sessions = [
                    clean_session(clean(cell.replace("\n", " ")))
                    for cell in header_row[1:]
                    if cell and cell.strip()
                ]

                start_idx = table.index(header_row) + 1

                for row in table[start_idx:]:
                    if not row or not row[0]:
                        continue

                    event_desc = clean(row[0])

                    for i, date in enumerate(row[1:]):
                        if i >= len(sessions):
                            continue
                        if not date or not date.strip():
                            continue

                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Term Name": sessions[i],
                            "Term Date": clean(date),
                            "Term Date Description": event_desc,
                        })

            # Page 2: Narrative calendar
            if len(pdf.pages) > 1:
                page2 = pdf.pages[1]
                lines = [clean(l) for l in page2.extract_text().split("\n") if clean(l)]

                i = 0
                while i < len(lines):
                    line = lines[i]

                    # Date range spanning multiple lines
                    if DATE_RE.search(line) and line.endswith("-"):
                        start_date = DATE_RE.search(line).group()
                        desc = clean(lines[i + 1])
                        end_date = DATE_RE.search(lines[i + 2]).group()

                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Term Name": term_name,
                            "Term Date": f"{start_date} - {end_date}",
                            "Term Date Description": desc,
                        })

                        i += 3
                        continue
                    
                    # Single date entries
                    if DATE_RE.search(line):
                        date = DATE_RE.search(line).group()
                        description = clean(line.replace(date, ""))

                        if not description and i + 1 < len(lines):
                            description = clean(lines[i + 1])
                            i += 1

                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Term Name": term_name,
                            "Term Date": date,
                            "Term Date Description": description,
                        })

                    i += 1

    # Spider Shutdown Hook
    def closed(self, reason):
        """
        Persist scraped data when spider finishes.

        - Writes CSV debug files
        - Saves via shared save_df utility
        """
        
        self.logger.info("Spider closed")

        if self.course_rows:
            df_course = pd.DataFrame(self.course_rows)
            save_df(df_course, self.institution_id, "course")
            df_course.to_csv("DEBUG_course_output.csv", index=False)
            self.logger.info(f"Course rows written: {len(df_course)}")

        if self.directory_rows:
            df_dir = pd.DataFrame(self.directory_rows)
            save_df(df_dir, self.institution_id, "campus")
            self.logger.info(f"Directory rows written: {len(df_dir)}")

        if self.calendar_rows:
            df_cal = pd.DataFrame(self.calendar_rows)
            save_df(df_cal, self.institution_id, "calendar")
            self.logger.info(f"Calendar rows written: {len(df_cal)}")


