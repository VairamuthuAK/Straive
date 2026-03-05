import re
import io
import scrapy
import base64
import pdfplumber
from ..utils import *
from inline_requests import inline_requests

"""
Ordered list of months used to preserve chronological order
when parsing academic calendar PDFs.
"""

MONTH_ORDER = [
    "JANUARY", "FEBRUARY", "MARCH",
    "APRIL", "MAY", "JUNE",
    "JULY", "AUGUST", "SEPTEMBER",
    "OCTOBER", "NOVEMBER", "DECEMBER"
]

"""
Standard weekday headers used in calendar tables.
"""


WEEKDAYS = ["Su", "M", "T", "W", "Th", "F", "S"]

"""
Mapping of calendar symbols to their textual meanings.

Each key represents a symbol found in the calendar PDF.
Values may include:
- text    : Human-readable description
- details : Optional session-specific date ranges
"""

KEY_MAPPING = {
    "IS": {"text" : "In-Service (College is closed, but Faculty and Staff Attend In-Service)"},
    "C_COMMENCEMENT": {"text": "Commencement Ceremony"},
    "sA/B": {"text": "Semester Starts, Summer Session A (5 weeks): 6/1/26 – 7/2/26, Semester Ends, Summer Session B KEY (8 weeks): 6/1/26 – 7/24/26"},
    "/B":{"text": "Semester Ends, Summer Session B", "details": {"8_weeks": "6/1/26 – 7/24/26"}},
    "n": {"text": "Non College Day"},
    "/": {"text": "Semester Ends"},
    "u": {"text": "Faculty Work Day"},
    "G": {"text": "Graduation"},
    "WC": {"text": "Workshop Days College Closed"},
    "/A":{"text": "Semester Ends, Summer Session A", "details": {"5_weeks": "6/1/26 – 7/2/26"}},
    "A": {"text": "Summer Session A", "details": {"5_weeks": "6/1/26 – 7/2/26"}},
    "s": {"text": "Semester Starts"},
    "B": {"text": "Semester Starts"},
    "C": {"text": "College Closed"},
    "W": {"text": "Workshop Days"},
}


def extract_day(cell):

    """
    Extract numeric day value from a calendar cell.

    Args:
        cell (str): Raw cell text from calendar table

    Returns:
        int | None: Day number if present, else None
    """
    
    digits = "".join(ch for ch in cell if ch.isdigit())
    return int(digits) if digits else None

def is_footer_row(row):

    """
    Detect footer rows inside calendar tables.

    Args:
        row (list)

    Returns:
        bool
    """

    footer_words = {"Faculty", "Student", "TOTAL", "GRAND", "CALENDAR", "SUBJECT", "CHANGE"}
    return any(word in cell for cell in row for word in footer_words)

MONTH_RE = re.compile(r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d{4}")

# Map months to semesters
SEMESTER_MAP = {
    "JULY": "Fall Semester",
    "AUGUST": "Fall Semester",
    "SEPTEMBER": "Fall Semester",
    "OCTOBER": "Fall Semester",
    "NOVEMBER": "Fall Semester",
    "DECEMBER": "Fall Semester",
    "JANUARY": "Spring Semester",
    "FEBRUARY": "Spring Semester",
    "MARCH": "Spring Semester",
    "APRIL": "Spring Semester",
    "MAY": "Summer Session",
    "JUNE": "Summer Session",
    "JULY": "Summer Session",
}

def split_by_semester(lines):
    """
    Splits PDF lines into Fall, Spring, Summer sections
    using month order:
      - First 2 months → Fall
      - Middle months → Spring
      - Last month → Summer
    """

    sections = {
        "Fall Semester": [],
        "Spring Semester": [],
        "Summer Session": []
    }

    # ───────── PASS 1: detect months ─────────
    months = []
    for line in lines:
        m = MONTH_RE.search(line)
        if m:
            months.append(m.group(0))

    if not months:
        return sections

    total_months = len(months)

    # ───────── PASS 2: map month → semester ─────────
    month_to_semester = {}
    for idx, month in enumerate(months, start=1):
        if idx == total_months:
            month_to_semester[month] = "Summer Session"
        elif idx <= 2:
            month_to_semester[month] = "Fall Semester"
        else:
            month_to_semester[month] = "Spring Semester"

    # ───────── PASS 3: assign lines ─────────
    current_semester = None

    for line in lines:
        line = line.strip()
        if not line:
            continue

        m = MONTH_RE.search(line)
        if m:
            current_semester = month_to_semester[m.group(0)]
            sections[current_semester].append(line)
            continue

        if not current_semester:
            continue

        # skip key / footer junk
        if any(word in line.upper() for word in [
            "KEY",
            "CALENDAR SUBJECT TO CHANGE"
        ]):
            continue

        sections[current_semester].append(line)

    return sections



def normalize_row(row):

    """
    Normalize a PDF text row into column tokens.

    Args:
        row (str): Raw text line from PDF

    Returns:
        list: Tokenized cells
    """

    # Split by 2+ spaces (common in PDFs to separate columns)
    tokens = re.split(r'\s{2,}', row.strip())
    # fallback: if split fails (single spaces), split by any space
    if len(tokens) < 3:
        tokens = row.strip().split()
    return tokens

def build_month_rows(block_lines, col_index, num_months):
    """
    Build month rows for a specific column (month) from a block of lines containing multiple months side by side.
    """
    month_rows = []
    collecting = False

    for line in block_lines:
        tokens = normalize_row(line)
        if not tokens:
            continue

        # start new month when weekday header appears
        if tokens[:7] == WEEKDAYS:
            month_rows.append(WEEKDAYS)
            collecting = True
            continue

        if collecting:
            # stop at footer rows
            if is_footer_row(tokens):
                collecting = False
                continue

            # pad tokens so we can safely slice
            while len(tokens) < num_months * 7:
                tokens.append("")

            start = col_index * 7
            end = start + 7
            cells = tokens[start:end]
            
            # ignore empty rows
            if any(cells):
                month_rows.append(cells)

    return month_rows




def split_by_month(semester, lines):

    """
    Split semester lines into individual month blocks.

    Args:
        semester (str)
        lines (list)

    Returns:
        list[dict]
    """

    results = []
    i = 0

    while i < len(lines):
        line = lines[i]
        matches = list(MONTH_RE.finditer(line))
        if not matches:
            i += 1
            continue
        
        month_labels = [m.group() for m in matches]
        num_months = len(month_labels)

        block_rows = []
        i += 1
        while i < len(lines) and not MONTH_RE.search(lines[i]):
            block_rows.append(lines[i])
            i += 1

        for col_index, month in enumerate(month_labels):
            
            month_rows = build_month_rows(block_rows, col_index, num_months)
            
            results.append({
                "Calendar": semester,
                "Month": month,
                "ColumnIndex": col_index,
                "MonthRows": month_rows
            })

    return results

def extract_month_column(raw_rows, column_index):
    # extract a single month's column from MonthRows
    month_rows = []
    start = column_index * 7
    end = start + 7
    for row in raw_rows:
        if len(row) < end:
            row = row + [""] * (end - len(row))
        month_rows.append(row[start:end])
    return month_rows



class SouthCentralSpider(scrapy.Spider):
    name="south"
    institution_id = 258423416426620880

    # course_url 
    course_url = "https://www.ed2go.com/southcentral/SearchResults.aspx?Sort=Name&MaxResultCount=819"
    course_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'referer': 'https://www.ed2go.com/southcentral/SearchResults.aspx?Sort=POPULAR',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    }

    # directory_url
    directory_url= "https://southcentral.edu/about-us/directory-list?letter=&category=&tag=&filter-search=&filter-match=any&filter_order=name&filter_order_Dir=ASC&limit=234&start=1"

    # calendar_url
    calendar_url = "https://southcentral.edu/calendars/academic-calendar"


    def start_requests(self):

        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,headers=self.course_headers,callback=self.parse_course)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url,headers=self.course_headers,callback=self.parse_course)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url,headers=self.course_headers,callback=self.parse_course)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)
        
        else:
            yield scrapy.Request(url=self.course_url,headers=self.course_headers,callback=self.parse_course)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar) 


    @inline_requests
    def parse_course(self,response):

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
        
        product_urls =[]
        if re.search(r'\.catch\(console\.error\)\;\"\s*href\=\"(.*?)\"\>',response.text):
            product_urls = re.findall(r"\.catch\(console\.error\)\;\"\s*href\=\"(.*?)\"\>",response.text)

        rows=[]
        for product_url in product_urls:
            headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'referer':product_url,
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            }

            req = scrapy.Request(
            url=product_url,
            headers=headers,
            dont_filter=True,
            meta={
                'dont_redirect': True,
                'handle_httpstatus_list': [200, 301, 302, 403],
            }
            )

            resp = yield req   # ✅ inline response

            # 🚫 SKIP REDIRECTED COURSES
            if resp.status == 302:
                self.logger.info(f"Skipping redirected URL: {product_url}")
                continue

            # 🚫 Optional: skip forbidden
            if resp.status == 403:
                self.logger.warning(f"Blocked URL: {product_url}")
                continue


            
            class_number = resp.xpath('//div[@class="e2g-hosted-details-course-code"]/text()').get("").split(":")[-1].strip()
            course_title = resp.xpath('//h1/text()').get(default="").strip()
            parts = [class_number, course_title]


            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": resp.url,
                "Course Name": " ".join(p for p in parts if p),
                "Course Description": resp.xpath('//div[@id="divShortDesc"]/p/text()').get(""),
                "Class Number": class_number,
                "Section": "",
                "Instructor": resp.xpath('//div[@class="instructor-bio"]//p/strong/text()').get("").strip(),
                "Enrollment": "",
                "Course Dates": "",
                "Location": "",
                "Textbook/Course Materials": "",
                })
            
        if rows:
            course_df = pd.DataFrame(rows)
            save_df(course_df, self.institution_id, "course")
            

    @inline_requests
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

        listing_blocks = response.xpath('//table[@class="table table-striped"]//tr[contains(@class,"person")]')
        rows=[]
        for listing_block in listing_blocks:
            url = listing_block.xpath('.//td[@class="col_name"]//a/@href').get("").strip()
            email = listing_block.xpath('.//joomla-hidden-mail/@text').get("").strip()
            if email:
                email = email = base64.b64decode(email).decode().strip()
            url =f"https://southcentral.edu{url}"
            profile_response = yield scrapy.Request(url=url)
            rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": profile_response.url,
                    "Name": profile_response.xpath('//div[@class="personinfo"]/div/span[@title="Name"]/text()').get("").strip(),
                    "Title": profile_response.xpath('//div[@class="personinfo"]/div/span[@title="Position"]/text()').get("").strip(),
                    "Email": email,
                    "Phone Number": profile_response.xpath('//div[@class="personinfo"]/div/span[@title="Phone"]/text()').get("").strip(),
                })
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")

    @inline_requests
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
        
        pdf_links = ["https://southcentral.edu/images/academic_calendar/Academic%20Calendar%202025-2026.pdf","https://southcentral.edu/images/academic_calendar/Future%20Calendars/SCC_Calendar_2026-2027.pdf",
                     "https://southcentral.edu/images/academic_calendar/Future%20Calendars/SCC_Calendar_2027-2028.pdf","https://southcentral.edu/images/academic_calendar/Future%20Calendars/SCC_Calendar_2028-2029.pdf",
                     "https://southcentral.edu/images/academic_calendar/Future%20Calendars/SCC_Calendar_2029-2030.pdf","https://southcentral.edu/images/academic_calendar/Future%20Calendars/SCC_Calendar_2030-2031.pdf"]
        
        events = []
        for pdf_link in pdf_links:

            response = yield scrapy.Request(url=pdf_link)

            with pdfplumber.open(io.BytesIO(response.body)) as pdf:
                for page in pdf.pages:
                    width = page.width
                    height = page.height
                    
                    # define column boundaries (left, center, right)
                    cols = [
                        (0, 0, width/3, height),          # left
                        (width/3, 0, 2*width/3, height),  # center
                        (2*width/3, 0, width, height)     # right
                    ]

                    for col_idx, bbox in enumerate(cols):
                    # crop the page to the column
                        cropped = page.within_bbox(bbox)
                        text = cropped.extract_text()

                        lines = [l.strip() for l in text.split("\n") if l.strip()]
                        sections = split_by_semester(lines)

                        final_output = []
                        for semester, content in sections.items():

                            final_output.extend(split_by_month(semester, content))

                        for item in final_output:
                            month_label = item["Month"]
                            month_rows = item["MonthRows"]

                            previous_date = None
                            previous_symbol = None 

                            for month_row in month_rows[1:]:  # skip weekday header
                                if is_footer_row(month_row):
                                    continue

                                # 1️⃣ first date in row
                                first_date = next(
                                    (extract_day(c) for c in month_row if extract_day(c)),
                                    None
                                )

                                # 2️⃣ leading symbols (n n C before first date)
                                leading_symbols = []
                                for c in month_row:
                                    if extract_day(c):
                                        break
                                    if c in KEY_MAPPING:
                                        leading_symbols.append(c)

                                # 3️⃣ assign leading symbols
                                if first_date:
                                    start_date = first_date - len(leading_symbols)
                                    for sym in leading_symbols:
                                        if start_date <= 0:
                                            continue

                                        mapping = KEY_MAPPING.get(sym)
                                        if not mapping:
                                            start_date += 1
                                            continue

                                        details = mapping.get("details", "")
                                        details_text = (
                                            " ".join(f"{k}: {v}" for k, v in details.items())
                                            if isinstance(details, dict)
                                            else ""
                                        )
                                        month_label = month_label.split(" ")[0]
                                        events.append({
                                            "Cengage Master Institution ID": self.institution_id,
                                            "Source URL": response.url,
                                            "Term Name": item["Calendar"],
                                            "Term Date": f"{month_label} {start_date}",
                                            "Term Date Description": f"{mapping['text']} {details_text}".strip(),
                                        })

                                        start_date += 1

                                # 4️⃣ process row normally
                                
                                for cell in month_row:
                                    day = extract_day(cell)
                                    
                                    if day is not None:
                                        previous_date = day
                                        continue

                                    if cell in KEY_MAPPING and previous_date is not None:
                                        if cell == "C" and previous_symbol == "/":
                                            mapping = KEY_MAPPING["C_COMMENCEMENT"]
                                        else:
                                            mapping = KEY_MAPPING[cell]

                                        details = mapping.get("details", "")
                                        details_text = (
                                            " ".join(f"{k}: {v}" for k, v in details.items())
                                            if isinstance(details, dict)
                                            else ""
                                        )

                                        month_label = month_label.split(" ")[0]
                                        events.append({
                                            "Cengage Master Institution ID": self.institution_id,
                                            "Source URL": response.url,
                                            "Term Name": item["Calendar"],
                                            "Term Date": f"{month_label} {previous_date + 1}",
                                            "Term Date Description": f"{mapping['text']} {details_text}".strip(),
                                        })
                                        previous_symbol = None
                                        previous_date += 1

                                    if cell == "/":
                                        previous_symbol = "/"

            if events:
                df = pd.DataFrame(events)
                save_df(df, self.institution_id, "calendar")

