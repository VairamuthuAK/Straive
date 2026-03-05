
import re
import cv2
import time
import scrapy
import requests
import pytesseract
import numpy as np
import pandas as pd
from ..utils import *
from io import BytesIO
from ..utils import save_df
from PIL import Image, ImageEnhance
from pdf2image import convert_from_bytes
from playwright.sync_api import sync_playwright



# Tesseract & Poppler configuration (Windows paths)


# Path to Tesseract OCR executable
pytesseract.pytesseract.tesseract_cmd = (
    r"C:\Program Files\Tesseract-OCR\tesseract.exe"
)

# Path to Poppler binaries (required for pdf2image)
POPPLER_PATH = (
    r"C:\Users\GnanajeevanB\Downloads\Release-25.12.0-0"
    r"\poppler-25.12.0\Library\bin"
)


#Calendar parsing helpers

# Month names used to detect calendar sections in OCR text
MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

# Regex to identify month names (case-insensitive)
month_pattern = re.compile(rf"\b({'|'.join(MONTHS)})\b", re.I)

# Regex to extract dates like: 1, 12, 3-5, 10,12
date_pattern = re.compile(r"\b(\d{1,2}(?:[-,]\d{1,2})*)\b")


def parse_calendar_extract_term_name(pdf_url):
    """
    Determine the academic term name based on the PDF URL.

    Args:
        pdf_url (str): Calendar PDF URL

    Returns:
        str: Term name
    """

    if "2025-2026" in pdf_url:
        return "2025-2026 Academic Calendar"
    if "2026-2027" in pdf_url:
        return "2026-2027 Academic Calendar"
    return "Academic Calendar"


def parse_calendar_fix_misaligned_dates(df, pdf_url):
    """
    Normalize calendar OCR output into final calendar rows.

    Fixes cases where the date is embedded in the description
    (e.g., "12. Grade Checks - Week 4").

    Args:
        df (pd.DataFrame): Raw OCR calendar data
        pdf_url (str): Source calendar PDF URL

    Returns:
        pd.DataFrame: Cleaned and standardized calendar data
    """
    fixed = []
    term_name = parse_calendar_extract_term_name(pdf_url)

    for _, row in df.iterrows():
        date = str(row["date"]).strip()
        desc = row["description"].strip()

        # Handle misaligned descriptions containing dates
        match = re.match(r"^(\d{1,2}(?:-\d{1,2})?)[\.\s]+(.*)", desc)
        if match:
            date = match.group(1)
            desc = match.group(2)

        fixed.append({
            "Cengage Master Institution ID": 258458060593326033,
            "Source URL": pdf_url,
            "Term Name": term_name,
            "Term Date": f'{row["month"]} {date}',
            "Term Date Description": (
                desc.replace('—s', '')
                    .replace('—', '')
                    .replace('S$', '')
                    .replace('  ', ' ')
                    .replace('=- ','')
                    .replace(' - ','')
                    .strip()
            )
        })

    return pd.DataFrame(fixed)


class SccSpider(scrapy.Spider):
    name = "scc"
    institution_id = 258458060593326033

    calendar_rows = []

    course_url = "https://sccc.edu/academics/class-schedule/"
    
    directory_url = "https://sccc.edu/campus-directory/"

    # Headers for directory request
    directory_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

    # Academic calendar PDFs
    calendar_PDF_URLS = [
        "https://go.boarddocs.com/ks/sccc/Board.nsf/files/DLBLFJ5641C5/$file/Academic%20Calendar%202025-2026.pdf",
        "https://go.boarddocs.com/ks/sccc/Board.nsf/files/DNAKD9515447/$file/Academic%20Calendar%202026-2027%20BOT%20approved.pdf"
    ]

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url='https://www.google.com/',callback=self.parse_course,dont_filter=True)
        elif mode == 'directory':
           yield scrapy.Request(url = self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            for url in self.calendar_PDF_URLS:
                yield scrapy.Request(url=url,callback=self.parse_calendar,dont_filter=True)
                
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url='https://www.google.com/',callback=self.parse_course,dont_filter=True)
            yield scrapy.Request(url = self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url='https://www.google.com/',callback=self.parse_course,dont_filter=True)
            for url in self.calendar_PDF_URLS:
                yield scrapy.Request(url=url,callback=self.parse_calendar,dont_filter=True)

        elif mode in ['directory_calendar', 'calendar_directory']:
            for url in self.calendar_PDF_URLS:
                yield scrapy.Request(url=url,callback=self.parse_calendar,dont_filter=True)
            yield scrapy.Request(url = self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url='https://www.google.com/',callback=self.parse_course,dont_filter=True)
            yield scrapy.Request(url = self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            for url in self.calendar_PDF_URLS:
                yield scrapy.Request(url=url,callback=self.parse_calendar,dont_filter=True)

       


    def parse_course(self):
        
        """
        Scrape course data from Power BI grid using Playwright.

        Uses scrolling logic to fully load and capture all grid rows.
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False,slow_mo=80,args=["--disable-gpu"])
            context = browser.new_context()
            page = context.new_page()

            # Open course page
            page.goto(self.course_url, timeout=60000)
            page.wait_for_load_state("networkidle")

            # Locate Power BI iframe
            page.wait_for_selector("iframe", timeout=60000)
            powerbi_frame = page.frame(url=lambda u: u and "powerbi" in u.lower())

            if not powerbi_frame:
                raise Exception("Power BI iframe not found")

            # APPLY FILTERS
            powerbi_frame.locator("span.slicerText", has_text="Spring 2026").first.click(force=True)
            time.sleep(6)

            powerbi_frame.locator("span.slicerText", has_text="Late Start or Starts Soon").first.click(force=True)
            time.sleep(6)

            # Locate grid
            powerbi_frame.wait_for_selector('div[role="grid"]', timeout=60000)
            grid = powerbi_frame.locator('div[role="grid"][aria-colcount="21"]').first

            rows_dict = {}
            last_row_count = 0
            no_new_rounds = 0

           # Scroll until no new rows appear
            while no_new_rounds < 3:

                # COLLECT VISIBLE CELLS
                rows = grid.locator('div[role="row"][aria-rowindex]')
                for i in range(rows.count()):
                    row = rows.nth(i)
                    row_index = row.get_attribute("aria-rowindex")
                    if not row_index:
                        continue

                    rows_dict.setdefault(row_index, {})
                    cells = row.locator('div[role="gridcell"][aria-colindex]')

                    for j in range(cells.count()):
                        cell = cells.nth(j)
                        col_index = cell.get_attribute("aria-colindex")
                        text = (cell.text_content() or "").strip()
                        if col_index and col_index not in rows_dict[row_index]:
                            rows_dict[row_index][col_index] = text

                # SCROLL RIGHT
                grid.click()
                page.keyboard.down("Shift")
                page.mouse.wheel(3000, 0)
                page.wait_for_timeout(400)
                page.keyboard.up("Shift")

                # SCROLL LEFT
                grid.click()
                page.keyboard.down("Shift")
                page.mouse.wheel(-3000, 0)
                page.wait_for_timeout(400)
                page.keyboard.up("Shift")

                # SCROLL DOWN
                box = grid.bounding_box()
                page.mouse.move(
                    box["x"] + box["width"] / 2,
                    box["y"] + box["height"] / 2
                )
                page.mouse.wheel(0, 350)
                page.wait_for_timeout(500)

                if len(rows_dict) == last_row_count:
                    no_new_rounds += 1
                else:
                    last_row_count = len(rows_dict)
                    no_new_rounds = 0

            # FINAL ASSEMBLY (21 COLUMNS)
            final_rows = []
            for row_index in sorted(rows_dict, key=int):
                row = rows_dict[row_index]
                final_rows.append([row.get(str(i), "") for i in range(1, 22)])

            course_rows = []

            for row in final_rows:
                if not row[1].isdigit():
                    continue

                course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_url,
                    "Course Name": f"{row[2]} {row[5]}",
                    "Course Description": "",
                    "Class Number": row[1],
                    "Section": "",
                    "Instructor": row[18],
                    "Enrollment": "",
                    "Course Dates": f"{row[8]} - {row[9]}",
                    "Location": row[4],
                    "Textbook/Course Materials": "",
                })

            course_df = pd.DataFrame(course_rows)
            save_df(course_df, self.institution_id, "course")

            browser.close()

     
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

        rows = []
        blocks = response.xpath('//table[@id="tablepress-28"]//tbody/tr')
        for block in blocks:
            name = block.xpath('.//td[1]/text()').get().strip()
            phone = block.xpath('.//td[2]/text()').get('').strip()
            title = block.xpath('.//td[4]/text()').get('').strip()
            email = block.xpath('.//td[5]/a/text()').get('').strip()
            rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Name": name,
                        "Title": title,
                        "Email": email,
                        "Phone Number": phone,
                    }
                )

        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        Parse academic calendar PDF using OCR.
        """
        pdf_url = response.url
        pdf_bytes = BytesIO(requests.get(pdf_url, timeout=60).content)

        page = convert_from_bytes(
            pdf_bytes.getvalue(),
            dpi=400,
            first_page=1,
            last_page=1,
            poppler_path=POPPLER_PATH
        )[0]

        # Crop RIGHT side
        w, h = page.size
        page = page.crop((int(w * 0.40), 0, w, h))

        gray = page.convert("L")
        gray = ImageEnhance.Contrast(gray).enhance(2.5)

        img_np = np.array(gray)
        _, thresh = cv2.threshold(img_np, 170, 255, cv2.THRESH_BINARY)
        processed = Image.fromarray(thresh)

        ocr_text = pytesseract.image_to_string(
            processed,
            config="--oem 3 --psm 6 -c preserve_interword_spaces=1"
        )

        rows = []
        current_month = None

        for line in ocr_text.splitlines():
            clean = re.sub(r"[~_=|©{}]+", " ", line).strip()
            clean = re.sub(r"\s{2,}", " ", clean)

            if not clean:
                continue

            m = month_pattern.search(clean)
            if m:
                current_month = m.group(1)

            d = date_pattern.search(clean)
            if d and current_month:
                date = d.group(1)
                desc = clean.replace(current_month, "").replace(date, "")
                desc = desc.strip(" -.")

                if len(desc) > 3:
                    rows.append({
                        "month": current_month,
                        "date": date,
                        "description": desc
                    })

        if not rows:
            return

        df_raw = pd.DataFrame(rows)
        df_fixed = parse_calendar_fix_misaligned_dates(df_raw, pdf_url)

        for _, row in df_fixed.iterrows():
            self.calendar_rows.append(row.to_dict())

    def closed(self, reason):
        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")