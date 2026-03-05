import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from pypdf import PdfReader
from ..utils import save_df
from parsel import Selector


def parse_directory_extract_field(selector, label):
    """
    Extract text content that appears after a specific <strong> label
    inside a paragraph tag.

    Args:
        selector (Selector): Parsel selector object of the page.
        label (str): The label text to search for (e.g., "Email Address:").

    Returns:
        str: Cleaned extracted text or empty string if not found.
    """
    texts = selector.xpath(
        f'//strong[contains(text(),"{label}")]/parent::p//text()'
    ).getall()

    if not texts:
        return ""

    return (
        " ".join(texts)
        .replace(label, "")
        .replace("\xa0", "")
        .strip()
    )

class SwcuSpider(scrapy.Spider):
    """
    Scrapy spider for scraping course schedules, faculty directory,
    and academic calendar data from Southwestern Christian University (SWCU).
    """

    name = "swcu"

    # Unique institution identifier used across all datasets
    institution_id = 258441048093648849

    # COURSE CONFIGURATION
    course_urls = [
        'https://docs.google.com/spreadsheets/d/12JSwzBH5a-KXJQaiTuyQNIuFPwNR11crTc_pc7spn9Q/export?format=csv&gid=1551650779',
        "https://docs.google.com/spreadsheets/d/1RVQKjAO4JZgU6jji25EorZIVK4Sgo_YKGB7Kx4JaTRY/export?format=csv&gid=1551650779"
        ]
    course_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        }
    
    # DIRECTORY CONFIGURATION
    directory_url = "https://swcu.edu/academics/faculty"
    directory_headers = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
    }

    # CALENDAR CONFIGURATION
    calendar_urls= [
        {
            "url": "https://swcu.edu/application/files/7017/6366/6911/SCU_Calendar_25-26_1.pdf",
            "type": "academic",
            "term_name": "2025-2026 Academic Calendar"
        },
        {
            "url": "https://swcu.edu/application/files/4517/5189/9903/Fall_2025_Important_Dates.pdf",
            "type": "important_dates"
        }
    ]


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage for scraped data
        self.course_rows = []       # Stores all course data
        self.directory_rows = []    # Stores all directory (faculty/staff) data
        self.calendar_rows = []     # Stores all calendar events data

    def start_requests(self):
        """
        Entry point for the spider.
        Scrape mode can be controlled using SCRAPE_MODE setting.
        """
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')  # Determine mode

        if mode == 'course':
           self.parse_course()

        elif mode == 'directory':
            # Only scrape directory data
            self.parse_directory()

        elif mode == 'calendar':
            self.parse_calendar()

        else:
            # Default: scrape course, directory, and calendar
            self.parse_course()
            self.parse_directory()
            self.parse_calendar()


    def parse_course(self):
        """
        Extract course schedule data from Google Sheets CSV exports.

        Steps:
        1. Download CSV
        2. Detect actual header row
        3. Load into DataFrame
        4. Filter unwanted rows
        5. Normalize and store structured course data
        """

        for url in self.course_urls:
            
            response = requests.get(url,headers=self.course_headers)
            data = response.content.decode('utf-8')

            # 2. Find the course list start row
            lines = data.splitlines()
            start_index = 0
            for i, line in enumerate(lines):
                if "Class Room,Course Number,Section" in line:
                    start_index = i
                    break

            # 3. Load CSV into DataFrame
            df = pd.read_csv(io.StringIO("\n".join(lines[start_index:])))

            # 4. Keep required columns
            cols_to_keep = [
                'Class Room',
                'Course Number',
                'Section',
                'Course Name',
                'Instructor',
                'Time'
            ]
            df = df[cols_to_keep].copy()

            # 5. Exclusion Filter
            exclude_list = [
                'Course Number', 'Course #', 'Monday', 'Tuesday', 'Thursday', 'Friday',
                'CHAP 1', 'ENROLL IN CHAPEL', 'Online Options', '1BON:',
                '2BON:', 'Seminars', 'Evening Courses', 'Module',
                'Directed Studies', 'MUSIC COURSES'
            ]

            pattern = '|'.join(exclude_list)
            df = df[~df['Course Number'].astype(str).str.contains(pattern, na=False, case=False)]
            df = df.dropna(subset=['Course Number', 'Course Name'], how='all')

            # 6. Iterate and append properly
            for _, row in df.iterrows():

                course_number = str(row.get('Course Number', '')).strip()
                course_name = str(row.get('Course Name', '')).strip()
                section = str(row.get('Section', '')).strip()
                instructor = str(row.get('Instructor', '')).strip()
                location = str(row.get('Class Room', '')).strip()
                date = str(row.get('Time', '')).strip()

                full_course_name = f"{course_number} {course_name}".strip()

                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": re.sub(r'\s+', ' ', full_course_name),
                    "Course Description": "",
                    "Class Number": course_number,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": "",
                    "Course Dates": date,
                    "Location": location,
                    "Textbook/Course Materials": "",
                })
            
        # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    def parse_directory(self):
        """
        Scrape faculty directory page.

        Handles:
        - Faculty without profile pages
        - Faculty with profile pages
        - Extracts name, title, phone, and email
        """

        res = requests.get(self.directory_url, headers=self.directory_headers)
        response = Selector(text=res.text)

        blocks = response.xpath('//div[@class="sass-container main"]//ul//li')
        for block in blocks:
            link = block.xpath('./a/@href').get('')

            # -------- NO PROFILE PAGE -------- #
            if not link:
                text = " ".join(block.xpath('.//text()').getall()).strip()

                if "," in text:
                    name, title = map(str.strip, text.split(",", 1))
                else:
                    name = text
                    title = ""
                if name:
                    self.directory_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": res.url,
                        "Name": name,
                        "Title": title,
                        "Email": "",
                        "Phone Number": ""
                    })
                    continue

            # -------- PROFILE PAGE -------- #
            profile_res = requests.get(link, headers=self.directory_headers)
            profile = Selector(text=profile_res.text)

            name = profile.xpath('//h1/text()').get("").strip()
            title = parse_directory_extract_field(profile, "Title/Position:")
            phone = parse_directory_extract_field(profile, "Campus Phone Number:")
            email = parse_directory_extract_field(profile, "Email Address:")
            if name:
                self.directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": profile_res.url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone
                })

        # # Save updated DataFrame
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")


    def parse_calendar(self):
        """
        Extract academic calendar and important dates
        from PDF files.

        - Uses pypdf for structured academic calendar
        - Uses pdfplumber for table-based important dates
        """

        def clean_text(text):
            """Normalize spacing and fix broken words."""
            if not text:
                return ""
            text = re.sub(r'(?<=[a-zA-Z])\s(?=[a-zA-Z]\b)', '', text)
            text = text.replace("C losed", "Closed").replace("M ove i n", "Move In")
            return text.strip()
        for item in self.calendar_urls:
            url = item["url"]
            print(f"Processing: {url}")

            response = requests.get(url)
            response.raise_for_status()

            # Academic Calendar (Text Pattern Extraction)
            if item["type"] == "academic":
                reader = PdfReader(io.BytesIO(response.content))

                pattern = re.compile(
                    r"([A-Z][a-z]+\.?\s\d+(?:-\d+)?(?:-\s?[A-Z][a-z]+\s\d+)?)\s?-\s?(.*)"
                )

                for page in reader.pages:
                    text = page.extract_text()
                    if not text:
                        continue

                    for line in text.split("\n"):
                        match = pattern.search(line)
                        if match:
                            date, desc = match.groups()

                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": url,
                                "Term Name": item["term_name"],
                                "Term Date": clean_text(date),
                                "Term Date Description": clean_text(desc)
                            })

            # Important Dates (Table Extraction)
            elif item["type"] == "important_dates":
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                    for page in pdf.pages:
                        width, height = page.width, page.height
                        split_x = width * 0.51

                        regions = [
                            ((0, 0, split_x, height), "Fall 2025"),
                            ((split_x, 0, width, height), "Spring 2026")
                        ]

                        for bbox, term in regions:
                            crop = page.within_bbox(bbox)
                            table = crop.extract_table({
                                "vertical_strategy": "text",
                                "horizontal_strategy": "text",
                                "snap_y_tolerance": 6,
                                "intersection_x_tolerance": 15,
                            })

                            if not table:
                                continue

                            for row in table:
                                cells = [c for c in row if c]
                                if len(cells) < 2:
                                    continue

                                date_raw = cells[0].replace("\n", " ").strip()
                                desc_raw = " ".join(cells[1:]).replace("\n", " ").strip()

                                if "Date" in date_raw:
                                    continue

                                date_val = clean_text(date_raw)
                                desc_val = clean_text(desc_raw)

                                if term == "Spring 2026" and date_val.startswith("30/2026"):
                                    date_val = "6/30/2026"

                                self.calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": url,
                                    "Term Name": term,
                                    "Term Date": date_val,
                                    "Term Date Description": desc_val
                                })

        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")