import re
import io
import scrapy
import pdfplumber
from io import BytesIO
import pandas as pd
from ..utils import *
from ..utils import save_df


class LakelandSpider(scrapy.Spider):
    """
    Scrapy spider for Lakeland College data extraction.

    Supports three independent scrape modes:
    - course    : Extract course schedules from multiple PDF catalogs
    - directory : Extract faculty/staff directory from HTML page
    - calendar  : Extract academic calendar events from a PDF

    Output files are saved via project utility `save_df`.
    """

    name = "lakeland"
    institution_id = 258438351693047769
    
    #Accumulators for extracted data
    calendar_rows = []
    course_rows = []

    #Course catalog PDFs (Graduate + Undergraduate)
    course_urls = [
        'https://lakeland.edu/pdfs/catalog/2025/SU25%20GRD.pdf',
        'https://lakeland.edu/pdfs/catalog/2025/SU25%20UGRD.pdf',
        'https://lakeland.edu/pdfs/catalog/2025/FA25%20UGRD.pdf',
        'https://lakeland.edu/perch/resources/2025-12-19sp26-ugrd.pdf',
        'https://lakeland.edu/pdfs/catalog/2026/SP26%20GRD.pdf'
    ]
    
    #Faculty directory page
    directory_url = "https://lakeland.edu/faculty"

    #Academic calendar PDF
    calendar_url = "https://catalog.lakelandcc.edu/calendar/calendar.pdf"
  
    #Entry point
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            for course_url in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
           yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

    def parse_course(self, response):
        """
        Parse course schedules from PDF catalogs.

        Extracts:
        - Course code & title
        - Section number
        - Instructor
        - Enrollment availability
        - Course dates
        - Location

        Each PDF page is scanned line-by-line using regex.
        """

        def clean_instructor(text):
            """
            Remove meeting times and normalize instructor names.
            """
            if not text:
                return ""
            text = re.sub(
                r'\b((?:[MTWRF]\s*)+)?\d{1,2}:\d{2}\s*(am|pm)'
                r'(\s+\d{1,2}:\d{2}\s*(am|pm))?\b',
                '',
                text,
                flags=re.IGNORECASE
            )
            return re.sub(r'\s{2,}', ' ', text).strip()

        def normalize_location(text):
            """
            Normalize various campus/location phrases into
            standardized values.
            """
            t = text.lower()

            if "online" in t:
                return "Online"
            if "hybrid" in t:
                return "Hybrid"
            if "home campus" in t or "on campus" in t:
                return "On Campus"
            if "green bay" in t:
                return "Green Bay Center"
            if "fox cities" in t:
                return "Fox Cities Center"
            if "lu center" in t:
                return "LU Center"
            if "japan" in t:
                return "Lakeland University - Japan"

            return text.strip()


        # Regex patterns
        course_re = re.compile(r"\b([A-Z]{2,4}\s*\d{3})[:\t ]+(.+)")
        section_re = re.compile(r"\b(K\d+|\d{2,3})\b")
        seats_re = re.compile(r"Seats available:\s*-?\d+\s*of\s*\d+")
        date_re = re.compile(r"\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}/\d{1,2}/\d{4}")
        
        location_re = re.compile(
            r"(Online|Hybrid|On Campus|Home Campus|"
            r"(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+Center|"
            r"LU Center|Lakeland University(?:\s*[-–]\s*[A-Za-z ]+)?)",
            re.I
        )

        pdf_url = response.url
        self.logger.info(f"📄 Processing PDF: {pdf_url}")

        # Use Scrapy response body directly instead of requests.get
        pdf_bytes = BytesIO(response.body)

        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                lines = [l.strip() for l in text.split("\n") if l.strip()]

                current_course = None
                pending_sections = []

                for line in lines:

                    # Course header detection
                    m = course_re.search(line)
                    if m:
                        if pending_sections:
                            self.course_rows.extend(pending_sections)
                            pending_sections = []

                        current_course = {
                            "course_code": m.group(1),
                            "course_title": m.group(2),
                            "Course Date": "",
                            "Location": ""
                        }
                        continue

                    if not current_course:
                        continue

                    # Course dates & location
                    date_match = date_re.search(line)
                    loc_match = location_re.search(line)

                    if date_match:
                        d1, d2 = date_match.group().split()
                        date_val = f"{d1} - {d2}"
                        current_course["Course Date"] = date_val
                        for s in pending_sections:
                            s["Course Dates"] = date_val

                    if loc_match:
                        loc_val = normalize_location(loc_match.group())
                        current_course["Location"] = loc_val
                        for s in pending_sections:
                            s["Location"] = loc_val

                    if date_match or loc_match:
                        continue

                    # Section rows
                    if section_re.search(line) and seats_re.search(line):
                        section = section_re.search(line).group()
                        enrollment = seats_re.search(line).group().replace(
                            "Seats available:", ""
                        ).strip()

                        instr_match = re.search(
                            r"(?:K\d+|\d{2,3})\s+"
                            r"(?:[MTWRF]\s+\d{1,2}:\d{2}\s*(?:am|pm)\s+"
                            r"\d{1,2}:\d{2}\s*(?:am|pm)\s+)?"
                            r"(.*?)\s+Seats available",
                            line,
                            re.I
                        )

                        instructor = clean_instructor(
                            instr_match.group(1) if instr_match else ""
                        )

                        pending_sections.append({
                            "Cengage Master Institution ID": 258438351693047769,
                            "Source URL": pdf_url,
                            "Course Name": f"{current_course['course_code']} {current_course['course_title']}",
                            "Course Description": "",
                            "Class Number": current_course["course_code"],
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": enrollment,
                            "Course Dates": current_course.get("Course Date", ""),
                            "Location": current_course.get("Location", ""),
                            "Textbook/Course Materials": ""
                        })

                # Flush end of page
                if pending_sections:
                    self.course_rows.extend(pending_sections)

        # Save CSV per PDF (optional) or yield items
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")
        
    # DIRECTORY PARSER      
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
        rows = []
        blocks = response.xpath('//div[@class="fList list"]/div')
        for block in blocks:
            name = block.xpath('.//h3/text()').get('').strip()
            title = block.xpath('.//p/text()').get('').strip()
            email = block.xpath('.//a[starts-with(@href, "mailto:")]/@href').get('').replace("mailto:", "")
            phone = block.xpath(".//a[starts-with(@href, 'tel:')]/@href").get('').replace("tel:", "")

            rows.append(
                {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.directory_url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
                }
                )

        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")

    # CALENDAR PARSER
    def parse_calendar(self, response):
        """
        Parse academic calendar PDF and extract:
        - Term Name
        - Term Date
        - Term Date Description
        """

        rows = []
        current_term = None
        source_url = response.url

        # Load PDF directly from Scrapy response
        pdf_bytes = io.BytesIO(response.body)

        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                width = page.width
                height = page.height

                # Split page into left (event) and right (date)
                left_bbox = (0, 0, width * 0.58, height)
                right_bbox = (width * 0.58, 0, width, height)

                left_words = page.crop(left_bbox).extract_words(use_text_flow=True)
                right_words = page.crop(right_bbox).extract_words(use_text_flow=True)

                def group_by_line(words, tol=4):
                    lines = {}
                    for w in words:
                        y = round(w["top"] / tol) * tol
                        lines.setdefault(y, []).append(w["text"])
                    return {y: " ".join(texts).strip() for y, texts in lines.items()}

                left_lines = group_by_line(left_words)
                right_lines = group_by_line(right_words)

                for ly, event in left_lines.items():
                    if not event:
                        continue

                    text = event.strip().lower()

                    # Detect semester headers
                    if text == "summer semester":
                        current_term = "Summer Semester"
                        continue
                    elif text == "fall semester":
                        current_term = "Fall Semester"
                        continue
                    elif text == "spring semester":
                        current_term = "Spring Semester"
                        continue

                    if current_term is None:
                        continue

                    # Find closest date by Y-axis
                    nearest_y = min(
                        right_lines.keys(),
                        key=lambda ry: abs(ry - ly),
                        default=None
                    )

                    if not nearest_y:
                        continue

                    date = right_lines.get(nearest_y, "").strip()

                    # Basic date validation
                    if "," in date and len(date) > 6:
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": source_url,
                            "Term Name": current_term,
                            "Term Date": date,
                            "Term Date Description": event.replace('--','')
                        })

        if not rows:
            self.logger.warning("No calendar records extracted")
            return

        calendar_df = pd.DataFrame(rows).drop_duplicates()
        self.logger.info(f"Calendar records extracted: {len(calendar_df)}")
        # Save using your project utility
        save_df(calendar_df, self.institution_id, "calendar")
