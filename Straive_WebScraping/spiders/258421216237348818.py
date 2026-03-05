import re
import io
import json
import scrapy
import requests
import pdfplumber
import pandas as pd
from parsel import Selector
from datetime import datetime
from ..utils import save_df

class LrscSpider(scrapy.Spider):
    """
    Scrapy Spider for scraping Lake Region State College data.

    This spider extracts:
    1. Course schedules (Spring + Term V PDF formats)
    2. Faculty/Staff directory
    3. Academic calendar events

    Institution:
        Lake Region State College (LRSC)
    """

    name = "lrsc"

    # Unique institution identifier used across all datasets
    institution_id = 258421216237348818

    # Course PDF URLs (Spring + Term V formats)
    course_urls = [
        "https://www.lrsc.edu/sites/default/files/2026-01/2630%20Course%20Schedule_0.pdf",   # Spring
        "https://www.lrsc.edu/sites/default/files/2026-02/Term%20V%20Schedule%20.pdf"       # Term V
        ]
    
    # Faculty directory AJAX endpoint
    directory_url = "https://www.lrsc.edu/views/ajax?_wrapper_format=drupal_ajax&view_name=faculty_directory&view_display_id=block_2&view_args=&view_path=%2Fnode%2F8&view_base_path=&view_dom_id=df32b956de82db35c0cbada6046f67e9bc79ea930ba3593bf6025a0498c4159f&pager_element=0&page=0&_drupal_ajax=1&ajax_page_state%5Btheme%5D=lrsc&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D=eJyNkkFywyAMRS9km1XPwwhQCI5AHiQ3dk4fmow9zqJ1N8zX10OMkByqYrW4TCwY7CVRC8VELFiBOkfwWI1LPMAIS-eYVbTC9GUcCH7Eu-xH6QkURY_5SOyAetGVUonHTEYRiChHr3A4VLc_t1DMRHNMZXDgb7HyXIL1TFwHNycKv-NcA55Di5UrBL6fgBnqi_8TmiCE1uUJpbioBUqxZCz6H_jYrgfCEqCaTQx6xYydv2FIytWC963zxMXsarhULtrorhnYfrnm9v4DO3RqU25TMLvqqIrfxjbKR7hNUVZRzO9d-E54F_M638tyNDKHmfAJkYHzmw"
    directory_headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://www.lrsc.edu/faculty-staff',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }
    
    # Academic calendar page
    calendar_url = "https://www.lrsc.edu/calendar"

    def __init__(self, *args, **kwargs):
        """
        Initializes storage containers for all datasets.
        """

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
            for page in range(0, 24):  # Arbitrary page limit to prevent infinite scraping
                url = f"{self.directory_url}&page={page}"
                yield scrapy.Request(
                    url,
                    headers=self.directory_headers,
                    callback=self.parse_directory,
                    dont_filter=True
                )

        elif mode == 'calendar':
            # Only scrape academic calendar
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default: scrape course, directory, and calendar
            self.parse_course()
            for page in range(0, 24):  # Arbitrary page limit to prevent infinite scraping
                url = f"{self.directory_url}&page={page}"
                yield scrapy.Request(
                    url,
                    headers=self.directory_headers,
                    callback=self.parse_directory,
                    dont_filter=True
                )
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)


    def parse_course(self):
        """
        Downloads and parses course schedule PDFs.

        Handles two formats:
            1. Spring format (Early 8 Week / Late 8 Week keywords)
            2. Term V format (table-based alignment using word coordinates)
        """

        for url in self.course_urls:
            print(f"Processing: {url}")
            response = requests.get(url)
            if response.status_code != 200:
                continue

            with pdfplumber.open(io.BytesIO(response.content)) as pdf:

                for page in pdf.pages:
                    text = page.extract_text() or ""
                    words = page.extract_words()

                    # Detect Format Type (Spring vs Term V)
                    if "Early 8 Week" in text or "Late 8 Week" in text:
                        # ================= SPRING FORMAT =================

                        current_subject = ""
                        course_pattern = re.compile(r'^([A-Z]{2,4})?\s*(\d{3}[A-Z]?)\s+(\d{5})')
                        session_keywords = r'(Regular|Early 8 Week|Late 8 Week|15 Week)'

                        for line in text.split("\n"):
                            line = line.strip()
                            match = course_pattern.match(line)

                            if match:
                                subj_part = match.group(1)
                                if subj_part:
                                    current_subject = subj_part

                                catalog = match.group(2)
                                class_id = match.group(3)

                                remaining = line[match.end():].strip()
                                split_title = re.split(session_keywords, remaining, maxsplit=1)

                                if len(split_title) >= 2:
                                    course_title = split_title[0].strip()
                                    metadata = split_title[1] + split_title[2]

                                    parts = metadata.split()
                                    instructor = (
                                        " ".join(parts[-2:])
                                        if len(parts) >= 2 and ("," in parts[-1] or "," in parts[-2])
                                        else parts[-1]
                                    )
                                else:
                                    course_title = remaining
                                    instructor = "TBA"

                                course_name = f"{current_subject} {catalog} {course_title}".strip()
                                
                                self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": url,
                                    "Course Name": course_name,
                                    "Course Description": '',
                                    "Class Number": class_id,
                                    "Section": '',
                                    "Instructor": instructor,
                                    "Enrollment": '',
                                    "Course Dates": '',
                                    "Location": '',
                                    "Textbook/Course Materials": ''
                                })

                    else:
                        #TERM V FORMA

                        anchors = [w for w in words if re.fullmatch(r'\d{5}', w['text'])]

                        for anchor in anchors:
                            y_center = (anchor['top'] + anchor['bottom']) / 2

                            row_words = [
                                w for w in words
                                if abs(((w['top'] + w['bottom']) / 2) - y_center) < 8
                            ]

                            row_words.sort(key=lambda x: x['x0'])

                            if len(row_words) >= 4:
                                class_id = row_words[0]['text']
                                cr = row_words[1]['text']
                                dept = row_words[2]['text']
                                course = row_words[3]['text']

                                remainder_words = row_words[4:]
                                title_parts = []
                                instructor_parts = []
                                found_separator = False

                                for w in remainder_words:
                                    txt = w['text']

                                    if re.search(r'\d:\d{2}|Mon/Wed|Tues/Thur|Monday|Tuesday|n/a', txt):
                                        found_separator = True
                                        continue

                                    if not found_separator:
                                        if w['x0'] < 450:
                                            title_parts.append(txt)
                                    else:
                                        if w['x0'] >= 450:
                                            instructor_parts.append(txt)

                                course_name = f"{dept} {course.replace('x','')} {' '.join(title_parts)}".strip()

                                instructor = " ".join(instructor_parts)
                                instructor = re.sub(
                                    r'Mon/Fri|Friday|Thursday|Wednesday|Tuesday|Monday',
                                    '',
                                    instructor
                                ).strip()
                               
                                self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": url,
                                    "Course Name": course_name,
                                    "Course Description": '',
                                    "Class Number": class_id,
                                    "Section": cr,
                                    "Instructor": instructor,
                                    "Enrollment": '',
                                    "Course Dates": '',
                                    "Location": '',
                                    "Textbook/Course Materials": ''
                                })


    def parse_directory(self, response):
        """
        Parses AJAX directory listing page and extracts
        faculty profile links.
        """
     
        json_datas = response.json()
        for item in json_datas:
            if item.get("command") == "insert" and item.get("data"):
                html = item["data"]
                sel = Selector(text=html)
                links = sel.xpath('//a[starts-with(@href, "/faculty-profile/")]/@href').getall()
                for link in links:
                    yield scrapy.Request(response.urljoin(link), callback=self.parse_directory_details)
                
    def parse_directory_details(self, response):
        """
        Extracts faculty profile details:
            - Name
            - Title
            - Email
            - Phone
        """

        name = response.xpath('//span[@class="field field--name-title field--type-string field--label-hidden"]/text()').get('').strip()
        
        title1 = response.xpath('//div[@class="field field--name-field-faculty-title field--type-string field--label-hidden field__item"]/text()').get('').strip()
        title2 = response.xpath('//div[@class="field field--name-field-department field--type-entity-reference field--label-hidden field__item"]/text()').get('').strip()
        title = title1 + ', ' + title2 if title1 and title2 else title1 or title2
        
        email = response.xpath('//div[@class="field field--name-field-email field--type-email field--label-hidden field__item"]/a/text()').get('')
        phone = response.xpath('//div[@class="field field--name-field-phone field--type-string field--label-hidden field__item"]//text()').get('')
       
        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

    def parse_calendar(self, response):
        """
        Extracts academic calendar events from embedded
        Drupal JSON configuration.
        """
        calendar_datas = (response.xpath('//script[@data-drupal-selector="drupal-settings-json"]').get('').replace('</script>', '').replace('<script type="application/json" data-drupal-selector="drupal-settings-json">',''))
        calendar_json = json.loads(calendar_datas)
        calendar_options_str = calendar_json["fullCalendarView"][0]["calendar_options"]
        calendar_options = json.loads(calendar_options_str)

        events = []

        # Step 1: collect + parse datetime
        for event in calendar_options.get("events", []):
            title = event.get("title", "").strip()
            start = event.get("start", "")
            if not start:
                continue

            dt = datetime.fromisoformat(start)
            events.append({
                "title": title,
                "dt": dt
            })

        # Step 2: sort events by datetime (ascending)
        events.sort(key=lambda x: x["dt"])

        # Step 3: build output rows in order
        for e in events:
            title = e["title"]
            dt = e["dt"]

            term_name = dt.strftime("%B %Y")
            term_date = dt.strftime("%d-%A").lstrip("0")

            hour = dt.hour
            if hour == 0:
                time_str = "12a"
            elif hour < 12:
                time_str = f"{hour}a"
            elif hour == 12:
                time_str = "12p"
            else:
                time_str = f"{hour - 12}p"

            term_desc = f"{time_str} {title}"
            term_desc = term_desc.replace('&amp;', '&').replace('&#039;', "'")

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": term_name,
                "Term Date": term_date,
                "Term Date Description": term_desc,
            })

        # Save calendar events
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")

    # SPIDER CLOSE HANDLER
    def closed(self, reason):
        """
        Called automatically when spider closes.

        Saves directory dataset after all profile pages are processed.
        """
        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")

        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")