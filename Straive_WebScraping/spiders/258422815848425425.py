import io
import re
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from parsel import Selector


class SoutheasterSpider(scrapy.Spider):
    name = "southeater"

    # Unique institution ID (used across all datasets)
    institution_id = 258422815848425425

    # Faculty / Staff directory URL
    directory_url = 'https://www.cacc.edu/about/faculty-staff-directory'

    # Academic calendar page
    calendar_url = "https://www.southeasterntech.edu/academic-calendar/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.directory_rows = []
        self.calendar_rows = []
        self.course_rows = []


    def start_requests(self):

        # Read scrape mode from settings
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode ----
        if mode == 'course':
            self.parse_course()

        elif mode == 'directory':
            yield scrapy.Request(
                url=self.directory_url,
                callback=self.parse_directory,
                dont_filter=True
            )

        elif mode == 'calendar':
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar
            )

        # ---- Combined Modes ----
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            yield scrapy.Request(
                url=self.directory_url,
                callback=self.parse_directory,
                dont_filter=True
            )

        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar
            )

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar
            )
            yield scrapy.Request(
                url=self.directory_url,
                callback=self.parse_directory,
                dont_filter=True
            )

        # ---- Default: Scrape Everything ----
        else:
            self.parse_course()
            yield scrapy.Request(
                url=self.directory_url,
                callback=self.parse_directory,
                dont_filter=True
            )
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar
            )


    def parse_course(self):
        """Scrapes course data using requests-based pagination"""

        def clean(text):
            """Normalize text safely"""
            if isinstance(text, list) or hasattr(text, 'get'):
                text = text.get() if hasattr(text, 'get') else text[0] if text else ""
            return " ".join(text.split()) if text else ""

        session = requests.Session()

        # Browser-like headers
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/143.0.0.0 Safari/537.36"
            ),
            "referer": "https://ce.southeasterntech.edu/search/publicCourseAdvancedSearch.do?method=load",
        }
        session.headers.update(headers)

        # Base search URL
        base_url = (
            "https://ce.southeasterntech.edu/search/publicCourseAdvancedSearch.do"
            "?method=doPaginatedSearch"
            "&showInternal=false"
            "&courseSearch.courseDescriptionKeyword="
            "&courseSearch.courseCategoryStringArray=0"
            "&courseSearch.programAreaStringArray=1020779"
            "&courseSearch.programAreaStringArray=1114961"
            "&courseSearch.programAreaStringArray=1020780"
            "&courseSearch.programAreaStringArray=1128484"
            "&courseSearch.programAreaStringArray=1020783"
            "&courseSearch.programAreaStringArray=1020781"
            "&courseSearch.programAreaStringArray=1020782"
            "&courseSearch.sectionAccreditingAssociationStringArray=0"
            "&courseSearch.filterString=all"
        )

        # Pagination URL
        pagination_url = (
            "https://ce.southeasterntech.edu/search/publicCourseAdvancedSearch.do"
            "?method=doPagination"
            "&tag=displaytag"
            "&d-5246410-p={page}"
        )

        pages = []

        # First page
        resp = session.get(base_url)
        pages.append(Selector(text=resp.text))

        # Additional pages (fixed range)
        for page in range(2, 4):
            resp = session.get(pagination_url.format(page=page))
            pages.append(Selector(text=resp.text))

        # Collect course links
        course_links = []
        for res in pages:
            links = res.xpath(
                '//table[contains(@class,"table-striped")]//td/a/@href'
            ).getall()
            for link in links:
                course_links.append(f"https://ce.southeasterntech.edu{link}")

        # Process each course
        for link in course_links:
            response = session.get(link)
            res2 = Selector(text=response.text)

            class_num = clean(
                res2.xpath('//span[@class="courseCode"]//span/text()').get("")
            )
            title = clean(
                res2.xpath('//span[@class="title"]/text()').get("")
            )
            course_name = f"{class_num} - {title}".strip()

            description = clean(
                " ".join(
                    res2.xpath(
                        '//div[@class="courseDescriptionCollapsibleWrapper"]/div//text()'
                    ).getall()
                )
            )

            if description:
                description = description.replace(
                    'Course Description', ''
                ).strip()

            blocks = res2.xpath(
                '//div[@class="courseSection card panel-default"]'
            )

            # No section scenario
            if not blocks:
                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": link,
                    "Course Name": course_name,
                    "Course Description": description,
                    "Class Number": class_num,
                    "Section": "",
                    "Instructor": "",
                    "Enrollment": "",
                    "Course Dates": "",
                    "Location": "",
                    "Textbook/Course Materials": ""
                })
                continue

            seen_sections = set()

            for block in blocks:
                section = clean(
                    block.xpath(
                        './/h3[@class="courseSectionTitle"]//span[@class="sectionCode"]/text()'
                    ).get("")
                )

                course_dates = clean(
                    block.xpath(
                        './/span[@class="courseSectionBeginDate"]//text()'
                    ).get("")
                )

                if section in seen_sections:
                    continue
                seen_sections.add(section)

                # Location popup
                location_id_raw = clean(
                    block.xpath('.//ul[@class="list-unstyled"]/@id')
                )
                location_match = re.search(r'(\d+)$', location_id_raw)
                location_id = location_match.group(1) if location_match else None

                full_location = ""
                if location_id:
                    popup_url = (
                        "https://ce.southeasterntech.edu/search/courseSectionSchedulePopup.do"
                        f"?method=popupDates&site=public&sectionId={location_id}"
                    )
                    popup_resp = requests.get(popup_url, headers={
                        'user-agent': headers['user-agent'],
                        'x-requested-with': 'XMLHttpRequest',
                    })
                    popup_sel = Selector(text=popup_resp.text)
                    location_lines = popup_sel.xpath(
                        '//td[contains(@class,"sectionSchedulePopupLocation")]//text()'
                    ).getall()
                    full_location = " ".join(
                        line.strip() for line in location_lines if line.strip()
                    )

                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": link,
                    "Course Name": course_name,
                    "Course Description": description,
                    "Class Number": class_num,
                    "Section": section,
                    "Instructor": "",
                    "Enrollment": "",
                    "Course Dates": course_dates,
                    "Location": full_location,
                    "Textbook/Course Materials": ""
                })

        # ❌ Original logic preserved (even if incorrect)
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "course")


    def parse_directory(self, response):
        links = response.xpath(
            '//table[@class="table table-bordered table-hover"]//tr/td/a/@href'
        ).getall()

        for link in links:
            link = response.urljoin(link)
            yield scrapy.Request(link, callback=self.parse_directory_details)

    def parse_directory_details(self, response):

        name = response.xpath('//h2/text()').get(default='').strip()
        email = response.xpath(
            '//div[@class="mb-2"]/a/@href'
        ).get(default='').replace('mailto:', '').strip()

        title = response.xpath('//h3/text()').get(default='').strip()
        if not title:
            title = response.xpath(
                '//p[@class="h4 mb-1"]//text()'
            ).get('')

        phones = response.xpath(
            '//a[starts-with(@href,"tel:")]/@href'
        ).get(default='').strip()

        num = phones.replace('tel:', '')
        digits = re.sub(r'\D', '', num)

        formatted = ''
        if len(digits) == 10:
            formatted = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"

        ext_raw = response.xpath(
            "//a[span[text()='Phone number']]/following-sibling::text()"
        ).get(default='').strip()

        phone = f"{formatted} {ext_raw}".strip() if formatted else ''

        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):

        # Hardcoded calendar PDF URLs
        urls = [
            'https://www.southeasterntech.edu/wp-content/uploads/Summer-Academic-Calendar-202616.pdf',
            'https://www.southeasterntech.edu/wp-content/uploads/Spring-Academic-Calendar-202614_.pdf',
            'https://www.southeasterntech.edu/wp-content/uploads/Fall-Academic-Calendar-202612-101525.1.pdf',
            'https://www.southeasterntech.edu/wp-content/uploads/Summer-Academic-Calendar-202516.1-1.pdf',
            'https://www.southeasterntech.edu/wp-content/uploads/Spring-Academic-Calendar-202514_1-1.pdf'
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_calendar_pdf)

    def extract_term_name(self, text):
        m = re.search(r"(Spring|Summer|Fall|Winter)\s+(20\d{2})", text, re.I)
        return f"{m.group(1).title()} {m.group(2)}" if m else None

    # Robust date pattern
    DATE_PATTERN = re.compile(
        r"""^(
            TBD |
            [A-Za-z]{3,9}\s+\d{1,2} |
            [A-Za-z]{3,9}\s+\d{1,2}\s*[–-]\s*[A-Za-z]{3,9}\s+\d{1,2}
        )\s+(.*)$""",
        re.I | re.X
    )

    def normalize_range(self, date_str):
        return date_str.replace("-", "–").strip()

    def parse_calendar_pdf(self, response):

        pdf_bytes = io.BytesIO(response.body)

        with pdfplumber.open(pdf_bytes) as pdf:
            first_page = pdf.pages[0].extract_text() or ""
            full_text = "\n".join(
                p.extract_text() for p in pdf.pages if p.extract_text()
            )

        term_name = self.extract_term_name(first_page)
        current_event = None

        lines = [
            l.strip().replace("â€“", "–")
            for l in full_text.split("\n")
            if l.strip()
        ]

        for line in lines:

            # Skip publish dates
            if re.match(r"\d{1,2}/\d{1,2}/\d{2,4}$", line):
                continue

            # Skip headers
            if re.fullmatch(
                r"(Spring|Summer|Fall|Winter)\s+Term\s+\d{4}",
                line,
                re.I
            ):
                continue

            if "Information Disclaimer" in line:
                continue

            match = self.DATE_PATTERN.match(line)

            if match and term_name:
                if current_event:
                    self.calendar_rows.append(current_event)

                current_event = {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": self.normalize_range(match.group(1)),
                    "Term Date Description": match.group(2).strip().replace('–', '-'),
                }

            elif current_event:
                current_event["Term Date Description"] += " " + line

        if current_event and current_event["Term Date Description"].strip():
            self.calendar_rows.append(current_event)

    def closed(self, reason):

        df = pd.DataFrame(self.calendar_rows)

        # Final safety filter
        df = df[~df["Term Date Description"].str.contains(
            "Information Disclaimer", case=False, na=False
        )]

        save_df(df, self.institution_id, "calendar")

        print(f"✅ Final calendar count: {len(df)}")
