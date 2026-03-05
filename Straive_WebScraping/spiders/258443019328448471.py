import re                     
import scrapy             
import pdfplumber
import pandas as pd         
from ..utils import *       
from io import BytesIO    
from urllib.parse import quote    


class MsjSpider(scrapy.Spider):
    # Spider name used by Scrapy
    name = "msj"

    # Unique institution ID used for all datasets
    institution_id = 258443019328448471 

    # Base URL for course listings
    course_url =  "https://webreg.msj.edu/cgi-bin/public/crscat/SJschd.cgi?sess=S4&yr=2026&cat=GR"

    # Faculty / Staff directory page
    directory_url = 'https://www.msj.edu/faculty-and-staff-directory/index.html'

    # Academic calendar PDF URLs
    calendar_url = [
        "https://www.msj.edu/academics/registrar/academic-calendar/Revised-27-28-Academic-Calendar_Approved.pdf.2.pdf",
        "https://www.msj.edu/academics/registrar/academic-calendar/Revised-26-27-calendar.docx-approved-by-Cabinet.docx.3.pdf"
    ]

    # Initialize storage lists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Store scraped directory data
        self.directory_rows = []

        # Store scraped calendar data
        self.calendar_rows = []

        # Store scraped course data
        self.course_rows = []

    # Entry Point – Select Scrape Mode
    def start_requests(self):
        # why using playwright (dynamic content on course page)

        # Read scrape mode from settings (course / directory / calendar / combinations)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode Execution ----
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            # Loop through all calendar PDFs
            for url in self.calendar_url:
                yield scrapy.Request(url=url, callback=self.parse_calendar)

        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

    # Helper method to clean string values
    def clean(self, value):
        return (value or "").strip()

    def parse_course(self, response):
        """
        Scrapes all course schedule data.
        Page is dynamically rendered.
        """
        # Extract department values from dropdown
        values = response.xpath(
            '//td[@class="msjdatalight"]//select[@name="crs_area"]//option/@value'
        ).getall()

        # Skip first option (usually "Select")
        for value in values[1:]:
            dept_link = (
                f'https://webreg.msj.edu/cgi-bin/public/crscat/'
                f'SJschd.cgi?sess=S2&yr=2026&cat=UG26&cl_time=&command=Update+List'
                f'&cl_stat=&cl_form=&cl_del=&cl_type=&crs_area={value}'
            )

            # Debug logging
            print(f"department link -----> {dept_link}")

            # Custom headers to mimic browser behavior
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Referer': 'https://webreg.msj.edu/cgi-bin/public/crscat/SJschd.cgi?sess=S4&yr=2026&cat=GR',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            }

            # Request department-specific course list
            yield scrapy.Request(
                url=dept_link,
                headers=headers,
                callback=self.parse_department_courses,
                dont_filter=True
            )

    def parse_department_courses(self, response):
        # Extract links to individual course pages
        courses_links = response.xpath(
            '//tr/td[1]/a[@class="msjlink"]/@href'
        ).getall()

        for course_link in courses_links:
            # Extract textbook link from course row
            textbook_url = response.xpath(
                '//tr/td[11]/a[@class="msjlink"]/@href'
            ).get('')

            # Build absolute course URL
            course_url = f"https://webreg.msj.edu/cgi-bin/public/crscat/{course_link}"

            # Debug logging
            print(f"course link -----> {course_url}")

            # Request individual course details
            yield scrapy.Request(
                url=course_url,
                callback=self.parse_course_details,
                dont_filter=True,
                cb_kwargs={'textbook_url': textbook_url}
            )

    def parse_course_details(self, response, textbook_url):
        # Extract course number
        course_num = response.xpath(
            '//p[@class="msjsubheader"]//text()'
        ).get('').strip()

        # Extract title parts
        titles = response.xpath(
            '//p[@class="msjsubheader"]//text()'
        ).getall()

        # Combine title and subtitle
        title = f"{titles[0].strip()}- {titles[1].strip()}"

        # Extract course description
        description = response.xpath(
            '//tr[@class="msjmsg"]/td[b[contains(text(),"Description")]]/text()'
        ).get('')

        # Iterate through section rows
        for row in response.xpath('//tr[contains(@class,"msjdata")]'):
            instructor = row.xpath('./td[3]/text()').get(default="").strip()

            # ❌ Skip Crosslisted rows
            if "Crosslisted" in instructor:
                continue

            # ❌ Skip invalid instructor placeholders
            if re.fullmatch(r'[-MTWRFS]+', instructor):
                continue

            # Extract section number
            section = row.xpath('./td[2]/text()').get(default="").strip()

            # Extract course dates
            dates = row.xpath('./td[7]/text()').get(default="").strip()

            # Append course record
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": re.sub(r'\s+', ' ', title),
                "Course Description": re.sub(r'\s+', ' ', description),
                "Class Number": course_num,
                "Section": section,
                "Instructor": instructor,
                "Enrollment": '',
                "Course Dates": dates,
                "Location": '',
                "Textbook/Course Materials": textbook_url,
            })

        # Save course data after processing
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Scrapes faculty and staff directory profiles.
        """
        # Select visible faculty rows
        rows = response.xpath(
            '//table[contains(@class,"facultyTable")]//tbody/tr[not(contains(@class,"hide"))]'
        )

        for row in rows:
            # Extract profile fields
            name = row.xpath('.//h3/a/text()').get('')
            url = row.xpath('.//h3/a/@href').get('')
            source_url = f'https://www.msj.edu/faculty-and-staff-directory/{url}'
            title = row.xpath('.//span[@class="title"]/text()').get('')
            deperment = row.xpath('.//span[@class="department"]/text()').get('')
            email = row.xpath('.//span[@class="email"]/a/text()').get('')
            phone = row.xpath('.//span[@class="phone"]/a/text()').get('')

            # Clean extracted values
            name = re.sub(r'\s+', ' ', name).strip() if name else ""
            title = re.sub(r'\s+', ' ', title).strip() if title else ""
            department = re.sub(r'\s+', ' ', deperment).strip() if deperment else ""
            email = email.strip() if email else ""
            phone = phone.strip() if phone else ""

            # Append directory record
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": source_url,
                "Name": name,
                "Title": f'{title}, {department}' if department else title,
                "Email": email,
                "Phone Number": phone,
            })

        # Save directory data
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self, response):
        # Convert PDF bytes into a stream
        pdf_bytes = BytesIO(response.body)

        current_term = None
        previous_row = None

        # Regex to detect semester headers
        term_pattern = re.compile(r"(Fall Semester|Second Semester|Summer Semester).*?\(S\d+\)")

        # Regex to detect date rows
        date_start_pattern = re.compile(
            r"^([A-Za-z]+\s\d{1,2}(?:\s*[–-]\s*\d{1,2})?,\s\d{4})[, ]+(.*)"
        )

        # Regex to remove weekday names
        DAY_CLEAN_PATTERN = re.compile(
            r"\b(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
            r"(?:\s*(?:through|–|-)\s*"
            r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday))?\b",
            re.IGNORECASE
        )

        # Open and iterate through PDF pages
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                for line in text.split("\n"):
                    line = line.strip()
                    if not line:
                        continue

                    # Detect semester header
                    term_match = term_pattern.search(line)
                    if term_match:
                        current_term = term_match.group()
                        previous_row = None
                        continue

                    # Detect date rows
                    m_date = date_start_pattern.match(line)
                    if m_date and current_term:
                        date, event = m_date.groups()

                        # Remove weekday references
                        event = DAY_CLEAN_PATTERN.sub("", event).strip(" ,-")

                        previous_row = {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": current_term,
                            "Term Date": date.strip(),
                            "Term Date Description": event
                        }

                        self.calendar_rows.append(previous_row)
                        continue

                    # 🔥 Continuation lines for multi-line events
                    if previous_row:
                        clean_line = DAY_CLEAN_PATTERN.sub("", line).strip(" ,-")
                        previous_row["Term Date Description"] += " " + clean_line

        # Save calendar data once
        if self.calendar_rows:
            calendar_df = pd.DataFrame(self.calendar_rows)
            save_df(calendar_df, self.institution_id, "calendar")
