import json
import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class NewyorkSpider(scrapy.Spider):
    name = "newyork"
    institution_id = 258443991291291608
    
    # Course page URLs
    course_page_url = "https://apps.nyit.edu/course-search/"
    course_details_url = "https://apps.nyit.edu/course-search/course-class-details"

    # Directory API URL
    directory_api_url = (
        "https://site.nyit.edu/directory/"
    )

    # Academic calendar page URL
    calendar_url = "https://www.nyit.edu/students/classes-schedules-and-transcripts/academic-calendar/" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
                yield scrapy.Request(url=self.course_page_url, callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_page_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_page_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_page_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
       

    # Parse methods UNCHANGED from your original
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
        - "Course Dates"                  : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        """
        NYIT COURSE SCRAPER
        - All courses are present in HTML
        - Pagination is client-side
        - Each <tr class="course_data"> = one course
        - Clicking row triggers POST to course-class-details
        """
        rows = []
        # Extract CSRF token from page <head> (required for POST requests)
        csrf_token = response.xpath("//meta[@name='csrf-token']/@content").get()
        # Select all course rows already loaded in HTML
        course_rows = response.xpath(
            "//tr[contains(concat(' ', normalize-space(@class), ' '), ' course_data ')]"
        )
    
        self.logger.info(f"[NYIT] Found {len(course_rows)} course rows in HTML")
        
        # Loop through each course row
        for idx, tr in enumerate(course_rows, start=1):
            class_id = tr.attrib.get("data-classid") or tr.attrib.get("id")
            subject = tr.attrib.get("data-subject")
            catalog = tr.attrib.get("data-catalognbr")
            term = tr.attrib.get("data-term")
            # Extract course title from table cell
            course_title = tr.xpath("./td[4]/text()").get()
            course_title = (course_title or "").strip()
    
            if not class_id:
                self.logger.warning(f"[NYIT] Skipping row {idx} due to missing class_id")
                continue
    
            self.logger.info(
                f"[NYIT] ({idx}/{len(course_rows)}) Fetching details for {subject} {catalog} | class_id={class_id}"
            )
            # Build POST payload exactly as sent by the browser
            payload = {
                "f_data_class_id": class_id,
                "f_data_term": term,
                "f_data_subject": subject or "",
                "f_data_subject_descr": "",
                "f_data_catalog_number": catalog or "",
                "f_data_view": "desktop",
                "f_days": [],
                "f_start_time": "",
                "f_end_time": "",
                "f_instructor": "",
                "f_instruction_mode": "",
            }
            # Required AJAX headers
            headers = {
                "Content-Type": "application/json",
                "X-Requested-With": "XMLHttpRequest",
            }
            
            # Attach CSRF token header
            if csrf_token:
                headers["X-CSRF-TOKEN"] = csrf_token
            
            #  POST request to fetch section-level details
            detail_response = yield scrapy.Request(
                url=self.course_details_url,
                method="POST",
                headers=headers,
                body=json.dumps(payload),
                dont_filter=True,
            )
            
            # Safely parse JSON response
            try:
                detail_json = detail_response.json()
            except Exception:
                self.logger.error(f"[NYIT] Failed to parse JSON for class_id={class_id}")
                continue
            
            # Extract section rows from JSON response
            table_rows = detail_json.get("table", [])
            self.logger.info(
                f"[NYIT] class_id={class_id} | sections returned={len(table_rows)}"
            )
            
            # Each entry in table_rows represents one course section
            for sec in table_rows:
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_page_url,
                    "Course Name": (
                        f"{subject} {catalog} {course_title}"
                        if course_title
                        else f"{subject} {catalog}"
                    ),
                    "Course Description": sec.get("DESCRLONG", ""),
                    "Class Number": sec.get("CLASS_NBR"),
                    "Section": sec.get("CLASS_SECTION"),
                    "Instructor": sec.get("NAME") or "TBA",
                    "Enrollment": f"{sec.get('ENRL_TOT') or 0} of {sec.get('ENRL_CAP') or 0}",
                    "Course Dates": f"{sec.get('START_DT')} - {sec.get('END_DT')}",
                    "Location": (
                        f"{sec.get('LOCATION')}, {sec.get('FACILITY_DESCR')}"
                        if sec.get("FACILITY_DESCR")
                        else sec.get("LOCATION")
                    ),
                    "Textbook/Course Materials": "",
                })
    
        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "course")

        
    @inline_requests
    def parse_directory(self, response):
        """
        # Parse directory using Scrapy response.
    
        # Output columns:
        # - Cengage Master Institution ID
        # - Source URL
        # - Name
        # - Title
        # - Email
        # - Phone Number
        # """

        """
        Code flow summary:
        1) Fetch paginated directory data from the AJAX endpoint using blocking requests.
        2) Continue requesting pages until no profiles are returned.
        3) Extract name, titles, email, and phone from each profile.
        4) Collect all rows locally during pagination.
        5) Save the complete dataset once after pagination ends.

        """
        rows = []
        # Page counter for AJAX pagination
        page = 1
        
        # Loop until the AJAX endpoint returns no profiles
        while True:
            # Construct AJAX URL for the current page
            url = (
                f"https://apps.nyit.edu/directory/ajax.php"
                f"?page={page}&keyword=&school=&department=&emp_type=All"
            )
    
            self.logger.info(f"[parse_directory] Fetching page {page}")
            resp = yield scrapy.Request(url=url, dont_filter=True)
            
            # Each Profile div represents one employee
            profiles = resp.xpath('//div[@class="Profile"]')

            if not profiles:
                self.logger.info(f"[parse_directory] No profiles found on page {page}, stopping")
                break
            
            # Loop through each employee profile
            for profile in profiles:
                name = profile.xpath('.//h3/text()').get(default='').strip()
                title_blocks = profile.xpath(
                './/p[contains(@class,"primary") or contains(@class,"secondary") or contains(@class,"tertiary")]'
                ) 
                # Collect cleaned title strings
                titles = []

                for block in title_blocks:
                    parts = [
                        t.strip()
                        for t in block.xpath('.//text()').getall()
                        if t.strip()
                    ]
                    block_text = ', '.join(parts)

                    if block_text:
                        titles.append(block_text)
    
                title = ' | '.join(titles)
    
                email = profile.xpath('.//a[starts-with(@href,"mailto:")]/text()').get(default='').strip()
                phone = profile.xpath('.//a[starts-with(@href,"tel:")]/text()').get(default='').strip()
    
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.directory_api_url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                })
            # Move to the next page
            page += 1
    
        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "campus")

    
    def parse_calendar(self, response):
        """
        Parse calendar using Scrapy response.

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Term Name
        - Term Date
        - Term Date Description
        """

        """
        1. Select Academic Year 2025–2026 section.
        2. Loop through each term and its date blocks.
        3. Extract dates and descriptions (handle bullet and non-bullet cases).
        4. Normalize date dashes for CSV/Excel safety.
        5. Collect rows and save once at the end.
        """
        calendar_rows = []

        # Select only the Academic Year 2025–2026 section
        year_section = response.xpath(
            '//h2[@id="h-academic-year-2025-2026"]/ancestor::div[contains(@class,"Section")]'
        )

        if not year_section:
            self.logger.error("Academic Year 2025–2026 section not found")
            return

        # Each Accordion__item represents one academic term
        accordion_items = year_section.xpath(
            './/div[contains(@class,"Accordion__item")]'
        )

        for item in accordion_items:
            term_name = (
                item.xpath('.//h3//span/text()')
                .get(default='')
                .split('(')[0]
                .strip()
            )

            if not term_name:
                continue

            # Each wp-block-columns block represents a date row
            blocks = item.xpath('.//div[contains(@class,"wp-block-columns")]')

            for block in blocks:
                # MAIN BLOCK DATE (LEFT COLUMN)
                block_date = block.xpath('.//p/strong/text()').get()
                block_date = block_date.strip() if block_date else None

                if not block_date:
                    continue

                # normalize unicode dashes 
                block_date = block_date.replace("–", "-").replace("—", "-")

                # Check if the block contains bullet-based sub events
                bullet_items = block.xpath('.//ul/li')

                if bullet_items:
                    for li in bullet_items:
                        li_text = ' '.join(
                            li.xpath('.//text()').getall()
                        ).strip()

                        if not li_text:
                            continue

                        if ':' in li_text:
                            date_part, desc_part = li_text.split(':', 1)
                            term_date = date_part.strip()
                            term_desc = desc_part.strip()

                        else:
                            term_date = block_date
                            term_desc = li_text

                        # normalize unicode dashes in bullet dates
                        term_date = term_date.replace("–", "-").replace("—", "-")
                        calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": term_name,
                            "Term Date": term_date,
                            "Term Date Description": term_desc,
                        })

                else:
                    # NORMAL ROW (NO BULLETS)
                    desc_text = ' '.join(
                        block.xpath('.//p[not(strong)]//text()').getall()
                    ).strip()

                    if not desc_text:
                        continue

                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": block_date,
                        "Term Date Description": desc_text,
                    })

        df = pd.DataFrame(calendar_rows)
        save_df(df, self.institution_id, "calendar")
