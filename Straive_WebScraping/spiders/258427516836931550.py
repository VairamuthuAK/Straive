import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class CECILSpiderMixed(scrapy.Spider):
    name = "cecil"
    institution_id = 258427516836931550

    # course_links 
    course_url = "http://legacy.cecil.edu/course-search/course-search.asp"

    course_api_url="http://legacy.cecil.edu/course-search/course-search-results.asp"

    # Employee directory API endpoint
    directory_api_url = (
        "https://www.cecil.edu/about-us/leadership-staff"
    )

    # Academic calendar page URL
    calendar_url = "https://www.cecil.edu/programs-courses/academics/calendars" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(
                url=self.course_api_url,
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                body=(
                    "selectdays=no"
                    "&coursecode="
                    "&crstitlesub="
                    "&instructorlastname="
                    "&credit=credit"
                    "&coursestartdate="
                    "&courseenddate="
                    "&coursestarttime="
                    "&courseendtime="
                    "&page=Search"
                ),
                callback=self.parse_course,
                dont_filter=True,
            )

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(
                url=self.course_api_url,
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                body=(
                    "selectdays=no"
                    "&coursecode="
                    "&crstitlesub="
                    "&instructorlastname="
                    "&credit=credit"
                    "&coursestartdate="
                    "&courseenddate="
                    "&coursestarttime="
                    "&courseendtime="
                    "&page=Search"
                ),
                callback=self.parse_course,
                dont_filter=True,
            )
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(
                url=self.course_api_url,
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                body=(
                    "selectdays=no"
                    "&coursecode="
                    "&crstitlesub="
                    "&instructorlastname="
                    "&credit=credit"
                    "&coursestartdate="
                    "&courseenddate="
                    "&coursestarttime="
                    "&courseendtime="
                    "&page=Search"
                ),
                callback=self.parse_course,
                dont_filter=True,
            )
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(
                url=self.course_api_url,
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                body=(
                    "selectdays=no"
                    "&coursecode="
                    "&crstitlesub="
                    "&instructorlastname="
                    "&credit=credit"
                    "&coursestartdate="
                    "&courseenddate="
                    "&coursestarttime="
                    "&courseendtime="
                    "&page=Search"
                ),
                callback=self.parse_course,
                dont_filter=True,
            )
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

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
        Code flow summary:
        - Receive first search result page (POST response)
        - Loop through paginated result pages
        - For each course row, open its detail page (blocking)
        - Extract summary + detail data
        - Append rows locally
        - Save once at the end
        """

        rows = []
        page_no = 1
        # Holds the current result page
        page_response = response

        # PAGINATION LOOP
        while True:
            self.logger.info(f"Processing result page {page_no}")
            # Each <tr class="resultsdata"> = one course section
            course_rows = page_response.xpath("//tr[contains(@class,'resultsdata')]")
            self.logger.debug(f"Found {len(course_rows)} rows on page {page_no}")

            for row in course_rows:
                section_text = row.xpath(
                    "normalize-space(./td[1]//a/text())"
                ).get()

                # ---------- MULTIPLE DATES (COMMA SEPARATED) ----------
                date_parts = row.xpath("./td[4]/text()").getall()
                course_dates = ", ".join(
                    d.strip() for d in date_parts if d.strip()
                )

                instructor = row.xpath(
                    "normalize-space(./td[8]/text())"
                ).get()

                seats_text = row.xpath(
                    "normalize-space(./td[7]/text())"
                ).get()

                enrollment = ""

                if seats_text and "/" in seats_text:
                    left, right = seats_text.split("/", 1)
                    enrollment = f"{left.strip()} of {right.strip()}"

                #SECTION 
                section = section_text or ""

                self.logger.debug(
                    f"Section raw='{section_text}' → Saved Section='{section}'"
                )
                # DETAIL PAGE REQUEST 
                detail_href = row.xpath("./td[1]//a/@href").get()

                if not detail_href:
                    self.logger.debug("Missing detail URL. Skipping row.")
                    continue

                detail_url = page_response.urljoin(detail_href)
                self.logger.debug(f"Fetching detail page: {detail_url}")

                # DETAIL PAGE (BLOCKING)
                detail_response = yield scrapy.Request(
                    detail_url,
                    dont_filter=True
                )

                # COURSE NAME
                header_text = detail_response.xpath(
                    "normalize-space(//td[contains(text(),'-')][1])"
                ).get()

                course_name = ""

                if header_text:
                    course_name = " ".join(header_text.split())

                # COURSE DESCRIPTION
                desc_texts = detail_response.xpath(
                    "//tr[td/hr]/following-sibling::tr[1]/td//text()"
                ).getall()

                course_description = " ".join(
                    t.strip()
                    for t in desc_texts
                    if t.strip()
                    and "View Syllabus" not in t
                )

                if not course_description:
                    self.logger.warning(
                        f"Description missing for section {section}"
                    )

                # LOCATION
                location = detail_response.xpath(
                    "normalize-space(//tr[contains(@class,'scheduledata')]/td[5]/text())"
                ).get() or ""

                # TEXTBOOK
                textbook_href = detail_response.xpath(
                    "//a[contains(text(),'See Textbook')]/@href"
                ).get()

                textbook = (
                    detail_response.urljoin(textbook_href)
                    if textbook_href else ""
                )

                # SAVE ROW
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_url,
                    "Course Name": course_name,
                    "Course Description": course_description,
                    "Class Number": "",
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": enrollment,
                    "Course Dates": course_dates,
                    "Location": location,
                    "Textbook/Course Materials": textbook,
                })

            # NEXT PAGE
            next_href = page_response.xpath(
                "//a[contains(normalize-space(.),'Next')]/@href"
            ).get()

            if not next_href:
                self.logger.info("No Next button found. Pagination complete.")
                break

            next_url = page_response.urljoin(next_href)
            self.logger.info(f"Following Next page: {next_url}")

            page_response = yield scrapy.Request(
                next_url,
                dont_filter=True
            )
            page_no += 1

        # SAVE ONCE
        self.logger.info(f"Total rows collected: {len(rows)}")
        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "course")

        
    @inline_requests
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
        """
        Flow summary:
        - Parse leadership staff (President + VPs) from the landing page
        - Make one inline (blocking) request to the main directory page
        - Parse department-level phone directory
        - Combine both datasets and save once
        """

        rows = []
         # Source URL for leadership entries
        leadership_url = response.url

        # PART 1: LEADERSHIP STAFF FROM LANDING PAGE
        content = response.xpath(
            '//div[contains(concat(" ", normalize-space(@class), " "), " main-content ")]'
            '//div[contains(@class,"tm-article-content")]'
        )

        # President
        president_nodes = content.xpath(
            './/p[.//span[contains(@class,"uk-text-large")]]'
        )
        # Loop through each president node
        for p in president_nodes:
            name = p.xpath(
                './/span[contains(@class,"uk-text-large")][1]/text()'
            ).get()
            title = p.xpath(
                './/span[contains(@class,"uk-text-italic")][1]/text()'
            ).get()

            if not name or not title:
                continue

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": leadership_url,
                "Name": " ".join(name.split()),
                "Title": " ".join(title.split()),
                "Email": "",
                "Phone Number": "",
            })

        # President’s Staff (VPs)
        staff_nodes = content.xpath(
            './/ul[contains(concat(" ", normalize-space(@class), " "), " uk-list ")]//li/p'
        )
        # Loop through each staff node
        for p in staff_nodes:
            name = p.xpath(
                './/span[contains(@class,"uk-text-bold")]/text()'
            ).get()
            title = p.xpath(
                './/span[contains(@class,"uk-text-italic")]/text()'
            ).get()

            if not name or not title:
                continue

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": leadership_url,
                "Name": " ".join(name.split()),
                "Title": " ".join(title.split()),
                "Email": "",
                "Phone Number": "",
            })

        # PART 2: INLINE REQUEST TO DIRECTORY PAGE
        directory_url = "https://www.cecil.edu/directory"

        directory_response = yield scrapy.Request(
            directory_url,
            dont_filter=True
        )
        # Each table row represents one department entry
        table_rows = directory_response.xpath(
            '//table[contains(@class,"uk-table")]//tbody/tr'
        )
        
        # Loop through each table row
        for row in table_rows:
            cells = row.xpath('.//td')

            if len(cells) < 2:
                continue

            department = cells[0].xpath('string(.)').get()
            phone = cells[-1].xpath('string(.)').get()

            if not department:
                continue

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": directory_url,
                "Name": " ".join(department.split()),
                "Title": "",
                "Email": "",
                "Phone Number": " ".join(phone.split()) if phone else "",
            })

        # SAVE ONCE
        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")
  

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
        """
        Code flow summary:
        - Read academic calendar table rows from the page
        - Identify term header rows and track the current term
        - Extract date and description rows under each term
        - Normalize extracted text values
        - Collect all rows and save once at the end
        """

        calendar_rows = []

        source_url = response.url
        # Holds the current academic term header
        current_term = None
        
        # Select all rows from the academic calendar table
        table_rows = response.xpath(
            '//table[contains(@class, "uk-table")]//tr'
        )
        
        # Iterate through each table row
        for row in table_rows:
            term_header = row.xpath('string(.//th)').get()

            if term_header:
                current_term = " ".join(term_header.split())
                continue
            # Extract table cells for date rows
            cells = row.xpath('.//td')

            if len(cells) != 2:
                continue
            # First column = date or date range
            term_date = cells[0].xpath('string(.)').get()
            description = cells[1].xpath('string(.)').get()
            
            # Skip rows missing required data or term context
            if not term_date or not description or not current_term:
                continue

            term_date = " ".join(term_date.split())
            description = " ".join(description.split())
            term_date = term_date.replace("–", "-")

            # SAVE CALENDAR ROW
            calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": source_url,
                "Term Name": current_term,
                "Term Date": term_date,
                "Term Date Description": description,
            })

        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
