import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class NewPaltzSpider(scrapy.Spider):
    name = "newpaltz"
    institution_id = 258434659338184663
    
    # Custom settings to handle 429 and other transient errors
    custom_settings = {
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 20,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504, 522, 524, 408],
         "DOWNLOAD_DELAY": 3,
         "CONCURRENT_REQUESTS" : 5,
    }

    course_url = "https://www.newpaltz.edu/classes/"
        
    # Employee directory API endpoint
    directory_api_url = (
        "https://webapps.newpaltz.edu/directory/"
    )

    # Academic calendar page URL
    calendar_url = "https://catalog.newpaltz.edu/undergraduate/academic-calendar/" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

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
        Parse New Paltz course listings.
        - Use FULL TERM results page (term_part_code=1)
        - Group ALL rows by tr.@data-crn
        - Enrollment comes ONLY from the FIRST row of each CRN
        - Dates / instructors / locations are merged from all rows
        - Course Name + Description come ONLY from section page
        - Section page is visited ONCE per CRN
        - Save ONCE at the end
        """
        rows = []

        # Extract TERM URLs from landing page
        term_urls = response.xpath(
            '//div[@class="two-column row"]//a[contains(text(),"Schedule of Classes")]/@href'
        ).getall()

        # Loop through each academic term
        for term_relative_url in term_urls:
            # Build absolute term URL
            term_url = response.urljoin(term_relative_url)
            # Extract term code
            term_code = term_url.rstrip("/").split("/")[-1]
            # Load FULL TERM results page
            results_url = (
                f"https://schedule.newpaltz.edu/classes/{term_code}/results"
                f"?term_part_code=1"
            )

            results_response = yield scrapy.Request(results_url, dont_filter=True)
            # Select all meeting rows (each row = one meeting pattern)
            meeting_rows = results_response.xpath('//tr[@data-crn]')

            # GROUP ROWS BY CRN
            courses_by_crn = {}
            
            # loop meeting rows
            for row in meeting_rows:
                crn = row.xpath('./@data-crn').get()

                if not crn:
                    continue

                # Initialize ONCE per CRN (first row only)
                if crn not in courses_by_crn:
                    enrollment_parts = row.xpath(
                        './td[@data-label="Availability"]//text()'
                    ).getall()
                    
                    # Combine enrollment text fragments
                    enrollment_raw = "".join(
                        p.strip() for p in enrollment_parts if p.strip()
                    )

                    self.logger.info(
                        f"[ENROLLMENT] TERM={term_code} | CRN={crn} | Enrollment Raw='{enrollment_raw}'"
                    )

                    courses_by_crn[crn] = {
                        "meeting_rows": [],
                        "section_url": row.xpath(
                            './td[@data-label="CRN"]/a/@href'
                        ).get(),
                        "section": row.xpath(
                            'normalize-space(./td[@data-label="Section"]/text())'
                        ).get(),
                        "enrollment": enrollment_raw,
                    }
                # Append current row to the CRN's meeting rows
                courses_by_crn[crn]["meeting_rows"].append(row)

            # BUILD ONE OUTPUT ROW PER CRN
            for crn, course_data in courses_by_crn.items():
                collected_dates = []
                collected_locations = []
                collected_instructors = []

                # Merge data from all meeting rows
                for meeting_row in course_data["meeting_rows"]:
                    date_parts = meeting_row.xpath(
                        './td[@data-label="Dates"]//text()'
                    ).getall()
                    date_text = " ".join(p.strip() for p in date_parts if p.strip())

                    if date_text:
                        collected_dates.append(date_text)

                    location_parts = meeting_row.xpath(
                        './td[@data-label="Location"]//text()'
                    ).getall()
                    location_text = " ".join(p.strip() for p in location_parts if p.strip())

                    if location_text:
                        collected_locations.append(location_text)

                    instructor_parts = meeting_row.xpath(
                        './td[@data-label="Instructor"]//text()'
                    ).getall()

                    for inst in instructor_parts:
                        inst = inst.strip()

                        if inst and inst != ",":
                            collected_instructors.append(inst)

                # Remove duplicate dates/locations/instructors caused by multiple meeting rows
                def unique_preserve_order(values):
                    unique_values = []

                    for v in values:
                        if v not in unique_values:
                            unique_values.append(v)

                    return unique_values

                merged_dates = ", ".join(unique_preserve_order(collected_dates))
                merged_locations = ", ".join(unique_preserve_order(collected_locations))
                merged_instructors = ", ".join(unique_preserve_order(collected_instructors))

                # VISIT SECTION PAGE 
                course_name = ""
                course_description = ""
                textbook_url = ""
                section_page_url = course_data.get("section_url")
                
                # Only visit if URL exists
                if section_page_url:
                    section_page_url = results_response.urljoin(section_page_url)
                    section_response = yield scrapy.Request(
                        section_page_url, dont_filter=True
                    )

                    # COURSE NAME
                    course_name = section_response.xpath(
                        'normalize-space(//div[@id="dept-page-title"]/text())'
                    ).get() or ""

                    course_description = section_response.xpath(
                        'normalize-space(//div[@class="col-sm-8 col-xs-12"]/p[1])'
                    ).get() or ""

                    textbook_url = section_response.xpath(
                        '//div[contains(@class,"classes__section-details-links-container")]'
                        '//a[contains(text(),"Textbook")]/@href'
                    ).get() or ""

                # APPEND FINAL ROW
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": section_page_url,
                    "Course Name": course_name,
                    "Course Description": course_description,
                    "Class Number": crn,
                    "Section": course_data.get("section"),
                    "Instructor": merged_instructors,
                    "Enrollment": course_data.get("enrollment"),
                    "Course Dates": merged_dates,
                    "Location": merged_locations,
                    "Textbook/Course Materials": textbook_url
                })

        course_df = pd.DataFrame(rows)
        save_df(course_df, self.institution_id, "course")


    @inline_requests
    def parse_directory(self, response):
        """
        Parse New Paltz directory

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Name
        - Title            
        - Email
        - Phone Number
        """
        """
        Extract department IDs from the directory dropdown, request search results
        for each department, parse individual staff records, normalize titles and
        contact details, collect all rows locally, and save the directory data once.

        """

        rows = []
        # Extract  department entity_ids
        option_nodes = response.xpath(
            '//select[@name="entity_id"]/option[@value!=""]'
        )
        
        # Store unique department IDs
        entity_ids = []
        #loop options to get unique entity_ids
        for opt in option_nodes:
            entity_id = opt.xpath('./@value').get()

            if entity_id and entity_id not in entity_ids:
                entity_ids.append(entity_id)

        # Loop departments
        for entity_id in entity_ids:
            # Build department-specific search URL
            search_url = (
                "https://webapps.newpaltz.edu/directory/search"
                f"?first_name=&last_name=&phone=&entity_id={entity_id}"
            ) 
            # Request search results page for this department
            try:
                search_resp = yield scrapy.Request(
                    url=search_url,
                    dont_filter=True
                )

            except Exception as e:
                self.logger.error(
                    f"Directory request failed entity_id={entity_id}: {e}"
                )
                continue
            
            # Select valid staff record cards
            records = search_resp.xpath(
                '//div[@id="records"]//div[contains(@class,"well") '
                'and not(contains(@class,"fake-record"))]'
            )

            # Parse individual staff records
            for rec in records:
                # Extract staff name
                name = rec.xpath('normalize-space(.//h4)').get()

                if not name:
                    continue
                
                # Skip malformed or non-person entries
                if name.endswith(','):
                    continue
                
                if "fax machine" in name.lower():
                    continue

                # Extract title parts (bold text + following text)
                strong_part = rec.xpath('.//p/strong/text()').get()
                text_part = rec.xpath(
                    'normalize-space(.//p/strong/following-sibling::text()[1])'
                ).get()

                strong_part = strong_part.strip() if strong_part else ""
                text_part = text_part.strip() if text_part else ""
                
                # Combine title fragments into a single string
                if strong_part and text_part:
                    title = f"{strong_part}, {text_part}"
                else:
                    title = strong_part or text_part or ""

                # Skip rows that contain only department labels
                if title.startswith("Department:,"):
                    self.logger.debug(f"Skipping department-only row: {title}")
                    continue

                # Department
                department = rec.xpath(
                    './/strong[text()="Department:"]/following-sibling::text()[1]'
                ).get()
                department = department.strip() if department else ""

                # Merge Title + Department
                if department:
                    if title:
                        title = f"{title}, {department}"
                    else:
                        title = department

                # Phone
                phone = rec.xpath(
                    './/strong[text()="Phone:"]/following-sibling::text()[1]'
                ).get()
                phone = phone.strip() if phone else ""

                # Decode Cloudflare-protected email address
                email = ""
                cfemail = rec.xpath(
                    './/span[contains(@class,"__cf_email__")]/@data-cfemail'
                ).get()

                if cfemail:
                    try:
                        key = int(cfemail[:2], 16)
                        email = "".join(
                            chr(int(cfemail[i:i+2], 16) ^ key)
                            for i in range(2, len(cfemail), 2)
                        )
                    except Exception:
                        pass

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone
                })

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "campus")


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

        calendar_rows = []
        
        # main calendar content container
        container = response.xpath('//div[@id="textcontainer"]')
        
        # Loop through each term heading (e.g., Fall 2025)
        for h2 in container.xpath('.//h2'):
            # Extract clean term name
            term_name = h2.xpath('normalize-space(.)').get()
            table = h2.xpath('following-sibling::table[1]')

            if not table:
                continue
            # Store previous date for continuation rows
            last_term_date = "" 
            
            # Loop through each row in the term table
            for tr in table.xpath('.//tbody/tr'):
                term_date = tr.xpath('normalize-space(./td[1])').get()
                term_desc = tr.xpath('normalize-space(./td[2])').get()

                if not term_desc:
                    continue

                # Handle rows where date is omitted and continues from above
                if term_date:
                    last_term_date = term_date
                else:
                    term_date = last_term_date

                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": term_date or "",
                    "Term Date Description": term_desc,
                })

        # SAVE OUTPUT
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
