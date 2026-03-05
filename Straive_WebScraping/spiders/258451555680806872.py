import re
import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class MaineSpider(scrapy.Spider):
    name = "maine"
    institution_id = 258451555680806872

    # Employee directory API endpoint
    directory_api_url = (
        "https://www.uma.edu/directory/"
    )
    
    # Course search UI page (used to read available terms)
    course_url='https://www.uma.edu/academics/courseguide/'

    # Actual course search endpoint (term-based)
    course_api_url = (
        "https://www.uma.edu/academics/courseguide/"
        "?doClassSearch=1"
        "&keywords="
        "&career="
        "&subject="
        "&courseNumber="
        "&meetingsStartTimeStart="
        "&meetingsStartTimeEnd="
        "&meetingsEndTimeStart="
        "&meetingsEndTimeEnd="
        "&location="
        "&startDate="
        "&endDate="
        "&strm={strm}"
    )

    # Academic calendar page URL
    calendar_url = "https://www.uma.edu/academics/calendar/" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
    
        if mode == "course":
            yield scrapy.Request(url=self.course_url,callback=self.parse_course,dont_filter=True,)
    
        elif mode == "directory":
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory,dont_filter=True)
    
        elif mode == "calendar":
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar,)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

        else:
            # all three
            yield scrapy.Request(url=self.course_url,callback=self.parse_course,dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory,dont_filter=True,)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)
    
           
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
            Course scraping flow:
            1. Load course search UI page
            2. Extract all valid academic term codes from the term dropdown
            3. For each term :
            - Call the course search endpoint with the term code
            - Loop through each course
            - Loop through each section within the course
            - Create ONE row per section
            4. Collect all rows locally
            5. Save the final dataset once at the end
         """
        rows = []
    
        # Get valid terms from UI (ignore empty option)
        terms = [
            term_code
            for term_code in response.xpath('//select[@name="strm"]/option/@value').getall()
            if term_code.strip()
        ]

        self.logger.warning(f"TERMS FOUND = {terms}")
    
        # Loop through each term (blocking requests)
        for strm in terms:
            term_response = yield scrapy.Request(
                url=self.course_api_url.format(strm=strm),
                dont_filter=True,
            )
    
            term_name = term_response.xpath(
                f'//select[@name="strm"]/option[@value="{strm}"]/text()'
            ).get(default="UNKNOWN").strip()
    
            self.logger.warning(f"PROCESSING TERM = [{term_name}]")
            
            # Each course block
            courses = term_response.xpath('//div[contains(@class,"courseResultDiv")]')

            # Loop through each course shown for the selected term
            for course in courses:
                # Extract course title (e.g. "ENG 101 – English Composition")
                course_name = course.xpath(
                    './/h2[@class="courseTitle"]/text()'
                ).get(default="").strip()

                # Extract course description text
                course_desc = course.xpath(
                    './/div[@class="courseDescription"]/text()'
                ).get(default="").strip()
                # Each course can have multiple sections (ONE ROW PER SECTION)
                sections = course.xpath('.//div[@class="classResultDiv"]')
                
                # Loop through each section of the course
                for section in sections:
                    # Extract raw enrollment text (example: "12 of 25")
                    enrollment_raw = section.xpath(
                    'normalize-space(.//div[contains(concat(" ", normalize-space(@class), " "), " enrollment ")]'
                    '//span[@class="csAttr-value"])'
                    ).get("")
                    # Parse standardized enrollment format
                    match = re.search(r'\d+\s+of\s+\d+', enrollment_raw)
                    enrollment = match.group(0) if match else None
                    self.logger.warning(
                    f"ENROLLMENT CHECK | TERM={term_name} | "
                    f"RAW=[{enrollment_raw}] | PARSED=[{enrollment}]"
                    )
                    # Try extracting instructor names from clickable links
                    instructor_links = section.xpath(
                        './/div[contains(@class,"instructorList")]//a/text()'
                    ).getall()

                    if instructor_links:
                        instructor = ", ".join(i.strip() for i in instructor_links if i.strip())

                    else:
                        instructor = section.xpath(
                            'normalize-space(.//div[contains(@class,"instructorList")]'
                            '//span[@class="csAttr-value"])'
                        ).get("")
                    
                    # Build final row
                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": course_name,
                        "Course Description": course_desc,
                        "Class Number": section.xpath(
                            './/div[contains(@class,"classNumber")]//span[@class="csAttr-value"]/text()'
                        ).get(default="").strip(),
                        "Section": section.xpath(
                            './/div[contains(@class,"classSection")]//span[@class="csAttr-value"]/text()'
                        ).get(default="").strip(),
                        "Instructor": instructor,
                        "Enrollment": enrollment,
                        "Course Dates": section.xpath(
                            './/div[contains(@class,"classDateRange")]//span[@class="csAttr-value"]/text()'
                        ).get(default="").strip(),
                        "Location": section.xpath(
                            './/div[contains(@class,"locationDescription")]//span[@class="csAttr-value"]/text()'
                        ).get(default="").strip(),
                        "Textbook/Course Materials": section.xpath(
                            './/div[@class="textbookLookupServiceURL"]/a/@href'
                        ).get(default="").strip(),
                    })
    
        self.logger.warning(f"TOTAL COURSE ROWS = {len(rows)}")
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
        Directory scraping flow (blocking):

        1. Start from the first directory listing page
        2. Extract all profile links on the current page
        3. Visit each profile page (blocking) and collect data
        4. Move to the next listing page (blocking)
        5. Repeat until no next page exists
        6. Save all collected rows once at the end
        """
    
        rows = []
        
        # Pointer to the current listing page
        current_response = response
        
        # Loop until pagination ends
        while True:
            # 1. Extract profile links on current page
            profile_links = current_response.xpath(
                '//div[contains(@class,"s_item")]//a[h3]/@href'
            ).getall()
            
            # Visit each profile page
            for link in profile_links:
                profile_response = yield scrapy.Request(
                    url=current_response.urljoin(link),
                    dont_filter=True
                )
    
                name = profile_response.xpath(
                    '//h1[@class="entry-title"]/text()'
                ).get(default="").strip()
    
                title = profile_response.xpath(
                    '//tr[@class="title"]/td//text()'
                ).get(default="").strip()
    
                phone = profile_response.xpath(
                    '//tr[@class="telephone"]/td/text()'
                ).get(default="").strip()
    
                email = profile_response.xpath(
                    '//tr[@class="email"]//a/text()'
                ).get(default="").strip()
    
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": profile_response.url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                })
    
            # Look for the "Next" pagination link on the listing page
            next_page = current_response.xpath(
                '//a[contains(@class,"next")]/@href'
            ).get()
            
            # If no next page exists, exit the loop
            if not next_page:
                break
    
            # Load the next listing page (blocking) and continue the loop
            current_response = yield scrapy.Request(
                url=current_response.urljoin(next_page),
                dont_filter=True
            )
    
        # Save after last page
        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "campus")

    
    def parse_calendar(self, response):
        """
        Academic calendar scraping flow:

        - Extract term names from calendar tabs (Fall, Spring, Summer)
        - Extract corresponding tab content blocks in the same order
        - Pair each term name with its content block using index
        - Read calendar table rows inside each term
        - Create ONE row per calendar event
        - Save all rows once at the end
        """
    
        rows = []

        # Fall 2025 | Spring 2026 | Summer 2026
        term_names = response.xpath(
            '//ul[contains(@class,"kt-tabs-title-list")]//span[@class="kt-title-text"]/text()'
        ).getall()
    
        term_names = [t.strip() for t in term_names if t.strip()]
    
        self.logger.info("TERM NAMES FOUND (ORDERED): %s", term_names)
    
        # Extract TAB CONTENT BLOCKS (same order)
        term_blocks = response.xpath(
            '//div[contains(@class,"wp-block-kadence-tab") and contains(@class,"kt-inner-tab")]'
        )
    
        self.logger.info("TERM BLOCKS FOUND: %d", len(term_blocks))
    
        # Pair term name with its content using index
        for idx, block in enumerate(term_blocks):
            # Current term name (mapped by order)
            term_name = term_names[idx]
            self.logger.info("PROCESSING TERM: %s", term_name)
    
            # STEP 4: Extract all calendar rows for this term
            for row in block.xpath('.//table//tbody/tr'):
                cells = row.xpath('./td')
                # Extract full text (handles <br> and nested tags)
                event = cells[0].xpath('string(.)').get("").strip()
                date = cells[1].xpath('string(.)').get("").strip()
    
                if not event and not date:
                    continue
    
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": date,
                    "Term Date Description": event,
                })
        
        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "calendar")
    