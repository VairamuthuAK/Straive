import json
import scrapy
import pandas as pd
from ..utils import *
import json
import html
from inline_requests import inline_requests


class UsiouxSpider(scrapy.Spider):
    name = "usioux_falls"
    institution_id = 258442886415149008

    # Course Endpoint
    course_page_url = "https://www.usiouxfalls.edu/about/offices-and-services/registrar/course-offerings"
    
    # URL-encoded JSON configuration for dynamically rendering a Sprig component (e.g., course listings).
    sprig_config = (
        "71614386a7b1556dc5928c4a3b47fdee873be81c7dc9ad5d6d6fa9e108e71a43"
        "%7B%22id%22%3A%22component-hlxfma%22%2C%22siteId%22%3A1%2C"
        "%22template%22%3A%22_includes%5C%2Fsprig%5C%2FcoursesFullListing%22%7D"
    )
    
    # Employee directory API endpoint
    directory_page_url="https://www.usiouxfalls.edu/directory"

    calendar_url = "https://www.usiouxfalls.edu/academics/academic-calendar"

    directory_page_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.usiouxfalls.edu/",
        }
    
    # Headers for employee directory request
    calendar_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Referer": "https://www.usiouxfalls.edu/",
        }
    
    course_page_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html",
        "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        }


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_page_url,headers=self.course_page_headers,callback=self.parse_course,dont_filter=True, meta={"cookiejar": 1})

        elif mode == 'directory':
        # Trigger with the public directory PAGE using browser-like headers
            yield scrapy.Request(url=self.directory_page_url,headers=self.directory_page_headers,callback=self.parse_directory,dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar,headers=self.calendar_headers,dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_page_url, callback=self.parse_course,headers=self.course_page_headers, dont_filter=True, meta={"cookiejar": 1})
            yield scrapy.Request(url=self.directory_page_url,headers=self.directory_page_headers, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_page_url,headers=self.course_page_headers, callback=self.parse_course, dont_filter=True,meta={"cookiejar": 1})
            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar,dont_filter=True)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers, callback=self.parse_calendar,dont_filter=True)
            yield scrapy.Request(url=self.directory_page_url,headers=self.directory_page_headers, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_page_url,headers=self.course_page_headers,callback=self.parse_course,dont_filter=True, meta={"cookiejar": 1},)
            yield scrapy.Request(url=self.directory_page_url,headers=self.directory_page_headers,callback=self.parse_directory,dont_filter=True)
            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers ,callback=self.parse_calendar,dont_filter=True)
       

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
        Scrapes courses using Craft CMS Sprig AJAX pagination.

        Flow:
        1. Start at offset 0
        2. Call Sprig endpoint
        3. Parse course blocks
        4. Increase offset
        5. Stop when no blocks returned
        """

        course_rows = []
        offset = 0
        page_size = 50

        # Headers to simulate a real browser HTMX (Sprig) request
        course_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/143.0.0.0 Safari/537.36"
            ),
            "Accept": "*/*",
            "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "HX-Request": "true",
            "HX-Current-URL": self.course_page_url,
            "HX-Target": "courses-listing-filter",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.course_page_url,
        }
        
        # Keep requesting more data until Sprig returns nothing
        while True:
            self.logger.info("Sprig offset=%s", offset)
            # Sprig AJAX endpoint that returns course HTML
            sprig_url = (
                "https://www.usiouxfalls.edu/index.php"
                "?p=actions/sprig-core/components/render"
                "&program="
                "&year="
                "&session="
                "&department="
                "&startDate="
                "&endDate="
                f"&offset={offset}"
                f"&sprig%3Aconfig={self.sprig_config}"
            )
            #AJAX request to Sprig
            sprig_response = yield scrapy.Request(
                url=sprig_url,
                headers=course_headers,
                dont_filter=True,
                meta={"cookiejar": response.meta["cookiejar"]},
            )

            self.logger.info(
                "Sprig response status=%s", sprig_response.status
            )
            
            # Each course appears inside a course accordion block
            course_blocks = sprig_response.xpath(
                '//div[contains(@class,"course-accordion")]'
            )

            # STOP CONDITION (Load More hidden)
            if not course_blocks:
                self.logger.info("No more courses, stopping pagination")
                break
            
            # Parse each course block
            for course in course_blocks:
                course_code = course.xpath(
                    'normalize-space(.//div[@class="course-meta"]/span[1])'
                ).get()
                course_title = course.xpath(
                    'normalize-space(.//h4)'
                ).get()
                course_name = " ".join(
                    part for part in [course_code, course_title] if part
                )
                # Extract section and instructor info
                meta_text = course.xpath(
                    'normalize-space(.//div[@class="course-meta"]/span[2])'
                ).get()

                section = ""
                instructor = ""

                if meta_text:
                    # example: (3.00 hrs · Sec: A · Van Kalsbeek)
                    meta_text = meta_text.strip("()")
                    parts = [p.strip() for p in meta_text.split("·")]

                    for part in parts:

                        if part.lower().startswith("sec"):
                            section = part.replace("Sec:", "").strip()

                        elif "hrs" not in part.lower():
                            instructor = part

                # DATES + LOCATION
                course_dates = course.xpath(
                    'normalize-space(.//div[@class="course-dates"])'
                ).get()

                if course_dates:
                    course_dates = course_dates.replace("–", "-").replace("—", "-")

                location = course.xpath(
                    'normalize-space(.//div[@class="course-location"])'
                ).get()

                # DESCRIPTION
                course_description = course.xpath(
                    'normalize-space(.//div[contains(@class,"accordion-content")]//p)'
                ).get()

                # APPEND ROW 
                course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_page_url,
                    "Course Name": course_name,
                    "Course Description": course_description,
                    "Class Number": course_code,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": "",
                    "Course Dates": course_dates,
                    "Location": location,
                    "Textbook/Course Materials": "",
                })

            # Increase offset for next page
            offset += page_size

        course_df = pd.DataFrame(course_rows)
        save_df(course_df, self.institution_id, "course")


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
        Code flow summary:
        - Start from directory landing page
        - Collect staff data from current page
        - Detect next page using HTMX data-hx-vals
        - Request next page in blocking mode
        - Repeat until no next page
        - Save once at the end
        """

        rows = [] 
        # current page                    
        page_response = response      
        page_no = 1

        while True:
            self.logger.info(f"Processing directory page {page_no}")
            # Each person is wrapped in <article class="person">
            people = page_response.xpath("//article[contains(@class,'person')]")
            self.logger.info(f"People found: {len(people)}")

            # Extract data for each person
            for person in people:
                name = person.xpath(".//h3/text()").get(default="").strip()
                title = person.xpath(".//p/text()").get(default="").strip()
                email = person.xpath(
                    ".//a[starts-with(@href,'mailto:')]/text()"
                ).get(default="").strip()
                phone = person.xpath(
                    ".//a[starts-with(@href,'tel:')]/text()"
                ).get(default="").strip()

                if not name:
                    continue

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.directory_page_url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                })

            # NEXT PAGE (HTMX)
            next_vals = page_response.xpath(
                "//button[contains(@class,'next') and not(@disabled)]/@data-hx-vals"
            ).get()

            if not next_vals:
                self.logger.info("No next page found. Pagination complete.")
                break

            try:
                data = json.loads(html.unescape(next_vals))
                next_page = data.get("page")

            except Exception:
                next_page = None

            if not next_page:
                self.logger.info("Next page value missing. Stopping.")
                break

            next_url = f"{self.directory_page_url}?page={next_page}"
            self.logger.info(f"Fetching next page: {next_url}")
            # Request next page 
            page_response = yield scrapy.Request(
                url=next_url,
                headers=self.directory_page_headers,
                dont_filter=True
            )
            page_no += 1

        self.logger.info(f"TOTAL DIRECTORY RECORDS: {len(rows)}")

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
        Code flow summary:
        - Load academic calendar page
        - Iterate through each calendar section header
        - Locate accordion blocks belonging to that header
        - Extract term name from accordion button text
        - Loop through table rows inside each accordion
        - Capture event description and date
        - Append rows locally
        - Save all calendar records once at the end
        """

        calendar_rows = []
        source_url = response.url
        # Each academic calendar section header
        calendar_headers = response.xpath("//div[@class='block-content']/h2")

        for header in calendar_headers:
            # All accordion blocks under this header
            accordions = header.xpath(
                "following::div[contains(@class,'block-accordion')]"
                "[preceding::div[@class='block-content'][1]/h2 = $h]",
                h=header.xpath("normalize-space(.)").get()
            )

            for accordion in accordions:
                # Raw term text from accordion button
                raw_term = accordion.xpath(
                    "normalize-space(.//button[contains(@class,'accordion-button')])"
                ).get()

                if not raw_term:
                    continue

                #  Remove date range in brackets
                term_name = raw_term.split("(")[0].strip()

                # Table rows
                rows = accordion.xpath(".//table/tbody/tr")
                
                # Extract each event + date
                for row in rows:
                    event = row.xpath("normalize-space(./td[1])").get()
                    date = row.xpath("normalize-space(./td[2])").get()

                    if not event and not date:
                        continue

                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": source_url,
                        "Term Name": term_name,
                        "Term Date": date or "",
                        "Term Date Description": event or ""
                    })

        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
