from io import StringIO
import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class EndicottSpider(scrapy.Spider):
    name = "endicott"
    institution_id = 258424099754239965

    # Course rows
    course_rows = []
    # Course urls
    course_csv_urls = [
    "https://docs.google.com/spreadsheets/d/1nSqb7ZJo_mii04KncZMT2v7L-gv_kxsYP7rt4vsOrbo/export?format=csv&gid=1257523167",
    "https://docs.google.com/spreadsheets/d/1tW_Lm0b6hCJ0PSLS07RxwQDZCfnDgMSCQ1a9bptEqn8/export?format=csv&gid=557722939"
    ]
    
    # Employee directory API endpoint
    directory_page_url = "https://www.endicott.edu/about/faculty-staff-directory"
    directory_api_url = "https://www.endicott.edu/api/sitecore/FacultyDirectory/Search"

 
    # Academic calendar page URL
    calendar_url = "https://www.endicott.edu/academics/academic-resources-support/academic-calendar" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == "course":
            for url in self.course_csv_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_course,
                    dont_filter=True,
                )

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_page_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            for url in self.course_csv_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_course,
                    dont_filter=True,
                )
            yield scrapy.Request(url=self.directory_page_url, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            for url in self.course_csv_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_course,
                    dont_filter=True,
                )
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_page_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            for url in self.course_csv_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_course,
                    dont_filter=True,
                )
            yield scrapy.Request(url=self.directory_page_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

    def parse_course(self, response):
        rows=[]
        # read CSV with NO assumed headers
        df = pd.read_csv(StringIO(response.text), header=None)
        # Build a map: header_name -> column_index
        header_map = {}
        for i in range(len(df.columns)):
            header = str(df.iloc[1, i]).strip().lower()
            if header:
                header_map[header] = i
        # Helper Function
        def get_val(row, header_name):
            idx = header_map.get(header_name)
            if idx is not None and idx < len(row) and pd.notna(row.iloc[idx]):
                return str(row.iloc[idx]).strip()
            return ""
        # Course records begin after the header row (row index 2 onward)
        for i in range(2, len(df)):
            row = df.iloc[i]
            # skip empty rows
            if row.isna().all():
                continue
            # Class Number
            class_number = get_val(row, "course subject abbreviation & number")
            if not class_number:
                continue
            # instructors (comma-separated)
            raw_ins = get_val(row, "all instructors")
            instructor = (
                raw_ins.replace("\n", ", ")
                    .replace(";", ",")
                    .replace("\t", " ")
            )
            instructor = ", ".join(
                p.strip() for p in instructor.split(",") if p.strip()
            )
            # dates
            start_date = get_val(row, "section start date")
            end_date = get_val(row, "section end date")
            # location
            location = get_val(row, "locations")
            # ENROLLMENT FROM Enrolled/Capacity
            enrollment_raw = get_val(row, "enrolled/capacity")
            enrollment = enrollment_raw.replace("/", " of ") if enrollment_raw else ""
            
            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": f"{class_number} {get_val(row, 'course section title')}".strip(),
                "Course Description": "",
                "Class Number": class_number,
                "Section": get_val(row, "section number"),
                "Instructor": instructor,
                "Enrollment": enrollment,
                "Course Dates": f"{start_date} - {end_date}".strip(" -"),
                "Location": location,
                "Textbook/Course Materials": "",
            })

        self.course_rows.extend(rows)
        
        
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

        rows = []

        # Required itemid from page
        item_id = response.xpath('//input[@id="ItemId"]/@value').get()

        # Extract all department filter values
        departments = response.xpath(
            '//select[@id="profile-filter-dept"]/option[normalize-space(@value)!="select"]/@value'
        ).getall()

        # Extract all school filter values
        schools = response.xpath(
            '//select[@id="profile-filter-school"]/option[normalize-space(@value)!="select"]/@value'
        ).getall()

        # directory API using department filters
        for department in departments:
            page = 1
                     
            while True:
                # Build API query parameters for department-based search
                params = {
                    "itemid": item_id,
                    "keyword": "",
                    "department": department,
                    "school": "select",
                    "pageRequested": page,
                }
                # Construct full API URL with query string
                url = self.directory_api_url + "?" + "&".join(
                    f"{k}={v}" for k, v in params.items()
                )

                response = yield scrapy.Request(
                    url=url,
                    dont_filter=True,
                )
                # Extract table rows containing faculty/staff profiles
                trs = response.xpath('//table[@id="profile-results"]//tbody/tr')

                if not trs:
                    self.logger.warning(
                        f"NO ROWS | Department={department} | Page={page}"
                    )
                    break
                # Parse each directory row
                for tr in trs:
                    name = tr.xpath(
                        './/td[contains(@class,"name")]//a/text() | '
                        './/td[contains(@class,"name")]//span[not(contains(@class,"footable-toggle"))]/text()'
                    ).get()

                    title = tr.xpath('.//div[@class="title"]/text()').get()
                    email = tr.xpath('.//div[@class="email"]/a/text()').get()
                    phone = tr.xpath('.//div[@class="phone"]/text()').get()
                    base_title = title.strip() if title else None
                    dept_name = department.strip() if department else None

                    final_title = None
                    if base_title and dept_name:
                        final_title = f"{base_title}, {dept_name}"
                    else:
                        final_title = base_title

                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_page_url,
                        "Name": name.strip() if name else None,
                        "Title": final_title,
                        "Email": email.strip() if email else None,
                        "Phone Number": phone.strip() if phone else None,
                    })
                
                # Check if a next page exists
                next_page = response.xpath(
                    f'//div[contains(@class,"pager")]//a[@data-dir-page="{page + 1}"]'
                )

                if not next_page:
                    break

                page += 1

        # query directory API using school filters
        for school in schools:
            page = 1

            while True:
                # Build API query parameters for school-based search
                params = {
                    "itemid": item_id,
                    "keyword": "",
                    "department": "select",
                    "school": school,
                    "pageRequested": page,
                }
                # Construct full API URL with query string
                url = self.directory_api_url + "?" + "&".join(
                    f"{k}={v}" for k, v in params.items()
                )

                response = yield scrapy.Request(
                    url=url,
                    dont_filter=True,
                )
                # Extract table rows from response
                trs = response.xpath('//table[@id="profile-results"]//tbody/tr')

                if not trs:
                    self.logger.warning(
                        f"NO ROWS | School={school} | Page={page}"
                    )
                    break
                # Parse each directory row
                for tr in trs:
                    name = tr.xpath(
                        './/td[contains(@class,"name")]//a/text() | '
                        './/td[contains(@class,"name")]//span[not(contains(@class,"footable-toggle"))]/text()'
                    ).get()

                    title = tr.xpath('.//div[@class="title"]/text()').get()
                    email = tr.xpath('.//div[@class="email"]/a/text()').get()
                    phone = tr.xpath('.//div[@class="phone"]/text()').get()

                    base_title = title.strip() if title else None
                    school_name = school.strip() if school else None

                    final_title = None
                    if base_title and school_name:
                        final_title = f"{base_title}, {school_name}"
                    else:
                        final_title = base_title

                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_page_url,
                        "Name": name.strip() if name else None,
                        "Title": final_title,
                        "Email": email.strip() if email else None,
                        "Phone Number": phone.strip() if phone else None,
                    })

                next_page = response.xpath(
                    f'//div[contains(@class,"pager")]//a[@data-dir-page="{page + 1}"]'
                )

                if not next_page:
                    break

                page += 1

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
        
        # Each accordion <li> represents one academic year block
        accordion_items = response.xpath('//ul[contains(@class,"accordion")]/li')
        # Loop through accordion_items
        for year_block in accordion_items:
            # Calendar content
            content = year_block.xpath('.//div[contains(@class,"content")]')
            if not content:
                continue
            content = content[0]
            # Each <h3> is a term name
            terms = content.xpath('.//h3')
            # Loop through terms
            for term in terms:
                term_name = term.xpath('normalize-space(.)').get()
                term_name = term_name.rstrip('*').strip()
                if not term_name:
                    continue

                siblings = term.xpath('following-sibling::*')
                
                # Process nodes until the next term header is encountered
                for node in siblings:
                    if node.root.tag == 'h3':
                        break

                    if node.root.tag != 'p':
                        continue

                    strong_parts = node.xpath('.//strong/text()').getall()
                    if not strong_parts:
                        continue

                    term_date = ''.join(strong_parts).strip()
                    term_date = term_date.replace('–', '-')

                    if not term_date:
                        continue

                    # skip month headers, keep TBD if needed
                    if ',' not in term_date and term_date.upper() != 'TBD':
                        continue

                    # extract description ONLY from non-strong text
                    desc_parts = node.xpath(
                        './/text()[not(ancestor::strong)]'
                    ).getall()

                    term_desc = ' '.join(
                        d.strip() for d in desc_parts if d.strip()
                    )

                    if not term_desc:
                        continue

                    if term_desc.startswith('*') or term_desc.startswith('+'):
                        continue

                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": term_date,
                        "Term Date Description": term_desc
                    })

        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
    
    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")
    