import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class CraftonSpider(scrapy.Spider):
    name = "crafton_hills"
    institution_id = 258444834321229783

    course_url="https://www.craftonhills.edu/eschedule/index.php"
      
    course_api_url = "https://www.craftonhills.edu/_resources/php/courses/open-json.php"

    # Employee directory API endpoint
    directory_api_url = (
        "https://www.craftonhills.edu/directory/"
    )
    
     # Academic calendar page URL
    calendar_url = "https://www.craftonhills.edu/admissions-and-records/dates-and-deadlines/index.php" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_api_url, callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar,dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_api_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url,callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_api_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar,dont_filter=True)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar,dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_api_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar,dont_filter=True)
       
    
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
        - Load course data from JSON response
        - Iterate through terms → subjects → courses → sections
        - Skip cancelled sections
        - Merge all meetings for a section into one row
        - Aggregate instructors, buildings, and date ranges
        - Build textbook link when applicable
        - Collect rows locally and save once at the end
        """
        # Parse JSON response returned by the course API
        data = response.json()
        rows = []
        
        # TERMS (top-level keys in JSON)
        for term_code, term_block in data.items():
            # Each term contains an "eschedule" object
            eschedule = term_block.get("eschedule")

            if not isinstance(eschedule, dict):
                continue

            # LOOP SUBJECTS (e.g. ACCT, MATH, ENGL)
            for subject_code, subject_block in eschedule.items():

                courses = subject_block.get("Courses")

                if not isinstance(courses, dict):
                    continue

                # LOOP COURSES (catalog-level course)
                for course in courses.values():
                    # Build course-level fields
                    course_name = f"{course.get('CrsName','')} - {course.get('CrsTitle','')}".strip()
                    course_desc = course.get("CrsDesc", "")
                    class_number = course.get("CrsName", "")
                    crs_number = course.get("CrsNumber", "")

                    sections = course.get("Sections")

                    if not isinstance(sections, dict):
                        continue

                    # SECTIONS (ONE ROW EACH)
                    for section in sections.values():
                        # Skip cancelled sections
                        if section.get("SecStatus") == "C":
                            continue

                        meetings = section.get("Meetings", [])

                        if not isinstance(meetings, list):
                            continue

                        instructors = set()
                        buildings = set()
                        start_dates = []
                        end_dates = []

                        # MERGE MEETINGS INTO ONE SECTION ROW
                        for meeting in meetings:

                            if meeting.get("StartDate"):
                                start_dates.append(meeting["StartDate"])

                            if meeting.get("EndDate"):
                                end_dates.append(meeting["EndDate"])

                            if meeting.get("Bldg"):
                                raw = meeting["Bldg"].strip()
                                first_part = raw.split(" ")[0] 
                                buildings.add(first_part)

                            # Collect instructor names
                            for fac in meeting.get("FacName", []):
                                instructors.add(fac)

                        # Course date (NO TIME)
                        course_date = ""

                        if start_dates and end_dates:
                            course_date = f"{min(start_dates)} - {max(end_dates)}"

                        # Textbook link (official)
                        textbook_link = ""
                        if not section.get("ZeroT"):
                            textbook_link = (
                                "https://surveysr18live.sbccd.edu/textbooks/redirect.aspx"
                                f"?LOC=CHC"
                                f"&TERM={term_code}"
                                f"&SUBJECT={subject_code}"
                                f"&COURSE.NO={crs_number}"
                                f"&SEC.NO={section.get('SecNumber','')}"
                                f"&METHODS="
                            )

                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.course_url,
                            "Course Name": course_name,
                            "Course Description": course_desc,
                            "Class Number": class_number,
                            "Section": section.get("SecNumber", ""),
                            "Instructor": "; ".join(sorted(instructors)),
                            "Enrollment": section.get("SecSeats", ""),
                            "Course Dates": course_date,
                            "Location": "; ".join(sorted(buildings)),
                            "Textbook/Course Materials": textbook_link,
                        })

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
            Parse Crafton Hills College employee directory.

            Flow:
            - Start from the first directory page
            - Extract employee data row by row
            - Follow pagination using the Next link (blocking)
            - Collect all rows locally
            - Save once at the end
        """

        self.logger.info("Starting directory scrape (INLINE MODE)")

        directory_rows = []
        # Holds the current directory page
        page_response = response
        page_no = 1

        # PAGINATION LOOP 
        while True:

            self.logger.info(f"Processing directory page {page_no}")
            # Each <tr> represents one employee record
            employee_rows = page_response.xpath(
                "//table[contains(@class,'employee-directory')]//tbody/tr"
            )

            # PROCESS EACH EMPLOYEE ROW
            for row in employee_rows:
                first_name = row.xpath(
                    "normalize-space(.//td[@class='first-name']//strong/text())"
                ).get()

                last_name = row.xpath(
                    "normalize-space(.//td[@class='last-name']//strong/text())"
                ).get()

                if not first_name and not last_name:
                    continue

                full_name = " ".join(
                    part for part in [first_name, last_name] if part
                )

                title_text = row.xpath(
                    "normalize-space(.//td[contains(@class,'employee-title')]/text())"
                ).get() or ""

                department_text = row.xpath(
                    "normalize-space(.//td[@data-title='Department']/text())"
                ).get() or ""
                
                # Combine title and department cleanly
                combined_title = ""

                if title_text and department_text:
                    combined_title = f"{title_text}, {department_text}"

                elif title_text:
                    combined_title = title_text

                elif department_text:
                    combined_title = department_text

                phone_number = row.xpath(
                    "normalize-space(.//td[@data-title='Phone']/text())"
                ).get() or ""

                directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": full_name,
                    "Title": combined_title,
                    "Email":'',
                    "Phone Number": phone_number,
                })

            # Locate the Next pagination link
            next_href = page_response.xpath(
                "//div[contains(@class,'directory-pagination')]//a[contains(normalize-space(.),'Next')]/@href"
            ).get()
            
            # Stop when no Next page exists
            if not next_href:
                self.logger.info("No Next page found. Pagination complete.")
                break
            
            # Build absolute URL for next page
            next_url = page_response.urljoin(next_href)
            self.logger.info(f"Following next page: {next_url}")
            # INLINE REQUEST to fetch the next page
            page_response = yield scrapy.Request(
                next_url,
                dont_filter=True
            )
            page_no += 1

        self.logger.info(f"Total directory rows collected: {len(directory_rows)}")
        directory_df = pd.DataFrame(directory_rows)
        save_df(directory_df, self.institution_id, "campus")


    @inline_requests
    def parse_calendar(self, response):
        """
        Parse Crafton Hills academic calendar.

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Term Name
        - Term Date
        - Term Date Description
        """
        """
        Parse Crafton Hills academic calendar.

        Flow:
        - Start from the calendar landing page
        - Collect links to individual “Term Dates” pages
        - Visit each term page inline (blocking)
        - Parse the first calendar table only
        - Handle rowspan-style dates correctly
        - Split descriptions based on page structure
        - Save all rows once at the end
        """

        calendar_rows = []

        # Collect term links
        term_links = response.xpath(
            "//h2/following-sibling::ul[1]/li/a[contains(text(),'Term Dates')]/@href"
        ).getall()
        # Convert relative URLs to absolute URLs
        term_links = [response.urljoin(link) for link in term_links]

        # VISIT EACH TERM PAGE
        for term_url in term_links:
            term_response = yield scrapy.Request(term_url, dont_filter=True)

            page_title = term_response.xpath(
                "//h1[@id='pageTitle']/text()"
            ).get("").strip()

            term_name = page_title.replace(" Term Dates", "").strip()

            # First table only
            rows = term_response.xpath(
                "(//table)[1]//tr[not(contains(@class,'table_header'))]"
            )

            last_date = None

            # PROCESS EACH TABLE ROW
            for row in rows:
                tds = row.xpath("./td")
                # Case 1: Date + description in same row
                if len(tds) == 2:
                    date_text = " ".join(
                        tds[0].xpath(".//text()").getall()
                    ).strip()
                    desc_cell = tds[1]
                    last_date = date_text

                # Case 2: Description-only row (date inherited)    
                elif len(tds) == 1:
                    date_text = last_date
                    desc_cell = tds[0]
                else:
                    continue

                if not date_text:
                    continue

                # Extract description text from THIS ROW 
                parts = [
                    part.strip()
                    for part in desc_cell.xpath(".//text()").getall()
                    if part.strip()
                ]

                if not parts:
                    continue

                # STRUCTURE-BASED SPLITTING
                descriptions = []
                
                # If multiple <p> tags exist, treat each as a separate event
                p_nodes = desc_cell.xpath("./p")

                if p_nodes:
                    for p in p_nodes:
                        text = " ".join(p.xpath(".//text()").getall()).strip()

                        if text:
                            descriptions.append(text)

                # If bullet-style content exists, split by bullet items
                elif any(part.startswith("•") for part in parts):
                    descriptions.extend(parts)
                    
                else:
                    descriptions.append(" ".join(parts))

                # Save rows
                for desc in descriptions:
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": term_url,
                        "Term Name": term_name,
                        "Term Date": date_text,
                        "Term Date Description": desc.lstrip("• ").strip(),
                    })

        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
