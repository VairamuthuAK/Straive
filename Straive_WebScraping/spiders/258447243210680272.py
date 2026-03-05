import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests

class LackawannaSpider(scrapy.Spider):
    name = "lackawanna"
    institution_id = 258447243210680272

    allowed_domains = ["hos.lackawanna.edu"]

    # course_link and headers for POST request

    course_url = "https://hos.lackawanna.edu/academics/course-schedules/campuses-new-covid.asp"
    course_source_url='https://www.lackawanna.edu/academics/course-schedules/'
    
    # Employee directory API endpoint
    directory_api_url = (
        "https://www.lackawanna.edu/student-resources/title-ix/"
    )
    
     # Academic calendar page URL
    calendar_url = "https://www.lackawanna.edu/academics/academic-calendar/" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
                yield scrapy.Request(url=self.course_url,  callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)

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
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url, callback=self.parse_directory, dont_filter=True)
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
        Load course index page, extract result page URLs from onclick attributes,
        visit each result page via blocking GET requests, parse course table rows,
        collect all course data locally, and save once at the end.
        """
        rows = []
        # Extract onclick values that contain result page URLs
        onclick_values = response.xpath(
            "//a[contains(@onclick,'results-new-covid.asp')]/@onclick"
        ).getall()
        
        # Store full result page URLs
        result_urls = []
        
        # Extract relative URL from onclick JavaScript
        for value in onclick_values:
            relative_path = value.split("('")[1].split("'")[0]
            # Build absolute URL for result page
            result_urls.append(
                "https://hos.lackawanna.edu/academics/course-schedules/"
                + relative_path
            )

        # Loop through each result page URL
        for result_url in result_urls:
            result_response = yield scrapy.Request(
                url=result_url,
                dont_filter=True,
            )

            # Skip if response is invalid
            if not result_response:
                continue
            
            # COURSE DATE 
            course_date = result_response.xpath(
            "//table[@id='Table1']//tr[1]//b/text()"
            ).get()
            course_date = course_date.strip() if course_date else ""
            location = result_response.xpath(
                "(//table[@id='Table1']//tr[1]//strong/text())[1]"
            ).get()
            location = location.strip() if location else ""
            # Extract all table rows containing course data
            table_rows = result_response.xpath("//table[@id='Table1']/tr")

            # Loop through each row
            for tr in table_rows:
                cells = tr.xpath("./td")

                if len(cells) < 6:
                    continue

                raw_code = cells[0].xpath("text()").get()
                course_title = cells[1].xpath("text()").get()
                instructor = cells[4].xpath("text()").get()
                textbook_link = cells[5].xpath(".//a/@href").get()

                if not raw_code or not course_title:
                    continue

                parts = raw_code.strip().split()
                class_number = " ".join(parts[:2])   
                section = parts[-1]                  

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": result_url,
                    "Course Name": f"{class_number} {course_title.strip()}",
                    "Course Description": "",
                    "Class Number": class_number,
                    "Section": section,
                    "Instructor": instructor.strip() if instructor else "",
                    "Enrollment": "",
                    "Course Dates": course_date,
                    "Location": location,
                    "Textbook/Course Materials": textbook_link or "",
                })

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "course")


    def parse_directory(self, response):
        """
        Parse directory using Scrapy response.

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Name
        - Title
        - Email
        - Phone Number
        """
        rows = []
        # Select all staff name headings
        name_nodes = response.xpath(
            "//h3[contains(@class,'elementor-heading-title')]"
        )
        
        # Loop through each name node
        for h3 in name_nodes:
            # Name comes directly from h3
            name = h3.xpath("normalize-space()").get()

            # Locate the following text-editor widget with contact details
            contact_block = h3.xpath(
                "./ancestor::div[contains(@class,'elementor-widget-heading')][1]"
                "/following-sibling::div[contains(@class,'elementor-widget-text-editor')][1]"
            )
            
            # Skip if contact block is missing
            if not contact_block:
                continue

            # Extract title from first strong tag
            title = contact_block.xpath(
            "normalize-space(.//p/strong[1])"
            ).get()
            
            # Extract email address
            email = contact_block.xpath(
                ".//a[starts-with(@href,'mailto:')]/text()"
            ).get()
            
            # Extract phone number
            phone = contact_block.xpath(
                ".//a[starts-with(@href,'tel:')]/text()"
            ).get()


            # Skip rows with no meaningful data
            if not name and not email:
                continue

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name or "",
                "Title": title or "",
                "Email": email or "",
                "Phone Number": phone or "",
            })

        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):
        """
        Parse academic calendar page.

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Term Name
        - Term Date
        - Term Date Description
        """

        calendar_rows = []
        # Select all main term headings (Fall, Spring, Summer, etc.)
        term_headers = response.xpath(
            "//h2[contains(@class,'elementor-heading-title')]"
        )

        # Loop through each main term
        for h2 in term_headers:
            # Extract term name text safely
            term_name = h2.xpath("normalize-space()").get()
            # Get sibling content blocks after this term heading
            content_nodes = h2.xpath(
                "ancestor::div[contains(@class,'elementor-widget-heading')][1]"
                "/following-sibling::div"
            )
            # Used only for Summer sessions
            current_subterm = None

            # Loop through each content node
            for node in content_nodes:
                # If a new h2 appears, this term is finished
                if node.xpath(".//h2"):
                    break

                # Detect subterm headings (Summer Subterm 1, 2, 3)
                h3 = node.xpath(".//h3[contains(@class,'elementor-heading-title')]")

                if h3:
                    # Store subterm name for upcoming table
                    current_subterm = h3.xpath("normalize-space()").get()
                    continue

                # Look for a table inside this node
                table = node.xpath(".//table")

                if not table:
                    # Skip nodes that do not contain tables
                    continue

                # Build final term name
                if current_subterm:
                    full_term_name = f"{term_name} - {current_subterm}"

                else:
                    full_term_name = term_name

                # Extract all table rows that actually contain data
                rows = table.xpath(".//tr[td]")
                
                # Loop through each data row
                for row in rows:
                    term_date = row.xpath(
                        "normalize-space(.//td[1])"
                    ).get()

                    term_desc = " ".join(
                        row.xpath(".//td[2]//text()").getall()
                    ).strip()

                    # Skip incomplete rows
                    if not term_date or not term_desc:
                        continue

                    # Append final row
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": full_term_name,
                        "Term Date": term_date,
                        "Term Date Description": term_desc,
                    })

        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
