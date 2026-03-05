import re
import io
import scrapy
import pandas as pd
import pdfplumber
from parsel import Selector
from inline_requests import inline_requests
from ..utils import *

class ClcmnSpider(scrapy.Spider):
    """
    Scrapy Spider for Central Lakes College (CLCMN).

    This spider scrapes:
    1. Course data
    2. Staff directory
    3. Academic calendar (PDF)

    And saves them as structured CSV/Excel using save_df().
    """
    
    name="clcmn"
    institution_id = 258417159363913692

    # Base URL for building full detail links
    course_base_url = "https://eservices.minnstate.edu"

    # Entry URL for course search
    course_url = "https://eservices.minnstate.edu/registration/search/advanced.html?searchrcid=0301&showAdvanced=&delivery=ALL&searchcampusid=&begindate=&courseNumber=&openValue=OPEN_PLUS_WAITLIST&subject=ABE&campusid=301&endtime=&mntransfer=&starttime=&resultNumber=250&yrtr=20271&textbookcost=all&honorsflag=honorsAll&site=&instructor=&credits=&keyword=&courseId=&credittype=ALL"

    # Directory page URL
    directory_url = "https://www.clcmn.edu/employee-directory/"

    # Academic calendar PDF URL
    calendar_url = "https://www.clcmn.edu/wp-content/uploads/2025/11/2025-2026-CLC-Academic-Calendar-Printable.pdf"

    # Headers to mimic a real browser
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    # Template URL used to dynamically load course data
    base_url = (
        "https://eservices.minnstate.edu/registration/search/advancedSubmit.html"
        "?campusid=301"
        "&searchrcid=0301"
        "&yrtr={sem}"
        "&subject={subject}"
        "&openValue=ALL"
        "&delivery=ALL"
        "&credittype=ALL"
        "&honorsflag=honorsAll"
        "&textbookcost=all"
        "&resultNumber=250"
    )
    
    def start_requests(self):
        """
        Entry point of the spider.

        This method decides what to scrape based on SCRAPE_MODE.
        """

        # Get scrape mode from settings
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
            # self.parse_calendar(self.calendar_url)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
            # self.parse_calendar(self.calendar_url)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)

    # Allows inline requests inside this method
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
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """

        # Store all courses here
        course_data =[]

        # Extract all semester values
        semester_values = response.xpath(".//select[@name='yrtr']/option[@value!='']/@value").getall()

        # Extract all subject values
        subject_values = response.xpath(".//select[@name='subject']/option[@value!='']/@value").getall()

        # Loop through every semester and subject
        for sem in semester_values:
            
            for subject in subject_values:

                # Build URL dynamically
                url = self.base_url.format(sem=sem, subject=subject)

                # Fetch course listing page
                all_course_response = yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    dont_filter=True
                )

                # Parse HTML
                sel = Selector(text=all_course_response.text)

                # Get all rows that contain detail links
                rows = sel.xpath('//table[.//a[contains(@href,"detail.html")]]//tbody/tr')
            
                # If no courses found, skip
                if not rows:
                    continue
                
                # Extract detail URLs
                detail_urls = rows.xpath('.//a[contains(@href,"/registration/search/detail.html")]/@href').getall()

                for detail in detail_urls:
                    
                    # Build full detail URL
                    fullUrl = self.course_base_url + detail

                    # Request course detail page
                    course_response = yield scrapy.Request(
                        url=fullUrl,
                        headers=self.headers,
                        dont_filter=True
                    )

                    courseSel = Selector(text=course_response.text)
                    
                    item = {}
                    
                    # Add fixed fields
                    item["Cengage Master Institution ID"] = self.institution_id
                    item["Source URL"] = fullUrl

                    # Extract and clean course name
                    item["Course Name"] = re.sub(r'\s+', ' ',courseSel.xpath(
                        '//h1//text()[normalize-space()]'
                    ).get(default="").strip())

                    # Extract description
                    item["Course Description"] = re.sub(r'\s+', ' ',courseSel.xpath(
                        '//div[@class="detaildiv" and contains(.,"Description")]/following-sibling::text()[normalize-space()]'
                    ).get(default="").strip())

                    # Extract class number
                    item["Class Number"] = re.sub(r'\s+', ' ',courseSel.xpath(
                        '//table[contains(@class,"myplantable")]//tbody[@class="course-detail-summary"]//tr/td[2]/text()'
                    ).get(default="").strip())

                    # Extract section
                    item["Section"] = re.sub(r'\s+', ' ',courseSel.xpath(
                        '//table[contains(@class,"myplantable")]//tbody[@class="course-detail-summary"]//tr/td[5]/text()'
                    ).get(default="").strip())

                    # Extract instructors
                    instructors = courseSel.xpath(
                        '//table[contains(@class,"myplantable")]'
                        '//tbody[@class="course-detail-summary"]'
                        '//tr/td[12]//text()[normalize-space()]'
                    ).getall()

                    # Clean + join
                    instructors = [i.strip() for i in instructors if i.strip()]
                    # ✅ Remove duplicates but keep order
                    unique_instructors = list(dict.fromkeys(instructors))
                    item["Instructor"] = "| ".join(unique_instructors)

                    seat_table = courseSel.xpath(
                        '//div[@class="detaildiv" and normalize-space(.)="Seat Availability"]'
                        '/following-sibling::table[1]'
                    )

                    # IMPORTANT: skip the first empty <td>
                    tds = seat_table.xpath('.//tr/td[position() > 1]')

                    size_raw  = tds[0].xpath('normalize-space(.)').get()
                    enrolled_raw  = tds[1].xpath('normalize-space(.)').get()
                    # ✅ keep only digits
                    size = re.search(r'\d+', size_raw).group() if size_raw else ""
                    enrolled = re.search(r'\d+', enrolled_raw).group() if enrolled_raw else ""

                    item["Enrollment"] = f"{enrolled} / {size}"

                    dates = courseSel.xpath(
                        './/table[contains(@class,"myplantable")]'
                        '//tbody[@class="course-detail-summary"]'
                        '//tr/td[7]//text()'
                    ).getall()

                    # Clean, normalize, remove empty strings
                    dates = [
                        re.sub(r'\s+', ' ', d.replace('\xa0', ' ').strip())
                        for d in dates
                        if d and d.strip()
                    ]

                    # Optional: keep only date ranges like 01/12 - 05/15
                    dates = [
                        d for d in dates
                        if re.search(r'\d{2}/\d{2}\s*-\s*\d{2}/\d{2}', d)
                    ]

                    # Remove duplicates while preserving order
                    dates = list(dict.fromkeys(dates))

                    item["Course Dates"] = " , ".join(dates)

                    item["Location"] = re.sub(r'\s+', ' ',courseSel.xpath(
                        '//div[@class="detaildiv" and contains(normalize-space(.), "Location Details")]'
                        '/following-sibling::table[1]'
                        '//b[normalize-space(.)="Location:"]/parent::td/text()[normalize-space()]'
                    ).get(default="").strip())
                    
                    # Textbook link
                    link = courseSel.xpath(
                        '//div[@class="detaildiv" and contains(.,"Course Books")]'
                        '/following::table[1]//tr/td[2]//a/@href'
                    ).get()

                    item["Textbook/Course Materials"] = link if link is not None else ""

                    # Add course to list
                    course_data.append(item)

        # Convert to DataFrame and save
        course_df = pd.DataFrame(course_data)
        save_df(course_df, self.institution_id, "course")

    def parse_directory(self, response):
        """
        PParses the staff directory pages.

        It also handles pagination and keeps collecting data until the last page is reached.
        Final output is saved using save_df().

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """

        # Get existing collected rows (used for pagination)
        # If this is the first page, it will be an empty list
        rows = response.meta.get("rows", [])

        # Each staff member is inside a card block
        cards = response.xpath('//div[contains(@class,"sp-team-pro-item")]//div[@class="caption"]')

        # Loop through each staff card
        for card in cards:

            # Extract staff details
            name = card.css("div.sptp-member-name h2.sptp-name::text").get(default="").strip()
            email = card.css("div.sptp-member-email a span::text").get(default="").strip()
            title = card.css("div.sptp-member-profession h4.sptp-profession-text::text").get(default="").strip()
            
            # Extract raw phone text
            phone_raw = card.xpath('normalize-space(.//div[@class="sptp-member-phone"])').get(default="")
            
            # Regex pattern to match phone numbers like:
            # (123) 456-7890
            # 123-456-7890
            # 123 456 7890
            phone = re.search(
                r'(\(?\d{3}\)?[\s\-]\d{3}[\s\-]\d{4}(?:\s*(?:or|ext\.?)\s*[\d\-]+)*)',
                phone_raw or ""
            )

            # If phone is found, extract it; else keep empty
            phone = phone.group(1) if phone else ""
            
            # Append the extracted data to rows list
            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })

        # Look for the "Next" pagination button
        next_page = response.xpath('//div[@class="sptp-post-pagination"]/a[@class="next page-numbers"]/@href').get()

        # If next page exists, request it and continue scraping
        if next_page:
            self.logger.info(f"➡️ Moving to next page: {next_page}")
            
            yield scrapy.Request(
                next_page,
                callback=self.parse_directory,
                meta={"rows": rows} # Pass already collected data forward
            )
        else:
            # If no more pages are available, safely save all collected staff details
            directory_df = pd.DataFrame(rows)
            save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        Parses the academic calendar from a PDF file.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """

        # This list will store all extracted calendar rows
        calendar_data = []

        # This variable will keep track of the current term
        # Example: "Fall Semester 2025"
        current_term = None

        # Convert the PDF response body into a byte stream
        # This allows pdfplumber to read it without saving to disk
        pdf_bytes = io.BytesIO(response.body)

        # Open the PDF using pdfplumber
        with pdfplumber.open(pdf_bytes) as pdf:

            # Loop through each page in the PDF
            for page in pdf.pages:

                # Extract all text from the page
                text = page.extract_text()

                # If no text is found, skip this page
                if not text:
                    continue

                # Split the text into lines and clean them
                lines = [line.strip() for line in text.split("\n") if line.strip()]

                # Loop through each line
                for line in lines:

                    # Detect term name lines like:
                    # "Fall Semester 2025"
                    # "Spring Session 2026"
                    # or "Semester Break"
                    if re.match(r"(Fall|Spring|Summer).*?(Semester|Session)\s+\d{4}", line) \
                    or line == "Semester Break":
                        current_term = line
                        continue

                    match = re.match(r"^([A-Za-z]+\s+\d+(?:-\d+)?)\s+(.*)", line)

                    # If a date pattern is found AND we already have a term
                    if match and current_term:
                        # Extract date part (e.g., "August 25")
                        term_date = match.group(1).strip()
                        # Extract description (e.g., "Classes Begin")
                        term_desc = match.group(2).strip()

                        # Store extracted info
                        calendar_data.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": current_term,
                            "Term Date": term_date,
                            "Term Date Description": term_desc
                        })

        calendar_df = pd.DataFrame(calendar_data)
        save_df(calendar_df, self.institution_id, "calendar")

