import re
import html
import scrapy
import requests
import pandas as pd
from ..utils import *
from parsel import Selector
from inline_requests import inline_requests
from playwright.sync_api import sync_playwright


class FitchSpider(scrapy.Spider):

    name = "fitch"
    institution_id = 258432954122921945

    # directory url and header
    directory_url = "https://www.fitchburgstate.edu/about/directory/all?department=All&major=All&name_contains"
    payload = {}
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'priority': 'u=0, i',
    'referer': 'https://www.fitchburgstate.edu/about/directory/all?department=All&major=All&name_contains',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'Cookie': 'gdpr_compliance=agreed'
    }

    # calendar url
    calendar_url = "https://www.fitchburgstate.edu/academics/academic-affairs-division/undergraduate-day-school-academic-calendar"


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.directory_rows = []
    
    def start_requests(self):

        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        if mode == 'course':
            self.parse_course()
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)

        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

    def parse_course(self):

        """
        Scrape course listings from Fitchburg State University's APEX course pages
        and persist the collected data as a structured dataset.

        This method uses Playwright (Chromium) to visit multiple APEX report pages,
        paginate through all available course listings, and extract detailed course
        information from the HTML tables. For each course row, it also opens the
        course title dialog (when available) to retrieve the course description
        from an embedded iframe.

        Extracted fields include:
            - Course number and title
            - Course description
            - CRN (class number)
            - Instructor
            - Enrollment (actual / maximum)
            - Course dates
            - Location
            - Textbook or course material link
            - Source URL and institution metadata

        The collected records are aggregated into a pandas DataFrame and saved
        using the `save_df` utility, keyed by the institution ID.

        Side Effects:
            - Launches a headless Chromium browser session.
            - Performs network requests to external university pages.
            - Writes course data to persistent storage via `save_df`.

        Returns:
            None
        """
         
        list_of_ids = [2, 4, 5, 14, 15, 26]
        rows = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            for ids in list_of_ids:
                url = f"https://web4.fitchburgstate.edu/apex/f?p=127:{ids}:::NO:::"
                page.goto(url, wait_until="networkidle")

                while True:
                    response = Selector(text=page.content())
                    blocks = response.xpath('//table[@class="report-standard"]//tr[td]')

                    previous_course_number = None

                    for i, block in enumerate(blocks):
                        course_number = block.xpath('./td[1]//text()').get("").strip()

                        if course_number == "Comments:":
                            continue

                        if course_number:
                            previous_course_number = course_number
                        else:
                            course_number = previous_course_number

                        title = block.xpath('./td[6]//text()').get("").strip()
                        crn_text = block.xpath('./td[2]//text()').get("").strip()
                        actual = block.xpath('./td[7]//text()').get("").strip()
                        maximum = block.xpath('./td[8]//text()').get("").strip()

                        desc = ""
                        try:
                            page.wait_for_timeout(1000)
                            page.click(
                                f"(//table[@class='report-standard']//tr[td])[{i+1}]//td[6]/a"
                            )
                            
                            frame = page.frame_locator("iframe").first
                            desc = frame.locator("body").inner_text().strip()
                            
                            page.wait_for_timeout(1000)
                            page.click('(//button[contains(text(),"Close")])[2]')
                            

                        except Exception as e:
                            print("Dialog failed:", e)
                       
                        location = block.xpath('./td[15]//text()').get("").strip()
                        location = "" if location.upper() in ["NONE", "NONE NONE"] else location

                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": url,
                            "Course Name": f"{course_number} {title}".strip(),
                            "Course Description": desc.split("\n")[-1].strip(),
                            "Class Number": crn_text,
                            "Section": "",
                            "Instructor": block.xpath('./td[11]//text()').get("").strip(),
                            "Enrollment": f"{actual}/{maximum}",
                            "Course Dates": block.xpath('./td[12]//text()').get("").strip(),
                            "Location": location,
                            "Textbook/Course Materials": block.xpath('./td[17]//@href').get("").strip()
                        })


                    # Pagination logic (CORRECT)
                    next_button = page.locator(
                        "(//td[@class='pagination']//a[normalize-space()='Next'])[2]"
                    )

                    if not next_button.is_visible():
                        break

                    next_button.click()
                    page.wait_for_timeout(2000)
                    page.wait_for_load_state("networkidle")

            browser.close()

        if rows:
            course_df = pd.DataFrame(rows)
            save_df(course_df, self.institution_id, "course")
            

    @inline_requests
    def parse_directory(self,response):

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

        rows=[]
        for count in range(0,56):
            url = f"https://www.fitchburgstate.edu/about/directory/all?department=All&major=All&name_contains=&page={count}"
            response = yield scrapy.Request(url)
            blogs = response.xpath('//div[@class="views-row"]//div[@class="facprofile-teaser__right"]')
            for blog in blogs:
                title = blog.xpath('./div[@class="facprofile-header__jobtitle"]/text()').getall()
                title= ", ".join(t.strip() for t in title if t.strip())
                
                title = html.unescape(title)
                dept = blog.xpath('./div[@class="facprofile-header__department"]/text()').get("").strip()
                school = blog.xpath('./div[@class="facprofile-header__school"]/text()').get("").strip()
                combined_title =", ".join(filter(None, [title, dept, school]))
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL":"https://www.fitchburgstate.edu"+ blog.xpath('./h3/a/@href').get("").strip(),
                    "Name": blog.xpath('./h3/a/span/text()').get("").strip(),
                    "Title": combined_title,
                    "Email": blog.xpath('./div[@class="contact__email"]/a/text()').get("").strip(),
                    "Phone Number": blog.xpath('./div[@class="contact__phone"]/a/text()').get("").strip(),
                })
        if rows:
            directory_df = pd.DataFrame(rows)
            save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self,response):

        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str

        Key behaviors:
            - Extracts and normalizes text from three separate date columns.
            - Filters out weekday names (e.g., Monday, Tuesday) from date fields
            to avoid contaminating actual term date values.
            - Applies inheritance logic so that when a primary term date is
            omitted in a row, the previously observed term date is reused.
            - Combines available date fragments into a single, well-formed
            "Term Date" string.
            - Extracts and cleans descriptive text from flexible HTML structures
            (including <p> and <strong> tags).

        The resulting data is appended as a structured row containing term
        metadata, normalized dates, and descriptive text suitable for storage
        or downstream processing.
        """

        sections = response.xpath('//div[@class="wysiwyg__body"]//h2')
        rows=[]
        for section in sections:

            term_name = section.xpath('./text()').get("").strip()
            blogs = section.xpath('./following::div[@class="table__inner"][1]//tbody/tr')
            previous_term_date1 = None

            for blog in blogs:

                term_date3 = " ".join(t.strip() for t in blog.xpath('./td[1]//text()').getall() if t.strip())

                term_date1 = " ".join(t.strip() for t in blog.xpath('./td[2]//text()').getall() if t.strip())

                term_date2 = blog.xpath('./td[3]/text()').get("").strip()

                weekdays = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"]

                if any(day in term_date2.lower() for day in weekdays):
                    term_date2 = ""

                # INHERIT LOGIC
                if term_date3:
                    previous_term_date1 = term_date3
                else:
                    term_date3 = previous_term_date1

                parts = [term_date3, term_date1, term_date2]
                combined_date = " ".join(p for p in parts if p)
                desc = blog.xpath('./td[4]//p//strong/text() | ./td[4]//text() | ./td[4]//strong/text()').getall()
                term_desc = " ".join(d.strip() for d in desc if d.strip())

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,

                    "Term Date": combined_date,

                    "Term Date Description":
                        term_desc,
                })

        if rows:
            calendar_df = pd.DataFrame(rows)  # load to dataframe
            save_df(calendar_df, self.institution_id, "calendar") 


    