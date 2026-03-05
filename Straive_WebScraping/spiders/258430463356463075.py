import time
import scrapy
import requests
import pandas as pd
from ..utils import *
from parsel import Selector
from datetime import datetime
from playwright.sync_api import sync_playwright

class BlackhwakSpider(scrapy.Spider):

    name = "hwak"
    institution_id = 258430463356463075

    course_rows = []
    course_url =  "https://www.blackhawk.edu/Programs-Courses/Course-Search"
  
    directory_url = "https://www.blackhawk.edu/About-Us/Staff-Directory"

    calendar_url = "https://www.blackhawk.edu/API/ICG.EventCalendar/Event/Init"

    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            self.parse_calendar()

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        # All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

            
    def parse_course(self):

        """
        Scrapes course and section data from the institution's course search page
        using Playwright for dynamic interaction and Requests for detail pages.

        Workflow:
        ---------
        1. Launches a Chromium browser instance using Playwright.
        2. Navigates to the course search URL.
        3. Iterates through each available "Term" option in the dropdown.
        4. Submits the form for each term to retrieve course listings.
        5. Extracts course listing links from each results page.
        6. Visits each course detail page using the requests library.
        7. Parses:
            - Course title and catalog number
            - Course description
            - CRN (Class Number)
            - Instructor
            - Location
            - Course dates
            - Book information link
        8. Handles pagination until no "next page" link is available.
        9. Stores extracted records into a pandas DataFrame.
        10. Saves the DataFrame using `save_df()`.

        Returns:
        --------
        None
            The function saves the scraped course data to storage
            but does not explicitly return a value.

        Notes:
        ------
        - Uses `networkidle` state to ensure dynamic content is fully loaded.
        - Instructor values containing only commas are cleaned.
        - Handles multiple course dates per section.
        - Browser is closed after data collection is complete.
        """

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            start_url = self.course_url
            context = browser.new_context()
            page = context.new_page()
            page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
            dropdown =  page.locator('//label[contains(text(),"Term")]/parent::div/select')
            dropdown.wait_for(state="visible")
            options = dropdown.locator("option")
            labels = options.all_text_contents()
            rows=[]
            for label in labels:
                print(f"Selecting: {label}")
                dropdown.select_option(label=label)
                page.locator('//input[@type="submit"]').click()
                page.wait_for_load_state("networkidle")
                while True:
                    time.sleep(3)
                    page.wait_for_load_state("networkidle")
                    resp = Selector(text=page.content())
                    listing_links = resp.xpath('//h3/a/@href').getall()
                    for link in listing_links:
                        print(f"Scraping course: {link}")
                        headers = {
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                        'cache-control': 'max-age=0',
                        'priority': 'u=0, i',
                        'referer': 'https://www.blackhawk.edu/Programs-Courses/Course-Search',
                        'upgrade-insecure-requests': '1',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                        }
                        response = requests.get(link,headers=headers)
                        response_sel = Selector(text=response.text)
                        title = response_sel.xpath('//h1/text()').get('').strip()
                        title_class = response_sel.xpath('//div[contains(@class,"catalog-number")]/text()').get('').strip()
                        desc= response_sel.xpath('//div[@class="space-y-7"]/div[@class="prose"][not(.//h2)]/p/text()').get('').strip()
                        blocks = response_sel.xpath('//div[@id="class-sections"]')
                        for block in blocks:
                            crn = block.xpath('.//div[@class="text-xl/normal font-bold"]/text()').getall()
                            crn = "".join([c.strip() for c in crn if c.strip()])
                            course_title = f"{title_class} {title}".strip()

                            # Description: ignore div.prose that has h2 inside
                            book_info = block.xpath('.//span[contains(text(),"Book Info:")]/following-sibling::span/a/@href').get('').strip()
                            instructor = block.xpath('.//span[contains(text(),"Instructor:")]/following-sibling::span/text()').get('').strip()
                            if instructor == ",":
                                instructor =""
                            
                            location = block.xpath('.//span[contains(text(),"Location:")]/following-sibling::span/text()').get('').strip()
                            dates = block.xpath('.//span[contains(text(),"Dates:")]/following-sibling::span/text()').getall()
                            dates = [d.strip() for d in dates if d.strip()]
                            if not dates:
                                dates = [""]

                            for course_date in dates:
                                
                                rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Course Name": course_title or "",
                                    "Course Description":desc or "",
                                    "Class Number":  crn or "",
                                    "Section": "",
                                    "Instructor": instructor or "",
                                    "Enrollment": "",
                                    "Course Dates": course_date,
                                    "Location": location or "",
                                    "Textbook/Course Materials": book_info or "",
                                })

                    next_page = page.locator('//a[contains(@class,"page-next")]')
                    if next_page.count() == 0 or not next_page.is_visible():
                        break
                    next_page.click()
                    page.wait_for_load_state("networkidle")

            if rows:
                browser.close()
                course_df = pd.DataFrame(rows)
                save_df(course_df, self.institution_id, "course")

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

        blocks = response.xpath('//table[@id="staff"]/tbody/tr')
        rows=[]
        for block in blocks:
            first_name = block.xpath('./td[1]/text()').get("").strip()
            second_name = block.xpath('./td[2]/text()').get("").strip()
            phone = block.xpath('./td[3]/a/text()').get("").strip()
            email = block.xpath('./td[4]/a/text()').get("").strip()
            title = block.xpath('./td[5]/text()').get("").strip()
            rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": f"{first_name} {second_name}",
                    "Title": title,
                    "Email":email,
                    "Phone Number": phone,
                })
            
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")


    def parse_calendar(self):

        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        
        headers = {
        'accept': 'application/json',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'content-type': 'application/json',
        'referer': 'https://www.blackhawk.edu/Events/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }
        response = requests.request("GET", self.calendar_url, headers=headers)
        json_data = response.json()
        datas = json_data.get('Data',{}).get('Events',[])
        rows=[]
        for data in datas:
            start_date = data.get('Start',"").strip()

            if "2026" in start_date or "2027" in start_date:
                end_date = data.get('End',"").strip()
                s = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                e = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                 
                if s.year == e.year and s.month == e.month and s.date() == e.date():
                    result = start_date.split("T")[0]
                else:
                    result = f"{start_date.split('T')[0]} - {end_date.split('T')[0]}"

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": "",
                    "Term Date": result.strip(),
                    "Term Date Description":  data.get('Title','').strip()
                })

        if rows:
            calendar_df = pd.DataFrame(rows)
            save_df(calendar_df, self.institution_id, "calendar")

    
    