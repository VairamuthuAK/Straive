import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class BethelSpider(scrapy.Spider):
    name = "bethel"
    institution_id = 258433196079736792

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = ""
    directory_source_url = "https://athletics.bethel.edu/staff-directory/evan-alexius/550"
    calendar_url = "https://www.bethel.edu/undergrad/academics/calendar/"

   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            self.parse_course()
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            
            self.parse_course()
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # # All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)

    # Parse methods UNCHANGED from your original
    @inline_requests
    def parse_course(self):
        pass
        
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
        # STEP 1: Initialize storage for directory rows
        directory_rows = []
        
        # STEP 2: Extract all staff/faculty profile URLs
        staff_urls = response.xpath('//select[@id="ddl_all_staff"]/option/@value').getall()
        for staff_url in staff_urls:
            
            # Construct absolute URL if needed
            staff_url = f"https://athletics.bethel.edu{staff_url}"
            
            # Request individual staff page
            staff_response = yield scrapy.Request(staff_url)
            
            # STEP 3: Extract staff member details
            name = " ".join(staff_response.xpath('//h2[@class="sidearm-staff-member-bio-heading sidearm-common-bio-heading"]//span/span/text()').getall()).strip()
            title = staff_response.xpath('//dt[contains(text(),"Title")]/following-sibling::dd/text()').get('').strip()
            phone = staff_response.xpath('//dt[contains(text(),"Phone")]/following-sibling::dd/text()').get('').strip()
            email = staff_response.xpath('//dt[contains(text(),"Email")]/following-sibling::dd/a/text()').get('').strip()
            
            # STEP 4: Append data to results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": staff_url,
                        "Name": name,
                        "Title": title,
                        "Email": email,
                        "Phone Number": phone,
                    }
                )
            
        # STEP 5: Convert results into a DataFrame and save
        directory_df = pd.DataFrame(directory_rows)
        save_df(directory_df, self.institution_id, "campus")
        
        
    @inline_requests
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
        # STEP 1: Initialize storage for calendar records
        calendar_rows = []
        
        # STEP 2: Extract calendar links from main listing page
        blocks = response.xpath('//div[@class="main"]//ul/li')
        for block in blocks:
            
            # Extract calendar page URL
            cal_url = block.xpath('./a/@href').get('').strip()
            
            cal_term_name = block.xpath('.//text()').get('').strip()
            
            # STEP 3: Filter only 2025 / 2026 calendars
            if "2025" in cal_term_name or "2026" in cal_term_name:
                if cal_url:
                    
                    # Request the individual calendar page
                    cal_response = yield scrapy.Request(cal_url)
                    
                    # STEP 4: Extract all term headers (e.g., Fall 2025)
                    term_headers = cal_response.xpath('//div[@id="textcontainer"]//h2//text()').getall()
                    for term_header in term_headers:
                        
                        # ---------------------------------------------------
                        # STEP 5: Locate the table belonging to each term
                        #
                        # Pattern explanation:
                        # - Some pages wrap term text inside <span> within <h2>
                        # - Some pages place text directly inside <h2>
                        #
                        # We handle BOTH structures safely
                        # ---------------------------------------------------
                        blocks = cal_response.xpath(f'//span[contains(text(),"{term_header}")]/parent::h2/following-sibling::table[1]/tbody/tr')
                        blocks1 = cal_response.xpath(f'//h2[contains(text(),"{term_header}")]/following-sibling::table[1]/tbody/tr')
                        
                        # STEP 6: Extract calendar rows (dates + descriptions)
                        if blocks:
                            for block in blocks:
                                
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_url,
                                    "Term Name": term_header,
                                    "Term Date": block.xpath('./td[2]/text()').get('').strip(),
                                    "Term Date Description": block.xpath('./td[1]/text()').get('').strip(),
                                })
                        elif blocks1:
                            for block in blocks1:
                                
                                calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": cal_url,
                                    "Term Name": term_header,
                                    "Term Date": block.xpath('./td[2]/text()').get('').strip(),
                                    "Term Date Description": block.xpath('./td[1]/text()').get('').strip(),
                                })
        
        # STEP 7: Save extracted calendar data
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
