import re
import scrapy
import pandas as pd
from inline_requests import inline_requests
from ..utils import save_df


class RollinsSpider(scrapy.Spider):
    
    name = "ROLLINS COLL-WINTER PARK"
    institution_id = 258440212655400924
    
    # List to store all records
    courseData = []
    calendar_data = []
    
    # Urls
    base_url = "http://www.rollins.edu/"
    course_url = "https://catalog.rollins.edu/content.php?catoid=18&navoid=707"
    directory_url = ""
    calendar_url = "https://www.rollins.edu/registrar/academic-calendar-cla/"
    
    # Default HTTP headers used for normal page requests
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

    def start_requests(self):

        # Read scrape mode from settings (course / directory / calendar / combinations)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode Execution ----
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        elif mode == 'directory':
            self.parse_directory()

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            self.parse_directory()

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            
    # inline_requests lets us yield request and get response directly
    @inline_requests
    def parse_course(self, response):
        """
        Parse course data using request session response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Course Name"                   : str
        - "Course Description"            : str
        - "Class Number"                  : str
        - "Section"                       : str
        - "Instructor"                    : str
        - "Enrollment"                    : int
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        
        # Extract all course preview links
        course_links = response.xpath("//a[contains(@href,'preview_course_nopop.php')]/@href").getall()
        
        # Remove duplicate links
        course_links = list(set(course_links))
        
        # Loop through each course page
        for link in course_links:
            
            # Convert relative URL to full URL
            link = response.urljoin(link)
            
            # Request course detail page
            desc_res = yield scrapy.Request(
                url=link,
                headers=self.headers,
                dont_filter=True
            )
            
            # Get course title text
            course_title = desc_res.xpath("normalize-space(//h1[@id='course_preview_title'])").get()
            
            course_code = ""
            
            # Extract course code from title (before '-')
            if course_title:
                parts = [p.strip() for p in course_title.split("-", 1)]
                if len(parts) == 2:
                    course_code = parts[0]
            
            # Store description text parts
            desc_texts = []
            
            # First try to get description near "Credit"
            credit_desc = desc_res.xpath(
                "//strong[contains(text(),'Credit')]/following-sibling::text() | "
                "//strong[contains(text(),'Credit')]/following-sibling::span//text()"
            ).getall()
            
            # If credit-based text exists, use it
            if credit_desc:
                desc_texts = credit_desc
                
            else:
                # Otherwise, get text after first <hr>
                desc_texts = desc_res.xpath(
                    "//h1[@id='course_preview_title']/following::hr[1]/following-sibling::text() | "
                    "//h1[@id='course_preview_title']/following::hr[1]/following-sibling::span//text()"
                ).getall()
            
            desc_parts = []
            
            # Clean description text
            for text in desc_texts:
                text = text.strip()
                if not text:
                    continue
                
                # Skip numeric-only junk (credits etc.)
                if re.fullmatch(r"[\d,\s]+(or\s\d+)?(-\d+)?", text):
                    continue
                
                desc_parts.append(text)
            
            # Join description into single line
            course_desc = " ".join(desc_parts)
            course_desc = re.sub(r"\s+", " ", course_desc).strip()

            self.courseData.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": desc_res.url,
                "Course Name": re.sub(r"\s+", " ", course_title).strip(),
                "Course Description": course_desc,
                "Class Number": course_code,
                "Section": "",
                "Instructor": "",
                "Enrollment": "",
                "Course Dates": "",
                "Location": "",
                "Textbook/Course Materials": ""
            })
        
        # Get next page link
        next_page = response.xpath(
            "//span[@aria-current='page']/following-sibling::a[1]/@href"
        ).get()
        
        # If next page exists, continue scraping
        if next_page:
            yield scrapy.Request(
                url=response.urljoin(next_page),
                callback=self.parse_course,
                headers=self.headers,
                dont_filter=True
            )
            return
        
        # Save course data to file
        if self.courseData:
            df = pd.DataFrame(self.courseData)
            save_df(df, self.institution_id, "course")

    def parse_directory(self):
        """
        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """
        
        # Dummy data (because no directory page exists)
        rows=[
            {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.base_url,
            "Name": "Data not found",
            "Title":"Data not found",
            "Email": "Data not found",
            "Phone Number": "Data not found",
            }
        ]
        
        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")

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
        
        # Each academic term section
        sections = response.xpath("//section[contains(@class, 'theme-default')] ")
            
        for section in sections:
            
            # Term name (example: Fall 2025)
            term_name = section.xpath(".//h2/text()").get()
            term_name = term_name.strip() if term_name else ""
            
            # Each row inside the table
            rows = section.xpath(".//table//tbody/tr")
            
            for row in rows:
                
                # First column: description
                raw_desc = row.xpath(".//td[1]//text()").getall()
                
                term_desc = re.sub(
                    r"\s*-\s*",
                    "",
                    ", ".join([
                        re.sub(r"\s+", " ", t.strip())
                        for t in raw_desc
                        if t.strip()
                    ])
                ).strip()
                term_desc = re.sub(r",\s*", ", ", term_desc).strip()
                
                # Second column: date
                raw_date = row.xpath(".//td[2]//text()").getall()
                
                term_date = re.sub(
                    r"\s+",
                    " ",
                    " ".join(t.strip() for t in raw_date if t.strip())
                ).strip()
                
                self.calendar_data.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": term_date.strip() if term_date else "",
                    "Term Date Description":  term_desc
                })
                
        cleaned_df = pd.DataFrame(self.calendar_data)
        save_df(cleaned_df, self.institution_id, "calendar")
