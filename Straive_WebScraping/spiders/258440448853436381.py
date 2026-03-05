import re
import scrapy
import pandas as pd
from inline_requests import inline_requests
from ..utils import save_df


class SpccSpider(scrapy.Spider):
    
    # Spider name used while running scrapy crawl spcc
    name = "spcc"
    
    # Unique institution ID
    institution_id = 258440448853436381
    
    # List to store all records
    courseData = []
    calendar_data = []
    
    # URLs for course, directory, and calendar
    course_url = "https://catalog.spcc.edu/content.php?catoid=5&navoid=723"
    directory_url = "https://spcc.edu/alli-access-home/directory/"
    calendar_url = "https://catalog.spcc.edu/content.php?catoid=5&navoid=731"
    
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
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

    @inline_requests
    def parse_course(self, response):
        """
        This function collects all course titles and descriptions

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
        
        # Get all page numbers shown in pagination
        page_numbers = response.xpath("//td[contains(., 'Page:')]//text()[normalize-space()]").getall()
        
        # Convert page numbers to integers
        pages = [int(p) for p in page_numbers if p.strip().isdigit()]
        
        # Loop through each page
        for page in pages:
            
            # Build URL for each page
            pageUrl = f"https://catalog.spcc.edu/content.php?catoid=5&catoid=5&navoid=723&filter%5Bitem_type%5D=3&filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D={page}#acalog_template_course_filter"
            
            # Request course list page
            pageRes = yield scrapy.Request(url=pageUrl, headers=self.headers)
            
            # Extract all course detail links
            course_links = pageRes.xpath(
                "//a[contains(@href,'preview_course_nopop.php')]/@href"
            ).getall()
            
            # Loop through each course link
            for link in course_links:
                
                link = response.urljoin(link)

                # Open course detail page
                desc_res = yield scrapy.Request(
                    url=link,
                    headers=self.headers,
                    dont_filter=True
                )

                # Extract course title
                course_title = desc_res.xpath(
                    "normalize-space(//h1[@id='course_preview_title'])"
                ).get()
                
                # Extract course code from title
                course_code = ""
                
                if course_title:
                    parts = [p.strip() for p in course_title.split("-", 1)]
                    if len(parts) == 2:
                        course_code = parts[0]
                    
                # Extract all text content from course page
                texts = desc_res.xpath(
                    '//td[@class="block_content"]//text()'
                ).getall()

                # clean whitespace
                cleaned = [
                    re.sub(r"\s+", " ", t).strip()
                    for t in texts
                    if t and t.strip()
                ]

                description_parts = []
                collecting = False
                
                # Loop through cleaned text to build description
                for t in cleaned:
                    
                    # skip junk + pipe
                    if t in ("|",):
                        continue
                    
                    # skip obvious junk
                    if any(j in t for j in (
                        "HELP",
                        "Print-Friendly",
                        "Back to Top",
                        "Catalog",
                        "Handbook"
                    )):
                        continue

                    # start collecting AFTER prereq / credits
                    if (
                        "course" in t.lower()
                        or t.lower().startswith("this seminar")
                        and not t.lower().startswith("prerequisite")
                        and not t.lower().startswith("corequisite")
                    ):
                        collecting = True

                    # stop when Offered appears
                    if t.startswith("Offered:"):
                        break

                    if collecting:
                        description_parts.append(t)
                
                # Join description text
                description = " ".join(description_parts)
                description = re.sub(r"\s+", " ", description).strip()
                
                # Save one course record
                self.courseData.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": desc_res.url,
                    "Course Name": course_title,
                    "Course Description": description,
                    "Class Number": course_code,
                    "Section": "",
                    "Instructor": "",
                    "Enrollment": "",
                    "Course Dates": "",
                    "Location": "",
                    "Textbook/Course Materials": ""
                })
        
        # Save all course data
        if self.courseData:
            df = pd.DataFrame(self.courseData)
            save_df(df, self.institution_id, "course")


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
        
        # Get all rows from directory table
        rows = response.xpath("//table[@id='tablepress-21']//tbody/tr[starts-with(@class,'row-')]")

        directory_data = []
        
        # Loop through each staff row
        for row in rows:
            last = row.xpath("./td[1]/text()").get()
            first = row.xpath("./td[2]/text()").get()
            title = row.xpath("./td[3]/text()").get()
            email = row.xpath("./td[4]/text()").get()
            phone = row.xpath("./td[5]/text()").get()
            
            # Combine first and last name
            name = " ".join(
                t.strip() for t in [first, last] if t and t.strip()
            )
            
            # Save one staff record
            directory_data.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title.strip() if title else "",
                "Email": email.strip() if email else "",
                "Phone Number": phone.strip() if phone else ""
            })
            
        # Save directory data
        df = pd.DataFrame(directory_data)
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
        
        # Find all term headers (Semester / Term)
        terms = response.xpath(
            "//h2[contains(normalize-space(.), 'Semester') or contains(normalize-space(.), 'Term')]"
        )
        
        # Loop through each term section
        for term in terms:
            term_name = term.xpath("normalize-space(.)").get()

            tables = []

            # Find tables belonging to this term
            for sib in term.xpath("following-sibling::*"):
                if sib.root.tag == "h2":
                    break  # STOP at next term

                if sib.root.tag == "table":
                    tables.append(sib)
            
            # Loop through each table
            for table in tables:
                
                rows = table.xpath(".//tbody/tr")
                
                # Loop through each calendar row
                for row in rows:
                    date = row.xpath("normalize-space(td[1])").get()
                    desc = row.xpath("normalize-space(td[2])").get()

                    if not date or not desc:
                        continue

                    self.calendar_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": date,
                        "Term Date Description": desc
                    })

        df = pd.DataFrame(self.calendar_data)
        save_df(df, self.institution_id, "calendar")
