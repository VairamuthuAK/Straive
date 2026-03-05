import io
import scrapy
import pdfplumber
import unicodedata
import pandas as pd
from ..utils import *
from inline_requests import inline_requests

class AugieSpider(scrapy.Spider):

    name = "augie"
    institution_id = 258455679445002202
    
    course_rows = []
    course_url = ["https://augie.smartcatalogiq.com/Institutions/Augustana/json/2025-2026/2025-2026-Graduate-Catalog.json","https://augie.smartcatalogiq.com/Institutions/Augustana/json/2025-2026/2025-2026-Undergraduate-Catalog.json"]
    

    directory_url = "https://www.augie.edu/directory"

    calendar_url = "https://www.augie.edu/academics/academic-calendars"

    def start_requests(self):

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        # Single functions
        if mode == "course":
            rows=[]
            for url in self.course_url:
                referrer=""
                if "Graduate-Catalog" in url:
                    referrer = 'https://augie.smartcatalogiq.com/en/2025-2026/2025-2026-graduate-catalog/graduate-courses'
                else:
                    referrer ="https://augie.smartcatalogiq.com/en/2025-2026/2025-2026-undergraduate-catalog/courses"
                course_headers = {
                'sec-ch-ua-platform': '"Windows"',
                'Referer': referrer,
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/javascript, */*; q=0.01'
                }
                yield scrapy.Request(url,headers=course_headers, callback=self.parse_course,meta={"rows":rows}, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            rows=[]
            for url in self.course_url:
                referrer=""
                if "Graduate-Catalog" in url:
                    referrer = 'https://augie.smartcatalogiq.com/en/2025-2026/2025-2026-graduate-catalog/graduate-courses'
                else:
                    referrer ="https://augie.smartcatalogiq.com/en/2025-2026/2025-2026-undergraduate-catalog/courses"
                course_headers = {
                'sec-ch-ua-platform': '"Windows"',
                'Referer': referrer,
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/javascript, */*; q=0.01'
                }
                yield scrapy.Request(url,headers=course_headers, callback=self.parse_course,meta={"rows":rows}, dont_filter=True)

            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            rows=[]
            for url in self.course_url:
                referrer=""
                if "Graduate-Catalog" in url:
                    referrer = 'https://augie.smartcatalogiq.com/en/2025-2026/2025-2026-graduate-catalog/graduate-courses'
                else:
                    referrer ="https://augie.smartcatalogiq.com/en/2025-2026/2025-2026-undergraduate-catalog/courses"
                course_headers = {
                'sec-ch-ua-platform': '"Windows"',
                'Referer': referrer,
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/javascript, */*; q=0.01'
                }
                yield scrapy.Request(url,headers=course_headers, callback=self.parse_course,meta={"rows":rows}, dont_filter=True)

            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # All three (default)
        else:
            rows=[]
            for url in self.course_url:
                referrer=""
                if "Graduate-Catalog" in url:
                    referrer = 'https://augie.smartcatalogiq.com/en/2025-2026/2025-2026-graduate-catalog/graduate-courses'
                else:
                    referrer ="https://augie.smartcatalogiq.com/en/2025-2026/2025-2026-undergraduate-catalog/courses"
                course_headers = {
                'sec-ch-ua-platform': '"Windows"',
                'Referer': referrer,
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/javascript, */*; q=0.01'
                }
                yield scrapy.Request(url,headers=course_headers, callback=self.parse_course,meta={"rows":rows}, dont_filter=True)

            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)


    @inline_requests
    def parse_course(self,response):
        
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
        - "Enrollment"                    : int
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """

        rows = response.meta["rows"]
        json_datas = response.json()
        links = json_datas.get('Children',[])
        for link in links:
            
            name =link.get('Name','').strip()
            if name == "Graduate Courses" or name == "Courses":
                chil_blocks = link.get('Children',[])
                
                for block in chil_blocks:
                    url = f"https://augie.smartcatalogiq.com/en{block.get('Path','').strip()}".lower()
                    headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Referer': f"https://augie.smartcatalogiq.com/en{block.get('Path','').strip()}",
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    }

                    response=yield scrapy.Request(url,headers=headers,dont_filter=True)
                    blogs = response.xpath('//div[@class="courselist"]/h2')
                    for blog in blogs:
                        name = blog.xpath('./a/span/text() | ./a/text()').getall()
                        desc =blog.xpath('./following-sibling::div[@class="desc"]/text()').get('').strip()
                        if desc:
                            desc =desc
                        elif desc == "":
                            desc = blog.xpath('./following-sibling::div[@class="desc"]/p/text()').get("").strip()
                            if desc == "":
                                desc = blog.xpath('./following-sibling::div[@class="desc"]/span/text()').get("").strip()
                                if desc == "":
                                    desc = blog.xpath('./following-sibling::div[@class="desc"]/span/span/text()').get("").strip()
                                    if desc =="":
                                        desc = blog.xpath('./following-sibling::div[@class="desc"]/p/span/text()').get("").strip()

                        rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": f"https://augie.smartcatalogiq.com{blog.xpath('./a/@href').get().strip()}",
                        "Course Name": ' -'.join(name),
                        "Course Description": unicodedata.normalize("NFKD", desc).encode("ascii", "ignore").decode(),
                        "Class Number": name[0],
                        "Section": '',
                        "Instructor": '',
                        "Enrollment": '',
                        "Course Dates": '',
                        "Location": '',
                        "Textbook/Course Materials": '',
                    })
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

        last_page = int(response.xpath('//li[@class="pager-item pager-item__last"]/a/@href').get("").strip().split("=")[-1])
        rows=[]
        for page in range(0,last_page+1):
            url = f"https://www.augie.edu/directory?page={page}"
            response=yield scrapy.Request(url,dont_filter=True)

            blocks = response.xpath('//div[@class="profile-wrap"]')
            for block in blocks:
                email_part_1 = block.xpath('.//div[@class="profile-content-footer"]/a/@data-name').get('').strip()
                email_part_2 = block.xpath('.//div[@class="profile-content-footer"]/a/@data-domain').get('').strip()
                email_part_3 = block.xpath('.//div[@class="profile-content-footer"]/a/@data-tld').get('').strip()
                email = f"{email_part_1}@{email_part_2}{email_part_3}"
                if email_part_1 and email_part_2 and email_part_3:
                    email = f"{email_part_1.strip()}@{email_part_2.strip()}.{email_part_3.strip()}"
                else:
                    email = ""
                text = block.xpath('./div[@class="profile-content"]/p/text()').getall()
                text = " ".join([t.strip() for t in text if t.strip])
                name = block.xpath('.//h2/a/text()').get('').strip()
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": f"https://www.augie.edu{block.xpath('.//h2/a/@href').get('').strip()}",
                    "Name": unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode(),
                    "Title": re.sub(r'\s+,', ',', re.sub(r'\s+', ' ', text)).strip(),
                    "Email":email,
                    "Phone Number": block.xpath('.//div[@class="profile-content-footer"]/p/a/text()').get("").strip(),
                })
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")


    @inline_requests
    def parse_calendar(self,response):

        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
         
        links = response.xpath('//section[@class="text-block body-text"]/blockquote/ul/li/a[contains(text(),"2025") or contains(text(),"2026")]/@href').getall()
        rows=[]
        for link in links:
            response = yield scrapy.Request(link,dont_filter=True)

            # Reading the pdf response using pdfplumber with the help of io
            with pdfplumber.open(io.BytesIO(response.body)) as pdf:
                
                for page in pdf.pages:

                    # Extract tables using line-based detection
                    tables = page.extract_tables(
                        table_settings={
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "intersection_tolerance": 5,
                        }
                    )
                    term_name=None
                    for table in tables:
                        for row in table:
                            if not row or all(cell is None or cell.strip() == "" for cell in row):
                                continue  # skip fully empty rows
                            
                            # Skip header row containing "Academic Calendar"
                            if any(cell and "Academic Calendar" in cell for cell in row):
                                continue

                            # If row has a None in the second column, it's a term name
                            if row[1] is None or row[1].strip() == "":
                                term_name = row[0].strip()
                                continue

                            term_date = row[1].strip()
                            term_desc = row[0].strip()
                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Term Name":term_name ,
                                "Term Date": term_date,
                                "Term Date Description": term_desc,
                            })
        if rows:
            calendar_df = pd.DataFrame(rows)
            save_df(calendar_df, self.institution_id, "calendar")
 