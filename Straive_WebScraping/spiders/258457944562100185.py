import io
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class LenoirSpider(scrapy.Spider):

    name = "ir"
    institution_id = 258457944562100185
    course_rows = []
    course_url = ["https://www.lr.edu/degree-program-finder?category\\[\\]=166&type=undergraduate&page=1&data=1","https://www.lr.edu/degree-program-finder?type=graduate&page=2&data=1"]
    
    course_headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'priority': 'u=1, i',
        'referer': 'https://www.lr.edu/degree-program-finder?category[]=166&type=undergraduate&page=1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        }


    directory_url = "https://www.lr.edu/about/directory?type=people&page=1&data=1"
    directory_headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'priority': 'u=1, i',
        'referer': 'https://www.lr.edu/about/directory?type=people&page=1&data=1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
        }



    calendar_url = "https://www.lr.edu/academics/registrar/academic-calendars"

    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            for link in self.course_url:
                if "undergraduate" in link:
                    referrer = "https://www.lr.edu/degree-program-finder?category[]=166&type=undergraduate&page=1"
                else:
                    referrer = 'https://www.lr.edu/degree-program-finder?type=graduate&page=1'
                course_headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=1, i',
                'referer': referrer,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
                yield scrapy.Request(url=link,headers=course_headers, callback=self.parse_course, dont_filter=True,meta={'rows':self.course_rows})

        elif mode == "directory":
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        # If only calendar data is required
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            for link in self.course_url:
                if "undergraduate" in link:
                    referrer = "https://www.lr.edu/degree-program-finder?category[]=166&type=undergraduate&page=1"
                else:
                    referrer = 'https://www.lr.edu/degree-program-finder?type=graduate&page=1'
                course_headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=1, i',
                'referer': referrer,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
                yield scrapy.Request(url=link,headers=course_headers, callback=self.parse_course, dont_filter=True,meta={'rows':self.course_rows})

            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            for link in self.course_url:
                if "undergraduate" in link:
                    referrer = "https://www.lr.edu/degree-program-finder?category[]=166&type=undergraduate&page=1"
                else:
                    referrer = 'https://www.lr.edu/degree-program-finder?type=graduate&page=1'
                course_headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=1, i',
                'referer': referrer,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
                yield scrapy.Request(url=link,headers=course_headers, callback=self.parse_course, dont_filter=True,meta={'rows':self.course_rows})

            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # All three (default)
        else:
            for link in self.course_url:
                if "undergraduate" in link:
                    referrer = "https://www.lr.edu/degree-program-finder?category[]=166&type=undergraduate&page=1"
                else:
                    referrer = 'https://www.lr.edu/degree-program-finder?type=graduate&page=1'
                course_headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=1, i',
                'referer': referrer,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
                yield scrapy.Request(url=link,headers=course_headers, callback=self.parse_course, dont_filter=True,meta={'rows':self.course_rows})

            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    @inline_requests
    def parse_course(self,response):

        """
        Scrapes undergraduate and graduate program data from the Degree Program Finder.

        This method paginates through program listing pages, retrieves individual
        program detail pages via inline requests, extracts program descriptions
        and metadata, and stores structured course data for export.

        Workflow:
        ---------
        1. Determines whether the request is for undergraduate or graduate programs
        based on the response URL.
        2. Extracts the total number of pagination pages.
        3. Iterates through each page using AJAX-based requests.
        4. Extracts program cards from listing responses.
        5. Sends a follow-up request to each program detail page.
        6. Extracts:
            - Program title
            - Program description (multiple fallback strategies)
            - Contact/Instructor name
        7. Cleans HTML using regex substitutions to normalize text.
        8. Appends structured records to `self.course_rows`.

        Description Extraction Strategy:
        --------------------------------
        - Primary: Regex extraction from `user-markup` and `intro` sections.
        - Fallback 1: Paragraphs with inline style attributes.
        - Fallback 2: Generic `user-markup` paragraph extraction.
        - Additional handling when extracted content equals "Learn More".

        Extracted Fields:
        -----------------
        - Cengage Master Institution ID
        - Source URL
        - Course Name
        - Course Description
        - Instructor
        - Other standard course metadata fields (left blank if unavailable)

        Args:
        -----
        response (scrapy.http.Response):
            The listing page response containing pagination metadata.

        Returns:
        --------
        None
            Data is accumulated in `self.course_rows` and saved when the spider closes.

        Notes:
        ------
        - Uses `@inline_requests` to synchronously fetch detail pages.
        - Relies heavily on regex-based HTML cleanup.
        - Designed specifically for AJAX pagination endpoints (`data=1`).
        - Final data export occurs in the `closed()` method.
        """
    
        is_undergrad = "undergraduate" in response.url
        count = response.xpath('//li[@class="pagination__item"]/a/@data-page').getall()
        count = max(map(int, count))

        for page in range(1,count+1):
            if is_undergrad:
                url = f"https://www.lr.edu/degree-program-finder?category\\[\\]=166&type=undergraduate&page={page}&data=1"
                referer = f"https://www.lr.edu/degree-program-finder?category[]=166&type=undergraduate&page={page}"
                headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=1, i',
                'referer': referer,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
            else:
                url = f"https://www.lr.edu/degree-program-finder?type=graduate&page={page}&data=1"
                referer = "https://www.lr.edu/degree-program-finder?type=graduate&page={page}"
                headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=1, i',
                'referer': referer,
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
            
            page_response = yield scrapy.Request(url=url,headers=headers,dont_filter=True)
            blocks = page_response.xpath('//div[@class="card card--program"]')

            for block in blocks:
                url =f"https://www.lr.edu{block.xpath('./a/@href').get('').strip()}"
                detail_response = yield scrapy.Request(url,headers=headers,dont_filter=True)
                desc=""
                if re.search(r"class\=\"user\-markup\"\>[\W\w]*?class\=\"intro\"[\W\w]*?<\/p>\s*<p>([\W\w]*?)<\/div>",detail_response.text):
                    desc = re.findall(r"class\=\"user\-markup\"\>[\W\w]*?class\=\"intro\"[\W\w]*?<\/p>\s*<p>([\W\w]*?)<\/div>",detail_response.text)[0]
                    desc = re.sub(r"</p>","",desc)
                    desc = re.sub(r"<p>","",desc)
                    desc = re.sub(r"\n","",desc)
                    desc = re.sub(r"\r","",desc)
                    desc = re.sub(r"<span>","",desc)
                    desc = re.sub(r"</span>","",desc)
                    desc = re.sub(r"<a>","",desc)
                    desc = re.sub(r"</a>","",desc)
                    desc = re.sub(r"<a.*?>","",desc)
                    desc = re.sub(r"<em>","",desc)
                    desc = re.sub(r"</em>","",desc)
                    desc = re.sub(r"<ul>","",desc)
                    desc = re.sub(r"</ul>","",desc)
                    desc = re.sub(r"<li.*?>","",desc)
                    desc = re.sub(r"</li>","",desc)
                    desc = re.sub(r"</strong>","",desc)
                    desc = re.sub(r"<br>","",desc)
                    desc = re.sub(r"<img.*?>","",desc)
                    desc = re.sub(r"<div.*?>","",desc)
                    desc = re.sub(r"<strong.*?>","",desc)
                    desc = re.sub(r"\s+"," ",desc)
                
                elif desc=="":
                    desc = " ".join(detail_response.xpath('//p[@style="margin-bottom:11px"]/text()').getall()).strip()
                    desc = re.sub(r"\n","",desc)
                    desc = re.sub(r"\r","",desc)
                    desc = re.sub(r"<a.*?>","",desc)
                    desc = re.sub(r"<em>","",desc)
                    desc = re.sub(r"</em>","",desc)
                    desc = re.sub(r"\s+"," ",desc)
                    if desc =="":
                        desc =" ".join(detail_response.xpath('//div[contains(@class,"user-markup")]/p/text()').getall()).strip()
                        desc = re.sub(r"\n","",desc)
                        desc = re.sub(r"\r","",desc)
                        desc = re.sub(r"<a.*?>","",desc)
                        desc = re.sub(r"<em>","",desc)
                        desc = re.sub(r"</em>","",desc)
                        desc = re.sub(r"\s+"," ",desc)
                elif desc == "Learn More":
                    if re.search(r"class\=\"user\-markup\"\>[\W\w]*?class\=\"intro\"[\W\w]*?<\/p>[\W\w]*?\<div[\W\w]*?\s*\>[\W\w]*?<\/div\>[\W\w]*?\<p>([\W\w]*?)<\/div>",detail_response.text):
                        desc = re.findall(r"class\=\"user\-markup\"\>[\W\w]*?class\=\"intro\"[\W\w]*?<\/p>[\W\w]*?\<div[\W\w]*?\s*\>[\W\w]*?<\/div\>[\W\w]*?\<p>([\W\w]*?)<\/div>",detail_response.text)[0]
                        desc = re.sub(r"</p>","",desc)
                        desc = re.sub(r"<p>","",desc)
                        desc = re.sub(r"\n","",desc)
                        desc = re.sub(r"\r","",desc)
                        desc = re.sub(r"<span>","",desc)
                        desc = re.sub(r"</span>","",desc)
                        desc = re.sub(r"<a>","",desc)
                        desc = re.sub(r"</a>","",desc)
                        desc = re.sub(r"<a.*?>","",desc)
                        desc = re.sub(r"<em>","",desc)
                        desc = re.sub(r"</em>","",desc)
                        desc = re.sub(r"<ul>","",desc)
                        desc = re.sub(r"</ul>","",desc)
                        desc = re.sub(r"<li.*?>","",desc)
                        desc = re.sub(r"</li>","",desc)
                        desc = re.sub(r"</strong>","",desc)
                        desc = re.sub(r"<br>","",desc)
                        desc = re.sub(r"<img.*?>","",desc)
                        desc = re.sub(r"<div.*?>","",desc)
                        desc = re.sub(r"<strong.*?>","",desc)
                        desc = re.sub(r"\s+"," ",desc)


                self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": detail_response.url,
                        "Course Name": detail_response.xpath('//h1/text()').get('').strip(),
                        "Course Description": re.sub(r'<.*?>','',desc),
                        "Class Number": "",
                        "Section": "",
                        "Instructor": detail_response.xpath('//h2[@class="contact-info__name h3"]/text()').get("").strip(),
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": '',
                    })

    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")
    
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

        count = response.xpath('//li[@class="pagination__item"]/a/@data-page').getall()
        count = max(map(int, count))
        rows=[]

        for page in range(1,count+1):
            url = f"https://www.lr.edu/about/directory?type=people&page={page}&data=1"
            headers = {
                'accept': '*/*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=1, i',
                'referer': f'{url}',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest'
                }
            response = yield scrapy.Request(url=url,headers=headers,dont_filter=True)
            links = response.xpath('//div[contains(@class,"card--faculty")]/figure/a/@href').getall()

            for link in links:
                url =f"https://www.lr.edu{link}"
                response = yield scrapy.Request(url=url,headers=headers,dont_filter=True)
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": response.xpath('//h2/text()').get("").strip(),
                    "Title":response.xpath('//address[@class="contact-info__address"]/text()').get("").strip(),
                    "Email":response.xpath('//li[@class="contact-info__item"][1]/a/span[@class="text"]/text()').get("").strip(),
                    "Phone Number": response.xpath('//li[@class="contact-info__item"][2]/a/span[@class="text"]/text()').get("").strip(),
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
        
        rows=[]
        links = response.xpath('//div[@class="user-markup"]//li/a[contains(@title,"2025-2026") or contains(@title,"2026-2027")]/@href').getall()
        for link in links:
            url=f"https://www.lr.edu{link}"
            response = yield scrapy.Request(url=url,dont_filter=True)
            with pdfplumber.open(io.BytesIO(response.body)) as pdf:
                
                for page in pdf.pages:
                    text = page.extract_text()
                    lines = text.split("\n")
                    current_term=None

                    for line in lines:
                        if "Academic Calendar" in line:
                            continue
                        if "Updated" in line:
                            continue
                        term_re = re.compile(r'(FALL|SPRING|SUMMER|WINTER)\s+\d{4}', re.I)
                        term_match = term_re.search(line)
                        if term_match:
                            current_term = term_match.group(0)

                        date_re = re.compile(r'^([A-Z][a-z]+\s+\d{1,2}(?:-\d{1,2})?(?:,\s*\w+(?:-\w+)?)?)\s+(.*)$', re.I)     
                        term_matches = date_re.search(line)
                        term_date=""
                        term_desc=""
                        if term_matches:    
                            term_date = term_matches.group(1)
                            term_desc = term_matches.group(2)
                        
                        rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Term Name": current_term,
                                    "Term Date": term_date,
                                    "Term Date Description": term_desc.replace('–','-')
                                })
                        
        if rows:
                calendar_df = pd.DataFrame(rows)
                save_df(calendar_df, self.institution_id, "calendar")
