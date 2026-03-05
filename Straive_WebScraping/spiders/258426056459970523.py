# import re                     
# import json
# import scrapy             
# import pdfplumber
# import pandas as pd         
# from ..utils import *       
# from io import BytesIO    
# from urllib.parse import quote    


# # Spider definition for St. Mary's College scraping
# class StmarysSpider(scrapy.Spider):
#     name = "stmarys"

#     # Unique institution ID used for all datasets
#     institution_id = 258426056459970523 

#     # Course catalog URL with filters applied
#     course_url =  "https://catalog.stmarys-ca.edu/content.php?filter%5B27%5D=-1&filter%5B29%5D=&filter%5Bcourse_type%5D=-1&filter%5Bkeyword%5D=&filter%5B32%5D=1&filter%5Bcpage%5D=1&cur_cat_oid=21&expand=&navoid=1487&search_database=Filter&filter%5Bexact_match%5D=1"

#     # Custom request headers to mimic a real browser
#     headers = {
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#         'Referer': 'https://catalog.stmarys-ca.edu/content.php?catoid=21&navoid=1487',
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
#     }

#     # Faculty / Staff directory page
#     directory_url = 'https://www.stmarys-ca.edu/faculty-directory'

#     # Academic calendar PDF URL
#     calendar_url = "https://www.stmarys-ca.edu/sites/default/files/2025-10/2025%E2%80%932026%20Academic%20Calendar%20%28last%20revised%2010.16.2025%29.pdf"

#     # Initialize storage lists
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         # Store scraped directory data
#         self.directory_rows = []

#         # Store scraped calendar data
#         self.calendar_rows = []

#         # Store scraped course data
#         self.course_rows = []

#     # Entry Point – Select Scrape Mode
#     def start_requests(self):
#         # Read scrape mode from Scrapy settings
#         # Supported modes: course, directory, calendar, or combinations
#         mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

#         # ---- Single Mode Execution ----
#         if mode == 'course':
#             yield scrapy.Request(url=self.course_url, headers=self.headers, dont_filter=True, callback=self.parse_course)

#         elif mode == 'directory':
#             yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

#         elif mode == 'calendar':
#             yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

#         # ---- Combined Modes (Order Independent) ----
#         elif mode in ['course_directory', 'directory_course']:
#             yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
#             yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

#         elif mode in ['course_calendar', 'calendar_course']:
#             yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
#             yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

#         elif mode in ['directory_calendar', 'calendar_directory']:
#             yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
#             yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

#         # ---- Default: Scrape Everything ----
#         else:
#             yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
#             yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
#             yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

#     # Utility method to safely clean string values
#     def clean(self, value):
#         return (value or "").strip()

#     def parse_course(self, response):
#         """
#         Scrapes all course schedule data.
#         This page is dynamically generated but works with normal Scrapy requests.
#         """

#         # Extract department links from the course table
#         departments = response.xpath('//table[@class="table_default"]//tr/td/a[@target="_blank"]//@href').getall()
#         for dept in departments:
#             dept_link = f'https://catalog.stmarys-ca.edu/{dept}'
#             yield scrapy.Request(url=dept_link, callback=self.parse_department_courses, dont_filter=True)

#         # Extract total number of pagination pages
#         last_page = response.xpath("//a[contains(@aria-label,'Page')][last()]/text()").get()
#         if last_page:
#             total_pages = int(last_page)

#             # Loop through paginated course listing pages
#             for page in range(1, total_pages + 1):
#                 url = f'https://catalog.stmarys-ca.edu/content.php?catoid=21&navoid=1487&filter[cpage]={page}&filter[27]=-1&filter[32]=1&filter[exact_match]=1&filter[item_type]=3&filter[only_active]=1&filter[3]=1#acalog_template_course_filter'
#                 print(f"pagination url -----> {url}")
#                 yield response.follow(url, callback=self.parse_course)

#     def parse_department_courses(self, response):
#         # Extract course title text
#         title = response.xpath('//h1[@id="course_preview_title"]//text()').get('').replace('\xa0','').strip()

#         # Split course title to extract class number
#         parts = title.split('-')
#         class_num = parts[0].strip() if parts else ""

#         # Extract course description text
#         description = response.xpath("//h1[@id='course_preview_title'] /following::text() [following::a[contains(@class,'portfolio_link')] ]").getall()
#         description = " ".join(t.strip() for t in description if t.strip())
#         description = description.replace('Back to Top | Print-Friendly Page (opens a new window)','').strip()

#         # Append course data
#         self.course_rows.append({
#             "Cengage Master Institution ID": self.institution_id,
#             "Source URL": response.url,
#             "Course Name": re.sub(r'\s+',' ',title),
#             "Course Description": re.sub(r'\s+',' ',description),
#             "Class Number": class_num,
#             "Section": '',
#             "Instructor": '',
#             "Enrollment": '',
#             "Course Dates": '',
#             "Location": '',
#             "Textbook/Course Materials": '',
#         })

#         # Save course data incrementally
#         course_df = pd.DataFrame(self.course_rows)
#         save_df(course_df, self.institution_id, "course")

#     # DIRECTORY SCRAPER
#     def parse_directory(self, response):
#         """
#         Scrapes faculty and staff profile links.
#         """

#         # Extract profile URLs
#         rows = response.xpath('//div[@class="node__content"]//a[@class="StaffPersonName"]/@href').getall()
#         for row in rows:
#             yield scrapy.Request(url=row, callback=self.parse_directory_details, dont_filter=True)

#         # Handle pagination
#         last_page_href = response.xpath('//li[contains(@class,"pager__item--last")]/a/@href').get()
#         if last_page_href:
#             total_pages = int(last_page_href.split('=')[-1]) + 1
#             for page in range(total_pages):
#                 url = f"https://www.stmarys-ca.edu/faculty-directory?page={page}"
#                 yield response.follow(url, callback=self.parse_directory)

#     # Text cleaner utility for directory fields
#     def clean(self, text):
#         if not text:
#             return ""

#         if isinstance(text, list):
#             text = " ".join(text)

#         return text.replace('\t', '').replace('\n', '').strip()

#     def parse_directory_details(self, response):
#         # Extract faculty/staff name
#         name = response.xpath('//h1/text()').get('').strip()

#         # Primary department text
#         department1 = self.clean(response.xpath("//div[@class='details']//p/strong/text()").get())

#         # Secondary department text
#         department2 = self.clean(response.xpath("//div[@class='departments']//div/text()").get())

#         department = ""

#         # Combine department sources based on availability
#         if department1 and department2:
#             department = f"{department1}, {department2}".strip()
#         elif department2:
#             department = department2
#         else:
#             department = self.clean(
#                 response.xpath("//span[contains(text(),'Department:')]/following-sibling::text()").get()
#             )

#         # Final fallback
#         if not department:
#             department = department1

#         # Extract email with multiple fallbacks
#         email = response.xpath("//span[contains(text(),'Email:')]/following-sibling::a//text()").get('').strip()
#         if not email:
#             email = response.xpath("//strong[contains(text(),'Contact:')]/following-sibling::a//@href").get('').replace('mailto:','').strip()
#         if not email:
#             email = response.xpath("//p[contains(.,'Email')]/a/@href").get('').replace('mailto:','').strip()

#         # Extract phone number with fallbacks
#         phone = response.xpath("//span[contains(text(),'Phone:')]/following-sibling::text()").get('')
#         if phone:
#             phone = phone.strip()
#         else:
#             phone = response.xpath(
#                 "//strong[contains(text(),'Contact:')]/following-sibling::text() | "
#                 "//strong[contains(text(),'Phone:')]/following-sibling::text() | "
#                 "//p[strong[normalize-space()='Contact:']]/text()[contains(., 'Phone')] | "
#                 "//p[strong[normalize-space()='Contact:']]/text()[contains(., 'Box')]"
#             ).get('').replace('Phone:','').replace('Art Admin, Phone:','').replace('Email:','').replace('(cell)','').replace('Art Admin,','').replace('for meeting times.', '').strip()

#         # Append directory record
#         self.directory_rows.append({
#             "Cengage Master Institution ID": self.institution_id,
#             "Source URL": response.url,
#             "Name": re.sub(r'\s+',' ',name),
#             "Title": re.sub(r'\s+',' ',department),
#             "Email": email,
#             "Phone Number": phone,
#         })

#         # Save directory data incrementally
#         directory_df = pd.DataFrame(self.directory_rows)
#         save_df(directory_df, self.institution_id, "campus")

#     # CALENDAR SCRAPER
#     def parse_calendar(self, response):
#         # Load PDF into memory
#         pdf_file = BytesIO(response.body)

#         current_term = None
#         current_row = None
#         desc_lines = []

#         # Regex patterns for parsing terms and dates
#         TERM_PATTERN = re.compile(r"(Summer|Fall|Spring|Jan) Term \d{4}")
#         DATE_PATTERN = re.compile(r"([A-Z][a-z]+ \d{1,2}(?:[-–][A-Z]?[a-z]*\s?\d{1,2})?, \d{4})")

#         # Lines to ignore in the calendar PDF
#         IGNORE_PATTERN = re.compile(
#             r"(Academic Calendar|Semester System|Graduate and Undergraduate Programs|\(continued\))",
#             re.IGNORECASE
#         )

#         with pdfplumber.open(pdf_file) as pdf:
#             for page in pdf.pages:
#                 text = page.extract_text()
#                 if not text:
#                     continue

#                 for line in text.split("\n"):
#                     line = line.strip()
#                     if not line or IGNORE_PATTERN.search(line):
#                         continue

#                     # Detect academic term
#                     term_match = TERM_PATTERN.search(line)
#                     if term_match:
#                         current_term = term_match.group(0)
#                         continue

#                     # Detect date entry
#                     date_match = DATE_PATTERN.search(line)
#                     if date_match and current_term:
#                         if current_row:
#                             current_row["description"] = " ".join(desc_lines).strip()
#                             self.calendar_rows.append(current_row)

#                         date_value = date_match.group(1)
#                         desc_lines = []

#                         remaining = line.replace(date_value, "").strip(" ,–-")
#                         if remaining:
#                             desc_lines.append(remaining)

#                         current_row = {
#                             "Cengage Master Institution ID": self.institution_id,
#                             "Source URL": response.url,
#                             "Term Name": current_term,
#                             "Term Date": date_value,
#                             "Term Date Description": ""
#                         }
#                         continue

#                     # Append description lines under the same date
#                     if current_row:
#                         desc_lines.append(line)

#             # Save last calendar entry
#             if current_row:
#                 current_row["description"] = " ".join(desc_lines).strip()
#                 self.calendar_rows.append(current_row)

#         # Save calendar data
#         calendar_df = pd.DataFrame(self.calendar_rows)
#         save_df(calendar_df, self.institution_id, "calendar")
