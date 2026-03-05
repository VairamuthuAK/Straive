import re
import time
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from ..utils import save_df
from parsel import Selector


# hCaptcha solver via 2Captcha
def parse_directory_solve_hcaptcha(target_url, site_key, api_key):
    """
    Solve hCaptcha using the 2Captcha API.

    Flow:
    1. Submit captcha task to 2Captcha
    2. Poll until solved or error
    3. Return captcha token

    Args:
        target_url (str): Page URL where captcha is present
        site_key (str): hCaptcha site key
        api_key (str): 2Captcha API key

    Returns:
        str | None: Captcha token if solved successfully
    """
    print(f"[*] Sending request to 2captcha for {target_url}...")
    
    # Initial submission endpoint
    task_url = "http://2captcha.com/in.php"

    # Required parameters for hCaptcha solving
    params = {
        'key': api_key,
        'method': 'hcaptcha',
        'sitekey': site_key,
        'pageurl': target_url,
        'json': 1
    }
    
    try:
        # Submit captcha task
        res = requests.post(task_url, data=params, timeout=30)
        if res.status_code != 200:
            print(f"[!] 2captcha returned status {res.status_code}")
            return None
            
        res_data = res.json()
        if res_data.get('status') != 1:
            print(f"[!] Solver Error: {res_data.get('request')}")
            return None
        
        # Request ID used for polling
        request_id = res_data.get('request')
        
        # Poll until captcha is solved
        while True:
            time.sleep(5)
            result_url = f"http://2captcha.com/res.php?key={api_key}&action=get&id={request_id}&json=1"
            result_res = requests.get(result_url, timeout=30)
            
            # Check if response is valid JSON
            try:
                result_data = result_res.json()
            except Exception:
                continue
            
            # Captcha solved
            if result_data.get('status') == 1:
                return result_data.get('request')
            
            # Still processing
            if result_data.get('request') == "CAPCHA_NOT_READY":
                continue
            
            # Any other error
            print(f"[!] Solver error during polling: {result_data}")
            return None
        
    except Exception as e:
        print(f"Error in solver: {e}")
        return None

# Spider
class GallaudetsSpider(scrapy.Spider):

    """
    gallaudet Scrapy Spider for Gallaudet University

    This spider supports scraping:
    1. Course schedules (Excel-based)
    2. Employee directory (with hCaptcha-protected emails)
    3. Academic calendars (PDF parsing, including special Summer sessions)
    """
    name = "gallaudets"
    institution_id = 258457486980311004

    # In-memory storage
    calendar_rows = []
    directory_rows = []
    
    # Static URLs and headers
    # Course schedules
    course_url = "https://gallaudet.edu/technology-services/gts-course-schedules/"
    course_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }

    
    # Directory AJAX endpointz
    directory_api_url = "https://gallaudet.edu/wp-admin/admin-ajax.php"
    directory_headers = {
        'accept': 'text/html, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://gallaudet.edu',
        'referer': 'https://gallaudet.edu/directory/personnel/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }
    

    # Academic calendar PDFs
    calendar_PDF_URLS = [
    "https://gallaudet.edu/wp-content/uploads/2025/05/2025-2026-Academic-Calendar_as_of_5.27.25.pdf",
    "https://gallaudet.edu/wp-content/uploads/2025/10/2026-2027-Academic-Calendar-as-of-10.21.25.pdf",
    "https://gallaudet.edu/wp-content/uploads/2025/10/Summer2026_Sessions.pdf"
    ]


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
           for page in range(1, 83):
               directory_payload = (
                    "action=get_ajax_posts"
                    "&template_part=template-parts%2Fdirectory%2Fuser-card"
                    "&main_args%5Bsection_title%5D=Personnel"
                    "&main_args%5Bsection_desc%5D=Browse+our+personnel"
                    "&main_args%5Bshow_switch%5D=false"
                    "&main_args%5Bquery_type%5D=users"
                    "&main_args%5Bpost_type%5D=user"
                    "&main_args%5Btemplate_part%5D=template-parts%2Fdirectory%2Fuser-card"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=directory"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=division"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=group"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=audience"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=subject"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=program-types"
                    "&main_args%5Brelationship_filters%5D=user_provider"
                    "&main_args%5Bsearch_placeholder%5D=Browse+our+personnel"
                    "&main_args%5Btax_query%5D="
                    "&search_data="
                    "&sort_data=ASC"
                    f"&paged_data={page}"
                    "&query_type=users"
                    )
               yield scrapy.Request(url=self.directory_api_url,method="POST",headers=self.directory_headers,body=directory_payload,callback=self.parse_directory,meta={"page": page},dont_filter=True)
        
        elif mode == 'calendar':
            for pdf_url in self.calendar_PDF_URLS:
                yield scrapy.Request(url=pdf_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)

            for page in range(1, 83):
                directory_payload = (
                    "action=get_ajax_posts"
                    "&template_part=template-parts%2Fdirectory%2Fuser-card"
                    "&main_args%5Bsection_title%5D=Personnel"
                    "&main_args%5Bsection_desc%5D=Browse+our+personnel"
                    "&main_args%5Bshow_switch%5D=false"
                    "&main_args%5Bquery_type%5D=users"
                    "&main_args%5Bpost_type%5D=user"
                    "&main_args%5Btemplate_part%5D=template-parts%2Fdirectory%2Fuser-card"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=directory"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=division"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=group"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=audience"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=subject"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=program-types"
                    "&main_args%5Brelationship_filters%5D=user_provider"
                    "&main_args%5Bsearch_placeholder%5D=Browse+our+personnel"
                    "&main_args%5Btax_query%5D="
                    "&search_data="
                    "&sort_data=ASC"
                    f"&paged_data={page}"
                    "&query_type=users"
                    )
                yield scrapy.Request(url=self.directory_api_url,method="POST",headers=self.directory_headers,body=directory_payload,callback=self.parse_directory,meta={"page": page},dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)

            for pdf_url in self.calendar_PDF_URLS:
                yield scrapy.Request(url=pdf_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            for pdf_url in self.calendar_PDF_URLS:
                yield scrapy.Request(url=pdf_url, callback=self.parse_calendar)

            for page in range(1, 83):
                directory_payload = (
                    "action=get_ajax_posts"
                    "&template_part=template-parts%2Fdirectory%2Fuser-card"
                    "&main_args%5Bsection_title%5D=Personnel"
                    "&main_args%5Bsection_desc%5D=Browse+our+personnel"
                    "&main_args%5Bshow_switch%5D=false"
                    "&main_args%5Bquery_type%5D=users"
                    "&main_args%5Bpost_type%5D=user"
                    "&main_args%5Btemplate_part%5D=template-parts%2Fdirectory%2Fuser-card"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=directory"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=division"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=group"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=audience"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=subject"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=program-types"
                    "&main_args%5Brelationship_filters%5D=user_provider"
                    "&main_args%5Bsearch_placeholder%5D=Browse+our+personnel"
                    "&main_args%5Btax_query%5D="
                    "&search_data="
                    "&sort_data=ASC"
                    f"&paged_data={page}"
                    "&query_type=users"
                    )
                yield scrapy.Request(url=self.directory_api_url,method="POST",headers=self.directory_headers,body=directory_payload,callback=self.parse_directory,meta={"page": page},dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)

            for page in range(1, 83):
                directory_payload = (
                    "action=get_ajax_posts"
                    "&template_part=template-parts%2Fdirectory%2Fuser-card"
                    "&main_args%5Bsection_title%5D=Personnel"
                    "&main_args%5Bsection_desc%5D=Browse+our+personnel"
                    "&main_args%5Bshow_switch%5D=false"
                    "&main_args%5Bquery_type%5D=users"
                    "&main_args%5Bpost_type%5D=user"
                    "&main_args%5Btemplate_part%5D=template-parts%2Fdirectory%2Fuser-card"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=directory"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=division"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=group"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=audience"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=subject"
                    "&main_args%5Btaxonomy_filters%5D%5B%5D=program-types"
                    "&main_args%5Brelationship_filters%5D=user_provider"
                    "&main_args%5Bsearch_placeholder%5D=Browse+our+personnel"
                    "&main_args%5Btax_query%5D="
                    "&search_data="
                    "&sort_data=ASC"
                    f"&paged_data={page}"
                    "&query_type=users"
                    )
                yield scrapy.Request(url=self.directory_api_url,method="POST",headers=self.directory_headers,body=directory_payload,callback=self.parse_directory,meta={"page": page},dont_filter=True)
            
            for pdf_url in self.calendar_PDF_URLS:
                yield scrapy.Request(url=pdf_url, callback=self.parse_calendar)
       

    # Parse methods UNCHANGED from your original
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
        - "Enrollment"                    : int
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """

        """
        Parse course Excel files and normalize rows.
        """

        course_rows = []
        blocks = response.xpath('//div[@id="overview"]//ul/li/a/@href').getall()
        for block in blocks:
            if 'Summer' in block:
              continue
            res = requests.get(block)
            
            excel_bytes = BytesIO(res.content)   
            df = pd.read_excel(excel_bytes, engine="openpyxl")

            df = pd.read_excel(excel_bytes)
            for _, row in df.iterrows():
               
                course_rows.append(
                    {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": res.url,
                    "Course Name": f'{row.get("Subject")} {row.get("Title")}',
                    "Course Description": row.get("Topic"),
                    "Class Number": row.get("Catalog"),
                    "Section": row.get("Section"),
                    "Instructor": row.get("Instructor"),
                    "Enrollment": f"{row.get('Current')} / {row.get('Limit')}",
                    "Course Dates": f'{row.get("Start Date")} - {row.get("End Date")}',
                    "Location": row.get('Room',''),
                    "Textbook/Course Materials": "",
                    }
                )

        # # ---------------- SAVE OUTPUT CSV----------------
        course_df = pd.DataFrame(course_rows)
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

        """
        Parse employee directory profiles and extract emails via hCaptcha.
        """
        failure_reason = ""
        SOLVER_API_KEY = "ENTER YOUR API KEY"
        SITE_KEY = "21b40520-2ae1-4fa4-9a20-3688402bf453"
        AJAX_URL = "https://gallaudet.edu/wp-admin/admin-ajax.php"
        
        session = requests.Session()
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

        listing_blocks = response.xpath('//div[@class="user row full-card-wrap"]')
        for listing in listing_blocks:
            
            links = listing.xpath('.//a[@class="d-flex"]/@href').get('')
            res = session.get(links, headers=headers)
            product_response = Selector(text=res.text)
            main_title = ''
            name = product_response.xpath('//h1[@class="entry-title"]/a/text()').get('')
            title1 = product_response.xpath('//p[@class="job-title"]/text()').get('')
            title2 = product_response.xpath('//a[@class="department"]//text()').getall()
            if title2 != []:
                title2 = ', '.join(title2)
                main_title = f'{title1} , {title2}'
            else:
                main_title = title1

            phone = ''
            phones = product_response.xpath('//div[@class="me-4 mb-0"]//p/text()').getall()
            if phones != []:
                phone = ','.join(phones)
            # --- Email Extraction via Captcha ---
            email = ""
            try:
                nonce_id = re.findall(r'"nonce":"(.*?)"', res.text)[0]
                user_id = re.findall(r'"origin_id":(.*?),', res.text)[0]
                
                # Solve Captcha
                captcha_token = parse_directory_solve_hcaptcha(links, SITE_KEY, SOLVER_API_KEY)

                if not captcha_token:
                    failure_reason = "Captcha not solved"
                    raise Exception(failure_reason)
                
                form_data = {
                    'action': (None, 'gux_get_email'),
                    'user_id': (None, str(user_id)),
                    'nonce': (None, str(nonce_id)),
                    'hcaptcha_response': (None, captcha_token)
                }
                    
                # Update referer for the AJAX call
                headers['referer'] = links
                email_res = session.post(AJAX_URL, files=form_data, headers=headers)
                email_data = email_res.json()
                if not email_data.get("success"):
                    failure_reason = "Email API failed"
                    raise Exception(failure_reason)

                email = email_data["data"]["email"]
                    
                self.directory_rows.append(
                {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": links,
                    "Name": name,
                    "Title": main_title,
                    "Email": email,
                    "Phone Number": phone,
                    }
                )
            except Exception as e:
                pass
                print(f"[FAIL] {name} | {links} |")
        


    def parse_calendar(self, response):
    
        pdf_bytes = BytesIO(response.body)

        # SPECIAL HANDLING: Summer 2026 Sessions
        if "Summer2026_Sessions" in response.url:
            EVENTS = [
                "Session Dates",
                "Class Start",
                "Deadline to drop with full refund",
                "Deadline to drop with 50% refund",
                "Deadline to drop with no refund",
                "Deadline to drop with WD refund",
                "Class End",
                "Grades Due",
            ]

            def clean(cell):
                if not cell:
                    return ""
                return " ".join(cell.replace("\n", " ").split()).strip()

            with pdfplumber.open(pdf_bytes) as pdf:
                page = pdf.pages[0]

                # Configure PDF table extraction to rely on visible ruling lines.
                # Using line-based strategies improves accuracy for structured,
                # grid-style tables commonly found in academic schedules.
                table = page.extract_table({
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
                "snap_tolerance": 4,
                "join_tolerance": 4,
                })

                for row in table:
                    if not row or not row[0]:
                        continue

                    term_code = row[0].strip()
                    if term_code not in ["5W1", "5W2", "8W1", "12W"]:
                        continue

                    termname = f"Summer 2026 {term_code}"

                    data_cells = [clean(c) for c in row[1:] if clean(c)]
                    if len(data_cells) < 8:
                        continue

                    data_cells = data_cells[:8]

                    for event, date in zip(EVENTS, data_cells):
                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": termname,
                            "Term Date": date,
                            "Term Date Description": event,
                        })

            return  #  IMPORTANT: stop here, do not run generic logic

        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                # Detect term from page header text
                text = page.extract_text() or ""
                match = re.search(r"(Fall|Spring|Summer)\s+(20\d{2})", text)
                term = None
                if match:
                    season, year = match.groups()
                    term = f"Academic Calendar {season} {year}"

                # Configure PDF table extraction to rely on visible ruling lines.
                # Using line-based strategies improves accuracy for structured,
                # grid-style tables commonly found in academic schedules.
                tables = page.extract_tables({
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "intersection_tolerance": 5
                })

                for table in tables:
                    for row in table:
                        if not row or len(row) < 2:
                            continue

                        date = (row[0] or "").strip()
                        desc = (row[1] or "").strip()

                        if not date or not desc:
                            continue

                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": term,
                            "Term Date": date,
                            "Term Date Description": desc,
                        })

      
    def closed(self, reason):

        """
        Final cleanup and persistence.

        Saves:
        - Directory dataset
        - Calendar dataset
        - Closes all file handles
        """

        if self.directory_rows:
            save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

        if self.calendar_rows:
            save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")




 