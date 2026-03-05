import io
import re
import math
import scrapy
import requests
import pdfplumber
from ..utils import *
from parsel import Selector


def xpath_literal(s):
    
    """
    Safely converts a Python string into a valid XPath string literal.

    Logic:
    - If the string contains no double quotes, it is wrapped in double quotes.
    - If the string contains no single quotes, it is wrapped in single quotes.
    - If the string contains both, an XPath concat() expression is generated.

    """
        
    if '"' not in s:
        return f'"{s}"'
    if "'" not in s:
        return f"'{s}'"
    return "concat(" + ", ".join(f'"{p}"' for p in s.split('"')) + ")"


def clean_title(title: str) -> str:
    """
    Cleans and normalizes course titles.
    """

    title = title.strip()
    
    # Remove credit information (e.g., "3 credits")
    title = re.sub(r'\b\d+\s*credits?\b', '', title, flags=re.I)

    # Drop titles that are only prerequisites
    if re.match(r'^(or\s+appropriate|appropriate\s+placement|placement\s+test|prerequisite)',title,flags=re.I):
        return ""

   # Remove trailing term codes like (F), (SP)
    title = re.sub(r'\s*\([A-Z,\s]+\)$', '', title)

    # Normalize spaces
    title = re.sub(r'\s{2,}', ' ', title)

    return title.strip()


def clean_description(text):
    """
    Cleans course descriptions by removing:
    - Credit headers
    - Next-course bleed
    - Junk phrases
    """

    # Remove leading phrases ending with "X credits"
    text = re.sub(r'^\s*(?:[A-Za-z ,&/-]+?\s+)?\d+\s*(?:–|-)?\s*\d*\s*credits?\b\.?\s*','',text,flags=re.I)

    # Remove lines that look like new course headers
    text = re.sub(r'^\s*[A-Z]{2,4}\s?\d{3}\b.*$','',text,flags=re.MULTILINE)

    # Remove junk phrases but keep prerequisites
    text = re.sub(r'or instructor approval|or appropriate math|;?\s*co-?requisite:?|co-requisite:','',text,flags=re.I)

    # Normalize whitespace
    text = re.sub(r'\s{2,}', ' ', text)

    return text.strip()

COURSE_CODE_RE = re.compile(
    r'(?m)^(?:\s*([A-Z]{2,4})\s*([0-9]{2,3}[A-Z]?)\s*(.+)|[^A-Z\n]{0,3}([A-Z]{2,4})\s*([0-9]{2,3}[A-Z]?)\b)'
)

def is_junk_course_name(name: str) -> bool:

    """
    Detects and filters out non-course titles.
    """

    name = name.strip().lower()

    # Multiple course codes merged together
    if re.fullmatch(r'(?:[A-Z]{2,3}\d{3}[A-Z]?\s+){1,}[A-Z]{2,3}\d{3}[A-Z]?',name):
        return True

    if name in {
        "or instructor approval",
        "or appropriate math",
        "co-requisite",
        "co-requisite:",
        "; co-requisite:",
    }:
        return True

    return False
    

def extract_courses(text):
    """
    Extracts structured course data from raw PDF text.
    """

    # Remove MOTR equivalency blocks
    text = re.sub(r'MOTR\s+Equivalent:.*?(?=\n[A-Z]{2,4}\s*[0-9]{2,3}[A-Z]?\b|\Z)','',text,flags=re.DOTALL | re.IGNORECASE)
    courses = []
    matches = list(COURSE_CODE_RE.finditer(text))

    for i, m in enumerate(matches):

        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[start:end].strip()
        lines = [l.strip() for l in block.splitlines() if l.strip()]
        if not lines:
            continue

        if m.group(1) and m.group(2):
            subject = m.group(1)
            number = m.group(2)
        else:
            subject = m.group(4)
            number = m.group(5)

        code = f"{subject}{number}"
        header = lines[0]

        # Extract title from header
        title_candidate = re.sub(r'^[^A-Z\n]{0,3}[A-Z]{2,4}\s*[0-9]{2,3}[A-Z]?\s*','',header).strip()

        if title_candidate:
            title = clean_title(title_candidate)
        else:
            title = clean_title(lines[1]) if len(lines) > 1 else ""

        # Skip when class number and title was lesser than lenght of 5
        if code =="" and len(title) < 5:
            continue

        description = clean_description(" ".join(lines[1:]))

        courses.append({
            "Class Number": code,
            "Course Name": title,
            "Course Description": description
        })

    return courses


class NcmissouriSpider(scrapy.Spider):

    name="ncmi"
    institution_id = 258428488753637339

    # course url and headers
    course_url = "https://www.ncmissouri.edu/academics/college-catalog/"
    course_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',        
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }
    
    # directory url and headers
    directory_rows=[]
    completed_directories = set()
    directory_source_url = ["https://www.ncmissouri.edu/directory/search-result/?directory_type=general&q=&in_cat=12","https://www.ncmissouri.edu/directory/search-result/?directory_type=department-search"]
    directory_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'referer': 'https://www.ncmissouri.edu/directory/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }


    # calendar url and headers
    calendar_url = "https://www.ncmissouri.edu/academics/academic-calendar/"
    calendar_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            for directory_url in self.directory_source_url:
                directory_type = re.findall(r"directory\_type\=([^&]+)",directory_url)[0]
                yield scrapy.Request(url=directory_url,headers=self.directory_headers,callback=self.parse_directory,meta={"directory_type":directory_type})
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers,callback=self.parse_calendar)
        

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            for directory_url in self.directory_source_url:
                directory_type = re.findall(r"directory\_type\=([^&]+)",directory_url)[0]
                yield scrapy.Request(url=directory_url,headers=self.directory_headers,callback=self.parse_directory,meta={"directory_type":directory_type})
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers,callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            for directory_url in self.directory_source_url:
                directory_type = re.findall(r"directory\_type\=([^&]+)",directory_url)[0]
                yield scrapy.Request(url=directory_url,headers=self.directory_headers,callback=self.parse_directory,meta={"directory_type":directory_type})

            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers,callback=self.parse_calendar)
        

        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            for directory_url in self.directory_source_url:
                directory_type = re.findall(r"directory\_type\=([^&]+)",directory_url)[0]
                yield scrapy.Request(url=directory_url,headers=self.directory_headers,callback=self.parse_directory,meta={"directory_type":directory_type})

            yield scrapy.Request(url=self.calendar_url,headers=self.calendar_headers,callback=self.parse_calendar)

            
    def parse_course(self, response):

        """
        Parses the course catalog PDF and extracts unique courses.

        Workflow:
        1. Fetch catalog PDF link
        2. Read PDF using pdfplumber
        3. Process pages starting from page 122
        4. Split each page into two columns
        5. Extract and clean course data
        6. Remove duplicates
        7. Save final data to DataFrame
        """

        # Track unique courses (by class number + name)
        seen = set()

         # Track unique class numbers to avoid duplicates
        seen_class_numbers = set()

        pdf_link = response.xpath(
            '//div[@class="elementor-button-wrapper"]/a[@title="2025-2026 Catalog PDF"]/@href'
        ).get("")

        response = requests.get(pdf_link, timeout=30)
        all_events = []

        # Read PDF pages
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:

                # Skip non-course pages
                if page.page_number < 122:
                    continue

                width, height = page.width, page.height
                
                # Split page into two columns
                # columns = [page.crop((0, 0, width / 2, height)),page.crop((width / 2, 0, width, height))]
                columns = [
                    page.crop((0, 0, width / 4, height)),
                page.crop((width / 4, 0, width / 2, height)),
                page.crop((width / 2, 0, 3 * width / 4, height)),
                page.crop((3 * width / 4, 0, width, height)),
            ]

                

                for col in columns:
                    text = col.extract_text()
                    if not text:
                        continue
                    
                    # Normalize line endings
                    text = text.replace("\r", "")

                    # Extract structured course blocks
                    extracted = extract_courses(text)

                    for c in extracted:

                        # Skip junk or empty entries
                        if is_junk_course_name(c["Course Name"]) or c["Course Description"] =="" :
                            continue

                        key = (c["Class Number"], c["Course Name"].lower())

                        if key in seen:
                            continue

                        seen.add(key)
                        class_number = c["Class Number"]

                        if class_number in seen_class_numbers:
                            print("skiped class name",class_number)
                            continue   # skip duplicate

                        seen_class_numbers.add(class_number)

                        all_events.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_link,
                            "Course Name": c["Class Number"] +" "+ c["Course Name"],
                            "Course Description": c["Course Description"],
                            "Class Number": class_number,
                            "Section": "",
                            "Instructor": "",
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": "",
                            "Textbook/Course Materials": "",
                        })


        if all_events:
            course_df = pd.DataFrame(all_events)
            save_df(course_df, self.institution_id, "course")


    def parse_directory(self, response):
        """
        Parses the staff/faculty directory by:
        1. Extracting AJAX nonce from page source
        2. Calculating total pages
        3. Sending AJAX POST requests page-by-page
        4. Extracting name, title, email, phone
        5. Saving final data once all directories are processed
        """
        
        # Extract ajax nonce value required for POST requests
        ajax_nonce=""
        nonce=""
        if re.search(r"var\s*directorist\s*\=\s*([^>]*?)<\/script>",response.text):
            ajax_nonce = re.findall(r"var\s*directorist\s*\=\s*([^>]*?)<\/script>",response.text)[0]
            nonce = re.findall(r'ajaxnonce":"(.*?)"\}', ajax_nonce)[0]

        directory_type = response.meta["directory_type"]

        # Total number of records shown on page
        number_of_records = int(response.xpath('//span[@class="directorist-header-found-title"]/span/text()').get(default=0))

        # Each page contains 6 records
        number_of_pages = math.ceil(number_of_records / 6)

        for page in range(1, number_of_pages + 1):
            url="https://www.ncmissouri.edu/directory/wp-admin/admin-ajax.php"

            # Payload differs slightly for "general" directory
            if directory_type == "general":
                payload = f"paged={page}&in_cat=12&directory_type={directory_type}&action=directorist_instant_search&_nonce={nonce}&current_page_id=2684&data_atts%5B_current_page%5D=search_result"
            else:
                payload = f"paged={page}&directory_type={directory_type}&action=directorist_instant_search&_nonce={nonce}&current_page_id=2684&data_atts%5B_current_page%5D=search_result"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"https://www.ncmissouri.edu/directory/search-result/?directory_type={directory_type}&paged={page}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                }

            response = requests.post(url, data=payload, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()

                # Load returned HTML fragment
                sel = Selector(text=data.get("render_listings", ""))

                for blog in sel.xpath('//article[contains(@class,"directorist-listing-single")]'):
                    raw = blog.xpath('.//h2/a/text()').get("").replace("–", "-").strip()
                    parts = [p.strip() for p in raw.split("-", 1)]
                    phones = ",".join(blog.xpath('.//li[contains(@class,"directorist-listing-card-phone")]/a/text()').getall())
                    self.directory_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": blog.xpath('.//h2/a/@href').get("").strip(),
                        "Name": parts[0],
                        "Title": parts[1] if len(parts) > 1 else "",
                        "Email": blog.xpath(
                            './/li[@class="directorist-listing-card-email"]/a/text()'
                        ).get("").strip(),
                        "Phone Number": re.sub(r'\s+', ' ', phones).strip(),
                    })
                    
        self.completed_directories.add(directory_type)
        if len(self.completed_directories) == len(self.directory_source_url):
            directory_df = pd.DataFrame(self.directory_rows)
            save_df(directory_df, self.institution_id, "campus")


        

    def parse_calendar(self,response):
        """
        Parses the academic calendar page and extracts:
        - Term name
        - Term date
        - Term date description

        The data is stored in a DataFrame and saved once parsing is complete.
        """

        rows=[]
        blogs = response.xpath('//div[@class="eael-accordion-list"]')

        for idx, blog in enumerate(blogs, start=1):
            name = blog.xpath("./div/h3/text()").get("").strip()

            # Make the term name XPath-safe (handles quotes properly)
            safe_name = xpath_literal(name)

            # Locate table rows under this term
            datas = blog.xpath(f'.//h3[contains(text(), {safe_name})]/parent::div/parent::div//div[@class="eael-accordion-content clearfix"]//table/tbody/tr')
            for data in datas:
                description= data.xpath('./td[@class="column-2"]/text()').getall()
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name":name,
                    "Term Date": data.xpath('./td[@class="column-1"]/text()').get("").strip(),
                    "Term Date Description": " ".join(description),
                })

        calendar_df = pd.DataFrame(rows)  # load to dataframe
        save_df(calendar_df, self.institution_id, "calendar")  

   
    