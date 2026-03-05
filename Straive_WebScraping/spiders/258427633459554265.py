import io
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests

class BerrySpider(scrapy.Spider):

    name = "berry"
    institution_id = 258427633459554265

    course_rows = []
    course_url = "https://catalog.berry.edu/content.php?catoid=28&navoid=1032"
    course_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    directory_url = "https://www.berry.edu/academics/fs/"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    calendar_url = "https://berry.edu/academics/academic-calendar"

    def start_requests(self):

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        # Single functions
        if mode == "course":
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # All three (default)
        else:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
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
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        rows=[]
        page_count = int(response.xpath('(//td[contains(text(),"Page:")]/a)[last()]/text()').get('').strip())
        for page in range(1,page_count+1):
            course_url = f"https://catalog.berry.edu/content.php?catoid=28&catoid=28&navoid=1032&filter%5Bitem_type%5D=3&filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D={page}#acalog_template_course_filter"
            response= yield scrapy.Request(url=course_url,headers=self.course_headers)

            urls = response.xpath('//table[@class="table_default"]/tr/td/a/@href').getall()
            for url in urls:
                if 'preview_course' in url:
                    url = response.urljoin(url)
                    headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                    'Connection': 'keep-alive',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    'Cookie': 'acalog_theme=1; PHPSESSID=2a00c30c31d7e7fdcdceb79532d729c2; _vwo_uuid_v2=D3693637664592C00DF5A69431001C751|3fee1eec8e180417dac8a69fe64f22d4; TUT-WAF=LEUcQEeyJGCF7y4U4OpWNnicPUQ0011; aws-waf-token=16f98f09-f289-4270-9ee3-adeb8719285e:EQoAqglDMZk7AAAA:Lo35AZEkHGx0zzXwuf4HHy8VQCLSxLm1t/Ljz9WQdimP67gbkzj8bc+NRn8iC08RDEXVl8NSdvWkS5xWvQ+GyhzocKWkibC5C+Y7DxMKi5rYO175DeLq4uVtKVguS/zwT7VrADhXXLHsuJNOMrOxncHK6ePNK4XDmCIuSNyeFiAMjO91RsS19Ulya9j+zpqfbvgslg==; AWSALB=3Dmr+oDPBHmsF+h7wt1QlAfETM5Cwf+VCvjo24Unxnyvr6C6Ni0Ii2wTFPx9ZJQAl28VzGr9S8PmGIqHeGYsXENHPJiP9yvTF5bT7+3lj2XGgMFhyTC+X1Bn2eE/; AWSALBCORS=3Dmr+oDPBHmsF+h7wt1QlAfETM5Cwf+VCvjo24Unxnyvr6C6Ni0Ii2wTFPx9ZJQAl28VzGr9S8PmGIqHeGYsXENHPJiP9yvTF5bT7+3lj2XGgMFhyTC+X1Bn2eE/; TUT-WAF=hyMofX9In703BOXqGMGDQBOyOLg0011; ADRUM_BT=R%3A0%7Cg%3A111a7053-312c-41b1-8498-052471babac4171%7Cn%3Adigarc_881d5e4b-64f1-425e-8ceb-5e44d2b69b37%7Ci%3A4820341%7Cd%3A188%7Ce%3A205; AWSALB=bBBXeClYl146k/HmX1UsB/nFid9N2LljTkGAawcqnWsLj2Sgmgnhg8+qwb6LjskWn4LjdI+rl+xpf466lmhVet2iqmqBAJeBqRwF4+GFDr5g39m64Fcp5TX20w53; AWSALBCORS=bBBXeClYl146k/HmX1UsB/nFid9N2LljTkGAawcqnWsLj2Sgmgnhg8+qwb6LjskWn4LjdI+rl+xpf466lmhVet2iqmqBAJeBqRwF4+GFDr5g39m64Fcp5TX20w53; PHPSESSID=2a00c30c31d7e7fdcdceb79532d729c2; acalog_theme=1'
                    }
                    response = yield scrapy.Request(url=url,headers=headers,dont_filter=True)

                    title = response.xpath('//h1[@id="course_preview_title"]/text()').get('').strip()
                    title_clean = re.sub(r'\s+',' ',title)
                    class_num=""
                    if title_clean:
                        class_num = title_clean.split("-")[0].strip()

                    description_first = response.xpath("//h1[@id='course_preview_title'] /following-sibling::text()[2]").get('').strip()
                    if description_first =="":
                        description_first = response.xpath("//h1[@id='course_preview_title'] /following-sibling::text()[1]").get('').strip()
                    if "Cross-listed with" in description_first or "(See also" in description_first:
                        description_first = response.xpath("//h1[@id='course_preview_title'] /following-sibling::text()[3]").get('').strip()
                    if description_first =="CR:":
                        description_first=""
                    if description_first == "(See" or description_first == "(see":
                        description_first=""

                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Course Name": re.sub(r'\s+',' ',title),
                        "Course Description": re.sub(r'\s+',' ',description_first),
                        "Class Number": re.sub(r'\s+',' ',class_num),
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

        listing_links = response.xpath('//a[@class="bioButton"]/@href').getall()
        rows= []
        for link in listing_links:
            url = link.split(".")[0]
            url = response.urljoin(url)
            response = yield scrapy.Request(url,dont_filter=True)
            title = response.xpath('//h2/text()').getall()
            title_clean =", ".join([t.strip() for t in title if t.strip()]).strip()
            dept = response.xpath('//div[@class="department"]/strong/text() | //div[@class="department"]/strong/following-sibling::text()').getall()
            dept = [d.strip() for d in dept if d.strip()]
            dept_clean = " ".join(dept).strip()
            title_parts = [p.strip().lower() for p in title_clean.split(",")]
            if dept_clean in title_parts:
                course_title = title_clean
            else:
                course_title = f"{title_clean}, {dept_clean}".strip(", ")
            
            rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": response.xpath('//h1/text()').get('').strip(),
                    "Title": course_title,
                    "Email":response.xpath('//div[@class="email"]/a/text() ').get("").strip(),
                    "Phone Number": response.xpath('//div[@class="phone"]/a/text() ').get("").strip(),
                })
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")


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

        links = response.xpath('//a[@class="btnOrange"]/@href').getall()
        rows = []

        for link in links:
            url = f"https://berry.edu/academics/{link}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36",
                "Accept": "application/pdf,*/*;q=0.9",
                "Referer": "https://berry.edu/academics",
            }

            response = yield scrapy.Request(url, headers=headers, dont_filter=True)

            with pdfplumber.open(io.BytesIO(response.body)) as pdf:
                # Regex for semester: Fall, Spring, Summer, Winter
                term_re = re.compile(r'(Fall|Spring|Summer|Winter)\s+Semester\s+\d{4}', re.I)

                date_re = re.compile(r'^([A-Z][a-z]+\s+\d{1,2}(?:-\d{1,2})?(?:,\s*\w+(?:-\w+)?)?)\s+(.*)$')                 
                range_date_re = re.compile(r'^([A-Z][a-z]+\s+\d{1,2}\s*[-—]\s*[A-Z][a-z]+\s+\d{1,2})\s+(.*)$')

                current_term = None

                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue

                    # Split lines and clean
                    lines = [l.strip() for l in text.split("\n") if l.strip()]

                    # Remove page headers/footers like "Pa ge 1 | 2"
                    lines = [l for l in lines if not re.match(r'^[Pp]\s*a\s*ge\b', l)]

                    # Merge multi-line descriptions safely
                    merged_lines = []
                    buffer = ""
                    buffer_term = current_term

                    for line in lines:
                        
                        # Detect semester line anywhere
                        term_match = term_re.search(line)
                        if term_match:
                            current_term = term_match.group(0)
                            buffer_term = current_term
                            continue

                        if not current_term:
                            continue

                        # If line starts with a date → new entry
                        is_date_start = bool(re.match(r'^[A-Z][a-z]+\s+\d{1,2}', line))
                        is_named_event = bool(re.match(r'^[A-Z][A-Za-z ]+\b(begins|ends|opens|closes)\b', line, re.I))

                        if is_date_start or is_named_event:
                            if buffer:
                                merged_lines.append((buffer_term, buffer.strip()))
                            buffer = line
                            buffer_term = current_term
                        else:
                            buffer += " " + line


                    # Add last buffer
                    if buffer:
                        merged_lines.append((buffer_term, buffer.strip()))

                    # Extract date and description
                    for term, line in merged_lines:
                        line = line.replace("—", "-").strip()

                        # Find all matches for range or single-date events in the line
                        matches = list(range_date_re.finditer(line)) + list(date_re.finditer(line))
                        matches.sort(key=lambda m: m.start())  # ensure left-to-right order

                        if not matches:
                            # fallback: whole line as description
                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Term Name": term,
                                "Term Date": "",
                                "Term Date Description": line,
                            })
                            continue

                        last_end = 0
                        for m in matches:
                            # description is everything after the date until next match or end of line
                            start, end = m.span()
                            desc = line[start:end].strip()
                            
                            term_date = m.group(1).strip()
                            term_desc = m.group(2).strip() if len(m.groups()) > 1 else line[end:].strip()

                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Term Name": term,
                                "Term Date": term_date,
                                "Term Date Description": term_desc,
                            })

        # Save to CSV if any rows found
        if rows:
            calendar_df = pd.DataFrame(rows)
            save_df(calendar_df, self.institution_id, "calendar")



    

