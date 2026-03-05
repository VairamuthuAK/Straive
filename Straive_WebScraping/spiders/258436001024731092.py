import re
import io
import json
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from parsel import Selector


class WesternSpider(scrapy.Spider):

    name = "western"
    institution_id = 258436001024731092
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://wnc.edu/flexible-learning.php"
    course_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'cache-control': 'max-age=0',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://pensacolastate.edu',
    'priority': 'u=0, i',
    'referer': 'https://pensacolastate.edu/course-search/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # DIRECTORY CONFIG
    directory_source_url = "https://wnc.edu/directory/index.php"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }
  
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course and Directory data is extracted using Scrapy.

        - Calendar data exstracted using pdfplumber
        """

        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

    # PARSE COURSE
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
        
        sections = ["Accounting","American Sign Lang","Anthropology","Art","Astronomy","Automotive Autobody","Automotive Mechanics","Aviation","Biology","Building & Trades","Apprenticeship Course","Building & Trades Carpentry","Business","Computer-aided Design and Drafting","Construction Management","Chemistry","Computer Information Technolog","Computer Science","Communication","Core Humanities","Counseling & Personal Develop","Criminal Justice","Dance","Drafting","Early Childhood Education","Economics","Education","Educational Psychology","Electrical Theory","Engineering","English","ME","Environmental Science","Finance","Fire Science","French","Geography","Geology","Graphic Communications","History","Medical Terminology","Holocaust Genocide Peace","Human Dev and Family Studies","Information Systems","Japanese","Laboratory Technician","Machine Tool Technology","Management Science","Manufacturing Prod Tech","Marketing","Math","Music","Nursing","Nutrition","Occupational Safety","Philosophy","Physics","Political Science","Psychology","Public Health","Real Estate","Social Work","Sociology","Spanish","Statistics","Surveying","Theater Arts","Welding","Emergency Medical Services","EDCT"]
        for sec in sections:
            url = "https://wabi-south-central-us-api.analysis.windows.net/public/reports/querydata?synchronous=true"
            payload = "{\"version\":\"1.0.0\",\"queries\":[{\"Query\":{\"Commands\":[{\"SemanticQueryDataShapeCommand\":{\"Query\":{\"Version\":2,\"From\":[{\"Name\":\"s\",\"Entity\":\"Spring Schedule\",\"Type\":0}],\"Select\":[{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"s\"}},\"Property\":\"Custom HTML 2\"},\"Name\":\"Spring Schedule.Custom HTML 2\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"s\"}},\"Property\":\"Catalog Number\"},\"Name\":\"Spring Schedule.Catalog Number\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"s\"}},\"Property\":\"Class Number\"},\"Name\":\"Spring Schedule.Class Number\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"s\"}},\"Property\":\"Subject\"},\"Name\":\"Spring Schedule.Subject\"}],\"Where\":[{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"s\"}},\"Property\":\"Subject\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'"+sec+"'\"}}]]}}},{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"s\"}},\"Property\":\"Schedule Print?\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'Y'\"}}]]}}},{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"s\"}},\"Property\":\"Class Status\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'A'\"}}]]}}}]},\"Binding\":{\"Primary\":{\"Groupings\":[{\"Projections\":[0,1,2,3]}]},\"DataReduction\":{\"DataVolume\":3,\"Primary\":{\"Top\":{}}},\"Version\":1},\"ExecutionMetricsKind\":1}}]},\"CacheKey\":\"{\\\"Commands\\\":[{\\\"SemanticQueryDataShapeCommand\\\":{\\\"Query\\\":{\\\"Version\\\":2,\\\"From\\\":[{\\\"Name\\\":\\\"s\\\",\\\"Entity\\\":\\\"Spring Schedule\\\",\\\"Type\\\":0}],\\\"Select\\\":[{\\\"Measure\\\":{\\\"Expression\\\":{\\\"SourceRef\\\":{\\\"Source\\\":\\\"s\\\"}},\\\"Property\\\":\\\"Custom HTML 2\\\"},\\\"Name\\\":\\\"Spring Schedule.Custom HTML 2\\\"},{\\\"Column\\\":{\\\"Expression\\\":{\\\"SourceRef\\\":{\\\"Source\\\":\\\"s\\\"}},\\\"Property\\\":\\\"Catalog Number\\\"},\\\"Name\\\":\\\"Spring Schedule.Catalog Number\\\"},{\\\"Column\\\":{\\\"Expression\\\":{\\\"SourceRef\\\":{\\\"Source\\\":\\\"s\\\"}},\\\"Property\\\":\\\"Class Number\\\"},\\\"Name\\\":\\\"Spring Schedule.Class Number\\\"},{\\\"Column\\\":{\\\"Expression\\\":{\\\"SourceRef\\\":{\\\"Source\\\":\\\"s\\\"}},\\\"Property\\\":\\\"Subject\\\"},\\\"Name\\\":\\\"Spring Schedule.Subject\\\"}],\\\"Where\\\":[{\\\"Condition\\\":{\\\"In\\\":{\\\"Expressions\\\":[{\\\"Column\\\":{\\\"Expression\\\":{\\\"SourceRef\\\":{\\\"Source\\\":\\\"s\\\"}},\\\"Property\\\":\\\"Subject\\\"}}],\\\"Values\\\":[[{\\\"Literal\\\":{\\\"Value\\\":\\\"'"+sec+"'\\\"}}]]}}},{\\\"Condition\\\":{\\\"In\\\":{\\\"Expressions\\\":[{\\\"Column\\\":{\\\"Expression\\\":{\\\"SourceRef\\\":{\\\"Source\\\":\\\"s\\\"}},\\\"Property\\\":\\\"Schedule Print?\\\"}}],\\\"Values\\\":[[{\\\"Literal\\\":{\\\"Value\\\":\\\"'Y'\\\"}}]]}}},{\\\"Condition\\\":{\\\"In\\\":{\\\"Expressions\\\":[{\\\"Column\\\":{\\\"Expression\\\":{\\\"SourceRef\\\":{\\\"Source\\\":\\\"s\\\"}},\\\"Property\\\":\\\"Class Status\\\"}}],\\\"Values\\\":[[{\\\"Literal\\\":{\\\"Value\\\":\\\"'A'\\\"}}]]}}}]},\\\"Binding\\\":{\\\"Primary\\\":{\\\"Groupings\\\":[{\\\"Projections\\\":[0,1,2,3]}]},\\\"DataReduction\\\":{\\\"DataVolume\\\":3,\\\"Primary\\\":{\\\"Top\\\":{}}},\\\"Version\\\":1},\\\"ExecutionMetricsKind\\\":1}}]}\",\"QueryId\":\"\",\"ApplicationContext\":{\"DatasetId\":\"7642360a-0733-42ee-bc5a-cfce748dd1e5\",\"Sources\":[{\"ReportId\":\"566081b8-05b5-497b-bd75-12754eb80dc5\",\"VisualId\":\"409c8f5688dd0343a9f2\"}]}}],\"cancelQueries\":[],\"modelId\":7171648}"
            headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
            'ActivityId': 'ff841905-4abb-4435-9f98-aae50fb637ac',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Origin': 'https://app.powerbi.com',
            'Referer': 'https://app.powerbi.com/',
            'RequestId': 'f439f2cd-8c7e-f62e-06d3-87ce9cd7c18e',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'X-PowerBI-ResourceKey': 'd1c87f4a-68f4-4f3a-b928-ee7e4a3aea9f',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
            }
            yield scrapy.Request(url,method='POST',body=payload,headers=headers,callback=self.parse_course_final,cb_kwargs={'sec':sec})

    def parse_course_final(self,response,sec):
        data = json.loads(response.text)
        try:
            html_blocks = data['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']['D3']
            html_blocks = ''.join(html_blocks)
            sel = Selector(text=html_blocks)

            blocks = sel.xpath('//div[@class="card-body"]')
           
            for block in blocks:
                course_name = block.xpath('.//div[@class="col card-title"]/text()').get('').replace('\n','').strip()
                section = block.xpath('.//span[contains(text(),"Section:")]/parent::div/text()').get('').replace('\n','').strip()
                instructor = block.xpath('.//span[contains(text(),"Instructor:")]/parent::div/text()').get('').replace('\n','').strip()
                location = block.xpath('.//span[contains(text(),"Location:")]/parent::div/text()').get('').replace('\n','').strip()
                enr = block.xpath('.//span[contains(text(),"Enrolled:")]/parent::div/text()').get('').replace('\n','').strip()
                cap = block.xpath('.//span[contains(text(),"Capacity:")]/parent::div/text()').get('').replace('\n','').strip()
                enrollment = f'{enr} of {cap}'
                start_date = block.xpath('.//span[contains(text(),"Start Date:")]/parent::div/text()').get('').replace('\n','').strip()
                end_date = block.xpath('.//span[contains(text(),"End Date:")]/parent::div/text()').get('').replace('\n','').strip()
                course_dates = f'{start_date} - {end_date}'
                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_url,
                    "Course Name": re.sub(r'\s+',' ', course_name),
                    "Course Description": '',
                    "Class Number": '',
                    "Section": section,
                    "Instructor": re.sub(r'\s+',' ', instructor),
                    "Enrollment": re.sub(r'\s+',' ', enrollment),
                    "Course Dates": re.sub(r'\s+',' ', course_dates),
                    "Location": re.sub(r'\s+',' ', location),
                    "Textbook/Course Materials": ""
                })
        except:
            pass
       
    # PARSE DIRECTORY
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

        urls = response.xpath('//td[@headers="fname"]/strong/a/@href').getall()
        for url in urls:
            url = response.urljoin(url)
            yield scrapy.Request(url,headers=self.directory_headers,callback=self.parse_directory_final)

    def parse_directory_final(self,response): 
        title = response.xpath('//h4/text()').get('').strip()
        name = response.xpath('//div[@class="faculty-box"]/h2/text()').get('').strip()
        email = response.xpath('//div[contains(text(),"Email")]/parent::div/div[2]/a/text()').get('').strip()
        if 'wnc.edu' not in email and email !='':
            email = f'{email}@wnc.edu'
        else:
            email = email
        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": response.url,
        "Name": re.sub(r'\s+',' ',name),
        "Title": re.sub(r'\s+',' ',title),
        "Email": email,
        "Phone Number": response.xpath('//div[contains(text(),"Phone")]/parent::div/div[2]/text()').get('').strip(),
        })

    # PARSE CALENDAR
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
        urls = ['https://wnc.edu/calendar/Fall%202025%20Dates%20and%20Deadlines.pdf',
                'https://wnc.edu/admissions/admissions_photos_documents/Spring_2026_Dates_and_Deadlines.pdf']
        for url in urls:
            if '2026':
                term_name = "Spring 2026"
    
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(url, headers=headers)
            
                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": url,
                    "Term Name": term_name,
                    "Term Date": "October 15",
                    "Term Date Description": 'Spring 2026 class schedule may be viewed in myWNC and on the WNC website.'
                })
                            
                
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                    current_month = ""
                    
                    for page in pdf.pages:
                        # Table extraction settings-ai konjam loose pannuvom
                        table = page.extract_table({
                            "vertical_strategy": "lines", 
                            "horizontal_strategy": "lines",
                            "snap_tolerance": 3,
                        })
                        
                        # Table kidaikkala na text-aavathu check pannuvom
                        if not table:
                            continue
                            
                        for row in table:
                            # Row clean up
                            row = [str(cell).strip() if cell else "" for cell in row]
                            if not any(row): continue
                            
                            # 1. Month Header Detection (Improved)
                            # Row-il month name mattum irundha (Ex: "OCTOBER 2025")
                            row_text = " ".join(row).upper()
                            months = ["OCTOBER", "NOVEMBER", "DECEMBER", "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY"]
                            
                            # Check if row is just a month header
                            is_month_header = False
                            for m in months:
                                if m in row_text and re.search(r'\d{4}', row_text):
                                    current_month = m.title()
                                    is_month_header = True
                                    break
                            
                            if is_month_header: continue

                            # 2. Extract Day and Description
                            day_val = row[0].strip()
                            # Row 1-il description irukkum, sila neram row 2-ilum irukkalaam
                            desc_val = " ".join(row[1:]).strip()
                            
                            # Date digit-aaga irundhaal (New Record)
                            if day_val.isdigit():
                                # Handle multiple bullets in same cell
                                points = re.split(r'[●•]', desc_val)
                                for p in points:
                                    p_clean = p.strip()
                                    if not p_clean: continue
                                    
                                    self.calendar_rows.append({
                                        "Cengage Master Institution ID": self.institution_id,
                                        "Source URL": url,
                                        "Term Name": term_name,
                                        "Term Date": f"{current_month} {day_val}".strip(),
                                        "Term Date Description": p_clean
                                    })
                            
                            # Date illamal text mattum irundhal (Continuation)
                            elif not day_val and desc_val:
                                if self.calendar_rows:
                                    # Kadaisi record-udan serthu vidu
                                    self.calendar_rows[-1]["Term Date Description"] += " " + desc_val

                # Final Cleaning
                for item in self.calendar_rows:
                    item["Term Date Description"] = re.sub(r'\s+', ' ', item["Term Date Description"]).strip()

            else:
                response = requests.get(url)
                
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                    current_month_year = ""
                    
                    for page in pdf.pages:
                        table = page.extract_table()
                        if not table:
                            continue
                            
                        for row in table:
                            if not row or not any(row):
                                continue
                            
                            # Check for month header
                            row_str = " ".join([str(cell) for cell in row if cell]).strip()
                            month_match = re.search(r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+\d{4}', row_str, re.IGNORECASE)
                            
                            if month_match:
                                current_month_year = month_match.group(0).strip().split()[0] # Just the month name
                                continue
                            
                            # Check for date rows
                            day_val = str(row[0]).strip() if row[0] else ""
                            desc_val = str(row[1]).strip() if len(row) > 1 and row[1] else ""
                            
                            if day_val.isdigit() and desc_val:
                                # Logic to handle multi-line descriptions and multiple bullets
                                lines = desc_val.split('\n')
                                merged_points = []
                                current_point = ""
                                
                                for line in lines:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    
                                    # If it starts with a bullet, it's a new record
                                    if line.startswith('●') or line.startswith('•'):
                                        if current_point:
                                            merged_points.append(current_point)
                                        current_point = line
                                    else:
                                        # If no bullet, it's a continuation of the previous point
                                        if current_point:
                                            current_point += " " + line
                                        else:
                                            current_point = line # Should not happen often but safe to have
                                
                                if current_point:
                                    merged_points.append(current_point)
                                    
                                for point in merged_points:
                                    clean_desc = re.sub(r'\s+', ' ', point).strip()
                                    clean_desc = clean_desc.replace('●','').replace('•','').strip()
                                    current_month_year = current_month_year.title()
                                
                                    term_name = "Fall 2025"
                                    self.calendar_rows.append({
                                        "Cengage Master Institution ID": self.institution_id,
                                        "Source URL": url,
                                        "Term Name": term_name,
                                        "Term Date": f"{current_month_year} {day_val}".strip(),
                                        "Term Date Description": clean_desc
                                    })

    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")
        