import io
import docx
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from playwright.sync_api import sync_playwright


class PiedmontccSpider(scrapy.Spider):

    name = "piedmontcc"
    institution_id = 258435633272350684
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_sourse_url = 'https://piedmontcc.edu/curriculum-courses-schedules/'

    # DIRECTORY CONFIG
    directory_source_url = "https://piedmontcc.edu/directory"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://piedmontcc.edu/student-life/academic-calendar/"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course data is extracted using Playwright, as the course listing
        and detail pages are dynamically rendered and cannot be reliably
        accessed using standard Scrapy requests.

        - Directory data is available as static HTML pages and is scraped
        using normal Scrapy requests in the `parse_directory` callback.

        - Calendar data is provided as PDF files.
        These PDFs are downloaded using HTTP requests and parsed using
        the pdfplumber library to extract academic calendar information.
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

    # PARSE COURSE
    def parse_course(self):
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

        urls = ['https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQBAU9YIgIkrSb19V8OVuMHpAdZrcENbuU1RmfZi8nVzpnw?e=m0Fgkh',
            'https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQCeIbFQxHL0R4sZXZLred9HAT_qH51vjEJVWn3IKC-2sdM?e=Ahs2to',
            'https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQDA4V0htm5pRI8wmIMnPqoTAfRnLnLahlc9iG-DIS8bgcw?e=1elPTd',
            'https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQD7u8-z4qV4R5n8s4xxDRzOAVMSpdQyVGGmy4M4nCv-IDU?e=0ap5pG',
            'https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQD6NPx2uIAiSq8DHsROyjRbAZu7-fchgIhfI9VWLTaW250?e=DOa8Oq',
            'https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQDqFAsuG3q5SYuxsf-a6g38AVfwfNvJGeYY4jjfoUIOomk?e=REbfpL',
            'https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQDLVVqKmi7FToEiudgCcYqJATE8wxA2EB8gL0LA5gkpvac?e=ajBe47',
            'https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQDc7AN4t6OkTIGfud-qLu-hAS5HNCry9ZdIkHIQwG1Huqk?e=PG55M2',
            'https://piedmontcc-my.sharepoint.com/:b:/g/personal/informerreports_piedmontcc_edu/IQDFnPcEpP4WQLTbpsRF8oZVAafuf7pzsXIvo2HfNKaG-xw?e=2GCird']
        for url in urls:
            pdf_bytes=None

            def clean(t):
                return re.sub(r"\s+"," ",str(t).replace("\n"," ")).strip() if t else ""

            def fix(n):
                if not n or n.lower()=="staff": return "Staff"
                n=re.sub(r'(?<=[a-zA-Z])\s(?=[a-zA-Z]\b)','',n)
                u=[]
                for p in n.split():
                    if p.lower() not in [x.lower() for x in u]: u.append(p)
                return ", ".join(u)

            def norm(n):
                m={'Thomps, on':'Thompson','McLaug, hlin':'McLaughlin','Nwangu, ma':'Nwanguma',
                'Sher, man':'Sherman','Lefev, ers':'Lefevers','McLa, ughlin':'McLaughlin',
                'Berna, rd':'Bernard','Lockl, ear':'Locklear','Haski, ns':'Haskins',
                'Montgo, mery':'Montgomery','Crock, ett':'Crockett','Botto, ms':'Bottoms',
                'Watki, ns':'Watkins','Dicke, rson':'Dickerson','Hind, man':'Hindman',
                'Hatch, ett':'Hatchett','Buch, anan':'Buchanan','Thom, pson':'Thompson',
                'Walk, er':'Walker','Morg, an':'Morgan','McGi, nnis':'McGinnis','Newt, on':'Newton'}
                return m.get(n,n)

            with sync_playwright() as p:

                browser=p.chromium.launch(headless=True)
                context=browser.new_context()
                page=context.new_page()

                # Listen for PDF response
                def handle_response(res):
                    nonlocal pdf_bytes
                    ct=res.headers.get("content-type","")
                    if "pdf" in ct:
                        pdf_bytes=res.body()


                page.on("response",handle_response)
                page.goto(url)
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(10000)
                browser.close()

            if not pdf_bytes:
                return

            pdf=io.BytesIO(pdf_bytes)


            with pdfplumber.open(pdf) as p:
                for pg in p.pages:
                    tb=pg.extract_table()
                    if not tb: continue
                    for rw in tb[1:]:
                        if not rw[0] or "Id" in str(rw[0]): continue
                        sec=clean(rw[2]).replace(" ","")
                        sp=sec.split("-")
                        name_sec=f"{sp[0]}-{sp[1]}" if len(sp)>=2 else sec
                        name=clean(rw[3])
                        cls=clean(rw[0]).replace(" ","")
                        cap=clean(rw[14])
                        enr=clean(rw[15])
                        inst=norm(fix(clean(rw[13])))


                        self.course_rows.append({
                            "Cengage Master Institution ID":self.institution_id,
                            "Source URL":self.course_sourse_url,
                            "Course Name":f"{name_sec} {name}",
                            "Course Description":"",
                            "Class Number":cls,
                            "Section":sec,
                            "Instructor":inst,
                            "Enrollment":f"{enr} of {cap}",
                            "Course Dates":f"{clean(rw[6]).replace(' ','')} - {clean(rw[7]).replace(' ','')}",
                            "Location":clean(rw[1]).replace(" ",""),
                            "Textbook/Course Materials":""
                        })

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
        pages = int(response.xpath('(//a[@class="page-numbers"])[last()]/text()').get('').strip())
        for page in range(1,pages+1):
            url = f'https://piedmontcc.edu/directory/page/{page}/'
            yield scrapy.Request(url,headers=self.directory_headers,callback=self.parse_directory_final)
    
    def parse_directory_final(self,response):
        blocks = response.xpath('//div[contains(@id,"post")]')
        for block in blocks:
            
            title = ''.join(block.xpath('.//h6/parent::div/text()').getall()).strip()
            name = block.xpath('.//h6/text()').get('').strip()
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": re.sub(r'\s+',' ',name),
            "Title": re.sub(r'\s+',' ',title),
            "Email": block.xpath('.//a[contains(@href,"mailto:")]/text()').get('').strip(),
            "Phone Number": block.xpath('.//a[@class="contact-link"]/text()').get('').strip(),
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
        urls = ['https://piedmontcc.edu/wp-content/uploads/2025/08/26-27-Academic-Calendar.pdf','https://piedmontcc.edu/wp-content/uploads/2024/01/2025-2026-ACADEMIC-CALENDAR-3.28.23.docx']
        for url in urls:
            if '.docx' in url:
                try:
                    response = requests.get(url)
                    response.raise_for_status()

                    file_stream = BytesIO(response.content)
                    doc = docx.Document(file_stream)

                    full_text = [para.text for para in doc.paragraphs]

                except Exception as e:
                    return

                # Step 2: Extract Data
                current_term = ""

                for line in full_text:

                    line = line.strip()

                    # Skip unwanted lines
                    if not line or "Approved by" in line or "is designated" in line or "is the 8th" in line:
                        continue

                    # Detect Semester
                    if "SEMESTER" in line.upper():
                        current_term = line
                        continue

                    # Date + Description pattern
                    match = re.match(
                        r'^([A-Z][a-z]+\s+\d+(?:[\s\-–]+(?:[A-Z][a-z]+\s+)?\d+)?)\s+(.*)',
                        line
                    )

                    if match:

                        date_str = match.group(1).strip()
                        desc_str = match.group(2).strip()

                        if current_term:

                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": self.calendar_source_url,
                                "Term Name": current_term,
                                "Term Date": date_str,
                                "Term Date Description": desc_str
                            })

                    else:
                        # Merge continuation lines
                        if self.calendar_rows and current_term:
                            if "is designated" not in line and "is the 8th" not in line:
                                self.calendar_rows[-1]["Term Date Description"] += " " + line

                if not self.calendar_rows:
                    return

                # Cleanup spaces
                for row in self.calendar_rows:
                    row["Term Date Description"] = re.sub(
                        r'\s+', ' ', row["Term Date Description"]
                    ).strip()


            else:
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                except Exception as e:
                    return

                # Read PDF
                with pdfplumber.open(io.BytesIO(response.content)) as pdf:

                    current_term = ""

                    for page in pdf.pages:

                        text = page.extract_text()
                        if not text:
                            continue

                        lines = text.split('\n')

                        for line in lines:

                            line = line.strip()

                            # 1. Skip unwanted lines
                            if not line or "Approved by" in line or "STUDENT ACADEMIC" in line.upper():
                                continue

                            # 2. Identify Semester
                            if "SEMESTER" in line.upper():
                                current_term = line
                                continue

                            # 3. Date + Description Regex
                            match = re.match(
                                r'^([A-Z][a-z]+\s+\d+(?:[\s\-–]+(?:[A-Z][a-z]+\s+)?\d+)?)\s+(.*)',
                                line
                            )

                            if match:

                                date_str = match.group(1).strip()
                                desc_str = match.group(2).strip()

                                # Extra info skip
                                if "is the" in desc_str.lower() or desc_str.startswith("•"):
                                    continue

                                if desc_str == 'classes end wednesday for 16-week classes':
                                    desc_str = 'Classes End'

                                self.calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": self.calendar_source_url,
                                    "Term Name": current_term,
                                    "Term Date": date_str,
                                    "Term Date Description": desc_str
                                })

                            else:
                              # Merge continuation lines
                                if self.calendar_rows and not line.startswith("•") and "is the" not in line.lower():

                                    if line != self.calendar_rows[-1]["Term Date"]:

                                        self.calendar_rows[-1]["Term Date Description"] += " " + line

                if not self.calendar_rows:
                    return

                # Cleanup spaces
                for row in self.calendar_rows:
                    row["Term Date Description"] = re.sub(
                        r'\s+', ' ', row["Term Date Description"]
                    ).strip()


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
        