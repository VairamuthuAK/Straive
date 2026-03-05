import re
import io
import time
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from ..utils import save_df
from parsel import Selector

class PeruSpider(scrapy.Spider):
    name = "peru"
    institution_id = 258434774987728848
    local = True
    calendar_rows = []


    # CONFIG
    SOLVER_API_KEY = "ENTER YOUR API KEY HERE"
    SITE_KEY = "6LdBKEIUAAAAANbVIuXHlYICHGZUYVM_N4V6DUmk"
    PAGE_URL = "https://mypsc.nebraska.edu/psc/mypsc/NBW/HRMS/c/COMMUNITY_ACCESS.CLASS_SEARCH.GBL"
    INSTITUTION = "PSCNE"

    # Mapping of academic terms to their internal system codes and subject filters
    TERMS = {
        "Spring 2026": {"strm": "1261", "subjects": ['ACCT','ANTH','ART','BIOL','BUS','CHEM','CA','COUN','CJ','ECON','EDUC','EDAD','EDCI','ENG','FCS','FIN','FYI','GEOG','GEOS','HPER','HIST','HUM','LS','MGMT','MKTG','MATH','MSL','MUS','NTR','PHIL','PHYS','PS','PDCE','PSYC','READ','SS','SW','SOC','SPED','TH']},
        "Fall 2025":   {"strm": "1258", "subjects": ['ACCT','ART','BIOL','BUS','CHEM','COLL','CMIS','COUN','CJUS','ESCI','ECON','EDUC','ENG','HPER','HIST','HP','INS','JOUR','MGMT','MATH','MUSC','PHIL','PSCI','PSYC','SOC','SPED','SPCH','STAT','THEA']},
        "Summer 2025": {"strm": "1255", "subjects": ['ACCT','ART','BIOL','BUS','COLL','CMIS','CJUS','ESCI','ECON','EDUC','ENG','HPER','HIST','INS','MGMT','MATH','MUSC','PHIL','PSCI','PSYC','SOC','SPED','STAT']},
        "Spring 2025": {"strm": "1251", "subjects": ['ACCT','ANTH','ART','BIOL','BUS','CHEM','COLL','CMIS','COUN','CJUS','ESCI','ECON','EDUC','ENG','HPER','HIST','HP','INS','JOUR','MGMT','MATH','MUSC','PHYS','PSCI','PSYC','SOC','SPED','SPCH','STAT','THEA']},
    }


    # Resource URLs
    course_url = "https://www.jwcc.edu/wp-content/uploads/2025/11/2025-26-Course-Catalog.pdf"
    directory_url = 'https://www.peru.edu/staff-directory/?showall=1&page=1'
    calendar_url = "https://www.peru.edu/media/site-sections/academics/academic-resources/documents/Peru-State-College-2025-2026-Academic-Calendar.pdf"

    
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
           self.parse_directory()
        elif mode == 'calendar':
            self.parse_calendar()
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_calendar()

        elif mode in ['directory_calendar', 'calendar_directory']:
            self.parse_calendar()
            self.parse_directory()
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
            self.parse_calendar()

    def solve_captcha(self):
        """
        Communicates with the 2Captcha API to solve the site's ReCaptcha.
        
        Returns:
            str: The captcha solution token if successful, else None.
        """

        post_url = (
            f"http://2captcha.com/in.php?key={self.SOLVER_API_KEY}"
            f"&method=userrecaptcha&googlekey={self.SITE_KEY}"
            f"&pageurl={self.PAGE_URL}"
        )
        resp = requests.get(post_url).text
        if "OK|" not in resp:
            return None

        req_id = resp.split("|")[1]
        fetch_url = f"http://2captcha.com/res.php?key={self.SOLVER_API_KEY}&action=get&id={req_id}"

        # Polling loop: Wait for the service to solve the captcha
        while True:
            time.sleep(5)
            res = requests.get(fetch_url).text
            if res == "CAPCHA_NOT_READY":
                continue
            if "OK|" in res:
                return res.split("|")[1]
            return None


    def parse_course(self):
        """
        Parses course information from a PeopleSoft-based portal.
        Handles state management (ICSID/ICStateNum) and form submissions.
        """
        all_rows = []

        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.PAGE_URL
        })

        for term_name, term in self.TERMS.items():
            strm = term["strm"]

            for subject in term["subjects"]:
                self.logger.info(f"[COURSE] {term_name} | {subject}")

                # Iterating through catalog numbers to find specific sections
                for catalog in range(1, 10):
                    init = session.get(f"{self.PAGE_URL}?INSTITUTION={self.INSTITUTION}")
                    sel = Selector(text=init.text)

                    # Extract ASP.NET/PeopleSoft session state tokens
                    icsid = sel.xpath('//input[@id="ICSID"]/@value').get()
                    state = sel.xpath('//input[@id="ICStateNum"]/@value').get()

                    captcha = self.solve_captcha()
                    if not captcha:
                        continue
                    
                    # Construct the POST payload for the search form
                    payload = {
                        "ICAJAX": "1",
                        "ICType": "Panel",
                        "ICStateNum": state,
                        "ICAction": "CLASS_SRCH_WRK2_SSR_PB_CLASS_SRCH",
                        "ICSID": icsid,
                        "CLASS_SRCH_WRK2_INSTITUTION$31$": self.INSTITUTION,
                        "CLASS_SRCH_WRK2_STRM$35$": strm,
                        "SSR_CLSRCH_WRK_SUBJECT_SRCH$0": subject,
                        "SSR_CLSRCH_WRK_SSR_EXACT_MATCH1$1": "C",
                        "SSR_CLSRCH_WRK_CATALOG_NBR$1": str(catalog),
                        "g-recaptcha-response": captcha
                    }

                    res = session.post(self.PAGE_URL, data=payload)
                    sel_list = Selector(text=res.text)

                    # Extract links to specific class detail pages
                    links = sel_list.xpath('//a[contains(@id,"MTG_CLASSNAME")]/@id').getall()
                    sections = sel_list.xpath('//span[@title="View Details"]//a/text()').getall()

                    for idx, action in enumerate(links):
                        # Request details for a specific class section
                        det_payload = {
                            "ICAJAX": "1",
                            "ICType": "Panel",
                            "ICStateNum": sel_list.xpath('//input[@id="ICStateNum"]/@value').get(),
                            "ICAction": action,
                            "ICSID": sel_list.xpath('//input[@id="ICSID"]/@value').get(),
                        }

                        det = session.post(self.PAGE_URL, data=det_payload)
                        if "Class Detail" not in det.text:
                            continue

                        sel_det = Selector(text=det.text)
                        # Data Extraction
                        course_name = sel_det.xpath('//span[@class="PALEVEL0SECONDARY"]/text()').get("").strip()
                        class_number = sel_det.xpath('//span[@id="SSR_CLS_DTL_WRK_CLASS_NBR"]/text()').get("").strip()
                        cap = sel_det.xpath('//div[@id="win0divSSR_CLS_DTL_WRK_ENRL_CAP"]/span/text()').get("0")
                        avail = sel_det.xpath('//span[@id="SSR_CLS_DTL_WRK_ENRL_TOT"]/text()').get("0")

                        all_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.PAGE_URL,
                            "Course Name": course_name,
                            "Course Description": sel_det.xpath('//span[@id="DERIVED_CLSRCH_DESCRLONG"]/text()').get(""),
                            "Class Number": class_number,
                            "Section": sections[idx] if idx < len(sections) else "",
                            "Instructor": sel_det.xpath('//span[contains(@id,"MTG_INSTR")]/text()').get(""),
                            "Enrollment": f"{avail}/{cap}",
                            "Course Dates": sel_det.xpath('//span[contains(@id,"MTG_DATE")]/text()').get(""),
                            "Location": sel_det.xpath('//span[@id="CAMPUS_LOC_VW_DESCR"]/text()').get(""),
                            "Textbook/Course Materials": "",
                        })

                        # Navigate back to results list to keep the session state valid
                        back_state = sel_det.xpath('//input[@id="ICStateNum"]/@value').get()
                        session.post(self.PAGE_URL, data={
                            "ICAJAX": "1",
                            "ICType": "Panel",
                            "ICStateNum": back_state,
                            "ICAction": "CLASS_SRCH_WRK2_SSR_PB_BACK",
                            "ICSID": icsid,
                        })
                        time.sleep(1)
        # Persistence
        if all_rows:
            course_df = pd.DataFrame(all_rows)
            save_df(course_df, self.institution_id, "course")

        self.logger.info(f"[COURSE] Total rows scraped: {len(all_rows)}")
            
    def parse_directory(self):
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
        rows = []
        for page in range(1, 23):  # Iterates through known page count (1 to 22)
            page_url = f"https://www.peru.edu/staff-directory/?showall=1&page={page}"
            res = requests.request("GET", page_url)
            response = Selector(text=res.text)

            # Identify each staff card on the summary page
            product_links = response.xpath('//div[@class="staff-card"]')
            for product_link in product_links:
                links = 'https://www.peru.edu'+ product_link.xpath('./a/@href').get('')
                product_res = requests.get(links)
                product_response = Selector(text=product_res.text)

                # Detail extraction
                name = product_response.xpath('//h1/span/text()').get('').strip()
                title1 = product_response.xpath('//h2/text()').get('').strip()
                title2 = product_response.xpath('//h2//following::p[1]/text()').get('').strip()
                if title2 !=  '':
                    title = title1 + ', ' + title2
                else:
                    title = title
                email = product_response.xpath("//a[starts-with(@href, 'mailto:')]/@href").get('')
                email = email.replace("mailto:", "") if email else None
                phone = product_response.xpath("//a[starts-with(@href, 'tel:')]/@href").get('')
                phone = phone.replace("tel:", "") if phone else None
                rows.append(
                    {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": links,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                    }
                )
            
        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self):

        # List of specific academic events to look for in the PDF
        TARGET_DESCRIPTIONS = [
            "Welcome Week Begins",
            "Regular Term & 8W1 Session classes begin",
            "Last day to drop/add 8W1 Session classes",
            "Regular Session begins",
            "Last day to drop/add Regular Session classes",
            "Labor Day (college closed)",
            "Payment Deadline - Regular & 8W1 (late fees after this date)",
            'Last day to withdraw from 8W1 Session classes with "W"',
            "Freshman Advising Begins",
            "8W1 Session classes end",
            "Mid-term Break (no classes, offices open)",
            "Graduation applications for May 2026 due",
            "Spring 2026 registration for Freshmen",
            "8W2 Session classes begin",
            "Last day to drop/add 8W2 Session classes",
            "Last day to drop/add 8W2 Session classes",
            "Payment Deadline - 8W2 (late fees after this date)",
            'Last day to withdraw from Regular Session classes with a "W"',
            'Last day to withdraw from 8W2 Session classes with "W"',
            "Fall Break (college closed 11/27-28)",
            "Last academic/instructional day",
            "Finals Week",
            "Regular Session and 8W2 Session end",
            "December 2025 Graduation (formal ceremony May 2026)",
            "Winter Break (college closed)",
            "Martin Luther King, Jr. Day (no classes, offices open)",
            "Advising for Summer 2026, Fall 2026, & Spring 2027 begins",
            "Registration for Summer 2026, Fall 2026, & Spring 2027 begins",
            "Graduation applications for August/December 2026 due",
            "Registration for Fall Term 2026",
            "Spring Break (no classes, offices open)",
            "Commencement 10:00 a. m.",
            "Final Grades Due at noon",
            "Memorial Day (college closed)",
            "Payment Deadline - Regular Session (late fees after this date)",
            "8W1 Session classes begin",
            "Juneteenth (college closed)",
            "Payment Deadline - 8W1 (late fees after this date)",
            "Independence Day Observed (college closed)",
            'Last day to withdraw from Regular and 8W1 classes with a "W"',
            "Regular and 8W1 Session classes end",
            "August 2026 Graduation (formal ceremony May 2026 or 2027)"	
        ]
        extracted_data = []
        response = requests.get(self.calendar_url)
        
        # Open PDF from memory stream
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            full_text = ""
        for page in pdf.pages:
            full_text += page.extract_text() + "\n"

        lines = full_text.split('\n')
        current_term = "Unknown Term"

        for line in lines:
            # Dynamically update the current term based on headers in the PDF
            if "Fall 2025" in line:
                current_term = "Fall Term 2025"
            elif "Spring 2026" in line:
                current_term = "Spring Term 2026"
            elif "Summer 2026" in line:
                current_term = "Summer Term 2026"

            # Check if the line contains any of our target descriptions
            for desc in TARGET_DESCRIPTIONS:
                if desc.lower() in line.lower():
                    # Use regex to find a date pattern (e.g., Aug 14 or Jan 12)
                    # This looks for Month names followed by digits
                    date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+', line)
                    term_date = date_match.group(0) if date_match else "Date Not Found"
                    
                    extracted_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_url,
                        "Term Name": current_term,
                        "Term Date": term_date,
                        "Term Date Description": desc,
                    })

            calendar_df = pd.DataFrame(extracted_data)
            save_df(calendar_df, self.institution_id, "calendar")