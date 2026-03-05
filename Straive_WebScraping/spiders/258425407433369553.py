import re
import io
import json
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *
from lxml import html


class TruetSpider(scrapy.Spider):
    """
    Spider for Truett McConnell University.
    
    This spider extracts three categories of data:
    1. Courses: Extracted from Undergraduate and Graduate PDF catalogs.
    2. Directory: Extracted via AJAX requests from the university's staff directory.
    3. Calendar: Extracted from HTML tables on the academic calendar page.
    """
    name = "truet"
    institution_id = 258425407433369553

    # In-memory storage
    course_rows = []
    calendar_rows = []
    directory_rows = []
    
    # Source PDF URLs for course catalogs
    course_urls = ['https://truett.edu/wp-content/uploads/2025/11/2025-2026-Undergraduate-Catalog.pdf',
                   'https://truett.edu/wp-content/uploads/2026/01/2025-2026-Grad-Catalog-updated-1-28-2026.pdf'
                   ]
    
    # URL for the academic calendar page
    calendar_URL = "https://truett.edu/academics/academic-calendar/"
    
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            for course_url  in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            for page in range(1,16):
                directory_url = f"https://truett.edu/wp-admin/admin-ajax.php?action=get_more_staff&page={page}&first_name=&last_name=&area=&list_format=0"
                yield scrapy.Request(url=directory_url,callback=self.parse_directory,dont_filter=True)
        
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_URL,callback=self.parse_calendar,dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            for course_url  in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)

            for page in range(1,16):
                directory_url = f"https://truett.edu/wp-admin/admin-ajax.php?action=get_more_staff&page={page}&first_name=&last_name=&area=&list_format=0"
                yield scrapy.Request(url=directory_url,callback=self.parse_directory,dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            for course_url  in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)

            yield scrapy.Request(url=self.calendar_URL,callback=self.parse_calendar,dont_filter=True)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_URL,callback=self.parse_calendar,dont_filter=True)

            for page in range(1,16):
                directory_url = f"https://truett.edu/wp-admin/admin-ajax.php?action=get_more_staff&page={page}&first_name=&last_name=&area=&list_format=0"
                yield scrapy.Request(url=directory_url,callback=self.parse_directory,dont_filter=True)
        
        # All three (default)
        else:
            for course_url  in self.course_urls:
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)

            for page in range(1,16):
                directory_url = f"https://truett.edu/wp-admin/admin-ajax.php?action=get_more_staff&page={page}&first_name=&last_name=&area=&list_format=0"
                yield scrapy.Request(url=directory_url,callback=self.parse_directory,dont_filter=True)
            
            yield scrapy.Request(url=self.calendar_URL,callback=self.parse_calendar,dont_filter=True)
       

    # Parse methods UNCHANGED from your original
    def parse_course(self, response):

        # Determine PDF structure based on the URL
        if '2025-2026-Undergraduate-Catalog' in response.url:
            START_PAGE = 277
            END_PAGE = 356

            def is_bold(obj):
                """Checks if a word is bold based on font name."""
                fontname = obj.get("fontname", "").lower()
                return "bold" in fontname or "bd" in fontname

            def clean_course_name(name):
                """
                Removes credit hour markers like '3 Hours' or '1-4 Hours' 
                from the end of the course title.
                """
                if not name: return ""
                # Matches patterns like '3 Hours', '1-4 Hours', '1-3 Hours', etc.
                # The '$' ensures it only removes it from the end of the string.
                cleaned_name = re.sub(r'\s+\d+(?:-\d+)?\s+Hours$', '', name.strip())
                return cleaned_name

            def clean_description(text):
                if not text: return ""
                # Remove the footer and page numbers
                text = re.sub(r'\d{4}-\d{2}\s+Academic\s+Catalog\s+\d+', '', text)
                # Remove standalone Department headers (all caps)
                text = re.sub(r'^[A-Z\s]{4,}(?:\s\([A-Z]{2}\))?$', '', text)
                return re.sub(r'\s+', ' ', text).strip()

            pdf_bytes = response.body
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                current_course_name = ""
                current_desc_parts = []
                
                for i in range(START_PAGE - 1, END_PAGE):
                    page = pdf.pages[i]
                    words = page.extract_words(extra_attrs=["fontname"])
                    
                    # Grouping words into lines
                    lines = []
                    if words:
                        last_top = words[0]['top']
                        temp_line = []
                        for w in words:
                            if abs(w['top'] - last_top) < 3:
                                temp_line.append(w)
                            else:
                                lines.append(temp_line)
                                temp_line = [w]
                                last_top = w['top']
                        lines.append(temp_line)

                    for line_words in lines:
                        line_text = " ".join([w['text'] for w in line_words])
                        
                        # Identify Course Header: Bold + Pattern (e.g., AB 101)
                        first_word_bold = is_bold(line_words[0])
                        is_course_pattern = re.match(r'^[A-Z]{2,4}\s\d{3,4}', line_text)

                        if first_word_bold and is_course_pattern:
                            # Save the previous course
                            if current_course_name:
                                final_name = clean_course_name(current_course_name)
                                final_name = final_name.replace('½ Hour','').replace('1 Hour/0 Hours','').replace('1 Hour','').replace('0 Hour','').replace('1/2 hour','').replace('½ Hour','').strip()
                                class_number = ' '.join(final_name.split(' ')[:2])
                            
                                final_desc = clean_description(" ".join(current_desc_parts))
                                final_desc = final_desc.replace('ACCOUNTING (AC)','').replace('ART (AR)','').replace('BIOLOGY (BI)','').replace('BUSINESS (BU)','').replace('CHEMISTRY (CH)','').replace('CRIMINAL JUSTICE (CJ)','').replace('CHINESE (CN)','').replace('COMMUNICATION (CO)','').replace('CHRISTIAN STUDIES (CS)','').replace('EDUCATION (ED)','').replace('ENGLISH (EN)','').replace('EXERCISE SCIENCE (ES)','').replace('FINE ARTS (FA)','').replace('FOUNDATIONS (FD)','').replace('FRENCH (FR)','').replace('FORENSIC SCIENCE (FS)','').replace('GREEK (GK)','').replace('HEBREW (HB)','').replace('HISTORY (HI)','').replace('HUMANITIES (HU)','').replace('LANGUAGE (LA)','').replace('LATIN (LT)','').replace('MATH (MA)','').replace('MISSIONS (MI)','').replace('MUSIC (MU)','').replace('NATURAL SCIENCES (NS)','').replace('NURSING (NU)','').replace('PHYSICAL EDUCATION (PE)','').replace('PHILOSOPHY (PH)','').replace('POLITICAL SCIENCE (PO)','').replace('PHYSICAL SCIENCE (PS)','').replace('PHYSICS (PX)','').replace('PSYCHOLOGY (PY)','').replace('SOCIOLOGY (SO)','').replace('SPANISH (SP)','').replace('SOCIAL SCIENCE (SS)','').replace('THEATRE (TH)','').replace('TRUETT MCCONNELL (TM)','')
                                # courses_list.append([final_name, final_desc])
                                self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Course Name": final_name,
                                    "Course Description": final_desc,
                                    "Class Number": class_number,
                                    "Section": "",
                                    "Instructor": "",
                                    "Enrollment": "",
                                    "Course Dates": "",
                                    "Location": "",
                                    "Textbook/Course Materials": ""
                                })
                            
                            current_course_name = line_text
                            current_desc_parts = []
                        else:
                            # Collect description, including non-bold prerequisite lines
                            current_desc_parts.append(line_text)

                # Append last record
                if current_course_name:
                    final_name = clean_course_name(current_course_name)
                    final_name = final_name.replace('½ Hour','').replace('1 Hour/0 Hours','').replace('1 Hour','').replace('0 Hour','').replace('1/2 hour','').replace('½ Hour','').strip()
                    class_number = ' '.join(final_name.split(' ')[:2])
                    final_desc = clean_description(" ".join(current_desc_parts))
                    final_desc = final_desc.replace('ACCOUNTING (AC)','').replace('ART (AR)','').replace('BIOLOGY (BI)','').replace('BUSINESS (BU)','').replace('CHEMISTRY (CH)','').replace('CRIMINAL JUSTICE (CJ)','').replace('CHINESE (CN)','').replace('COMMUNICATION (CO)','').replace('CHRISTIAN STUDIES (CS)','').replace('EDUCATION (ED)','').replace('ENGLISH (EN)','').replace('EXERCISE SCIENCE (ES)','').replace('FINE ARTS (FA)','').replace('FOUNDATIONS (FD)','').replace('FRENCH (FR)','').replace('FORENSIC SCIENCE (FS)','').replace('GREEK (GK)','').replace('HEBREW (HB)','').replace('HISTORY (HI)','').replace('HUMANITIES (HU)','').replace('LANGUAGE (LA)','').replace('LATIN (LT)','').replace('MATH (MA)','').replace('MISSIONS (MI)','').replace('MUSIC (MU)','').replace('NATURAL SCIENCES (NS)','').replace('NURSING (NU)','').replace('PHYSICAL EDUCATION (PE)','').replace('PHILOSOPHY (PH)','').replace('POLITICAL SCIENCE (PO)','').replace('PHYSICAL SCIENCE (PS)','').replace('PHYSICS (PX)','').replace('PSYCHOLOGY (PY)','').replace('SOCIOLOGY (SO)','').replace('SPANISH (SP)','').replace('SOCIAL SCIENCE (SS)','').replace('THEATRE (TH)','').replace('TRUETT MCCONNELL (TM)','')
                    # courses_list.append([final_name, final_desc])
                    self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Course Name": final_name,
                        "Course Description": final_desc,
                        "Class Number": class_number,
                        "Section": "",
                        "Instructor": "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": ""
                    })

        else:
            START_PAGE = 95
            END_PAGE = 113

            def is_bold(obj):
                """Checks if a word is bold based on font name."""
                fontname = obj.get("fontname", "").lower()
                return "bold" in fontname or "bd" in fontname

            def clean_course_name(name):
                """
                Removes credit hour markers like '3 Hours' or '1-4 Hours' 
                from the end of the course title.
                """
                if not name: return ""
                # Matches patterns like '3 Hours', '1-4 Hours', '1-3 Hours', etc.
                # The '$' ensures it only removes it from the end of the string.
                cleaned_name = re.sub(r'\s+\d+(?:-\d+)?\s+Hours$', '', name.strip())
                return cleaned_name

            def clean_description(text):
                if not text: return ""
                # Remove the footer and page numbers
                text = re.sub(r'\d{4}-\d{2}\s+Academic\s+Catalog\s+\d+', '', text)
                # Remove standalone Department headers (all caps)
                text = re.sub(r'^[A-Z\s]{4,}(?:\s\([A-Z]{2}\))?$', '', text)
                return re.sub(r'\s+', ' ', text).strip()
            
            pdf_bytes = response.body
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                current_course_name = ""
                current_desc_parts = []
                
                for i in range(START_PAGE - 1, END_PAGE):
                    page = pdf.pages[i]
                    words = page.extract_words(extra_attrs=["fontname"])
                    
                    # Grouping words into lines
                    lines = []
                    if words:
                        last_top = words[0]['top']
                        temp_line = []
                        for w in words:
                            if abs(w['top'] - last_top) < 3:
                                temp_line.append(w)
                            else:
                                lines.append(temp_line)
                                temp_line = [w]
                                last_top = w['top']
                        lines.append(temp_line)

                    for line_words in lines:
                        line_text = " ".join([w['text'] for w in line_words])
                        
                        # Identify Course Header: Bold + Pattern (e.g., AB 101)
                        first_word_bold = is_bold(line_words[0])
                        is_course_pattern = re.match(r'^[A-Z]{2,4}\s\d{3,4}', line_text)

                        if first_word_bold and is_course_pattern:
                            # Save the previous course
                            if current_course_name:
                                final_name = clean_course_name(current_course_name)
                                final_name = final_name.replace('½ Hour','').replace('1 Hour/0 Hours','').replace('1 Hour','').replace('0 Hour','').replace('1/2 hour','').replace('½ Hour','').strip()
                                class_number = ' '.join(final_name.split(' ')[:2]).replace(':','')
                                final_desc = clean_description(" ".join(current_desc_parts))
                                final_desc = final_desc.replace('ACCOUNTING (AC)','').replace('ART (AR)','').replace('BIOLOGY (BI)','').replace('BUSINESS (BU)','').replace('CHEMISTRY (CH)','').replace('CRIMINAL JUSTICE (CJ)','').replace('CHINESE (CN)','').replace('COMMUNICATION (CO)','').replace('CHRISTIAN STUDIES (CS)','').replace('EDUCATION (ED)','').replace('ENGLISH (EN)','').replace('EXERCISE SCIENCE (ES)','').replace('FINE ARTS (FA)','').replace('FOUNDATIONS (FD)','').replace('FRENCH (FR)','').replace('FORENSIC SCIENCE (FS)','').replace('GREEK (GK)','').replace('HEBREW (HB)','').replace('HISTORY (HI)','').replace('HUMANITIES (HU)','').replace('LANGUAGE (LA)','').replace('LATIN (LT)','').replace('MATH (MA)','').replace('MISSIONS (MI)','').replace('MUSIC (MU)','').replace('NATURAL SCIENCES (NS)','').replace('NURSING (NU)','').replace('PHYSICAL EDUCATION (PE)','').replace('PHILOSOPHY (PH)','').replace('POLITICAL SCIENCE (PO)','').replace('PHYSICAL SCIENCE (PS)','').replace('PHYSICS (PX)','').replace('PSYCHOLOGY (PY)','').replace('SOCIOLOGY (SO)','').replace('SPANISH (SP)','').replace('SOCIAL SCIENCE (SS)','').replace('THEATRE (TH)','').replace('TRUETT MCCONNELL (TM)','').replace(' COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 88','').replace(' COURSE DESCRIPTIONS','').replace('COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 91 COUNSELING (CL)','').replace(' COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 92','').replace('COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 94','').replace('COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 96','').replace('COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 98','').replace('2025-26 Graduate Catalog | 105 SPORTS MANAGEMENT (SM)','').replace(' 2025-26 Graduate Catalog | 102 OLD TESTAMENT (OT)','').replace('2025-26 Graduate Catalog | 99','').replace('2025-26 Graduate Catalog | 98','')
                                # courses_list.append([final_name, final_desc])
                                self.calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Course Name": final_name,
                                    "Course Description": final_desc,
                                    "Class Number": class_number,
                                    "Section": "",
                                    "Instructor": "",
                                    "Enrollment": "",
                                    "Course Dates": "",
                                    "Location": "",
                                    "Textbook/Course Materials": ""
                                })
                            
                            current_course_name = line_text
                            current_desc_parts = []
                        else:
                            # Collect description, including non-bold prerequisite lines
                            current_desc_parts.append(line_text)

                # Append last record
                if current_course_name:
                    final_name = clean_course_name(current_course_name)
                    final_name = final_name.replace('½ Hour','').replace('1 Hour/0 Hours','').replace('1 Hour','').replace('0 Hour','').replace('1/2 hour','').replace('½ Hour','').strip()
                    class_number = ' '.join(final_name.split(' ')[:2]).replace(':','')
                    final_desc = clean_description(" ".join(current_desc_parts))
                    final_desc = final_desc.replace('ACCOUNTING (AC)','').replace('ART (AR)','').replace('BIOLOGY (BI)','').replace('BUSINESS (BU)','').replace('CHEMISTRY (CH)','').replace('CRIMINAL JUSTICE (CJ)','').replace('CHINESE (CN)','').replace('COMMUNICATION (CO)','').replace('CHRISTIAN STUDIES (CS)','').replace('EDUCATION (ED)','').replace('ENGLISH (EN)','').replace('EXERCISE SCIENCE (ES)','').replace('FINE ARTS (FA)','').replace('FOUNDATIONS (FD)','').replace('FRENCH (FR)','').replace('FORENSIC SCIENCE (FS)','').replace('GREEK (GK)','').replace('HEBREW (HB)','').replace('HISTORY (HI)','').replace('HUMANITIES (HU)','').replace('LANGUAGE (LA)','').replace('LATIN (LT)','').replace('MATH (MA)','').replace('MISSIONS (MI)','').replace('MUSIC (MU)','').replace('NATURAL SCIENCES (NS)','').replace('NURSING (NU)','').replace('PHYSICAL EDUCATION (PE)','').replace('PHILOSOPHY (PH)','').replace('POLITICAL SCIENCE (PO)','').replace('PHYSICAL SCIENCE (PS)','').replace('PHYSICS (PX)','').replace('PSYCHOLOGY (PY)','').replace('SOCIOLOGY (SO)','').replace('SPANISH (SP)','').replace('SOCIAL SCIENCE (SS)','').replace('THEATRE (TH)','').replace('TRUETT MCCONNELL (TM)','').replace(' COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 88','').replace(' COURSE DESCRIPTIONS','').replace('COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 91 COUNSELING (CL)','').replace(' COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 92','').replace('COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 94','').replace('COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 96','').replace('COURSE DESCRIPTIONS 2025-26 Graduate Catalog | 98','').replace('2025-26 Graduate Catalog | 105 SPORTS MANAGEMENT (SM)','').replace(' 2025-26 Graduate Catalog | 102 OLD TESTAMENT (OT)','').replace('2025-26 Graduate Catalog | 99','').replace('2025-26 Graduate Catalog | 98','')
                    # courses_list.append([final_name, final_desc])
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Course Name": final_name,
                        "Course Description": final_desc,
                        "Class Number": class_number,
                        "Section": "",
                        "Instructor": "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": ""
                    })



        # SAVE OUTPUT CSV
        course_df = pd.DataFrame(self.course_rows)
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
        
        data = json.loads(response.text)
        html_text = data["content"]
        tree = html.fromstring(html_text)

        # Each staff member is represented as a flipping card
        cards = tree.xpath("//div[contains(@class,'m-directory--card flip-container')]")
        for card in cards:
            name = card.xpath(".//div[contains(@class,'card-front-text')]//h3/text()")
            name = " ".join(name[0].split()) if name else None
            title = card.xpath(".//h4[starts-with(@id,'empTitle')]/text()")
            title = list(dict.fromkeys(title))   # remove duplicates, keep order
            title = " ".join(title)
            phone = card.xpath(".//a[starts-with(@href,'tel:')]/text()")
            phone = ''.join(phone)
            email = card.xpath(".//a[starts-with(@href,'mailto:')]/@href")
            email = ''.join(email).replace('mailto:','').strip()
            
            self.directory_rows.append(
            {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": 'https://truett.edu/directory/',
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
                }
            )
    
    def parse_calendar(self,response):

        blocks = response.xpath('//div[@class="table-responsive"]//table')
        for block in blocks:
            header = block.xpath('.//tr[1]//td//text()').getall()
            header_text = " ".join(header)

            if "Class Day" in header_text:
                continue   # skip whole table

            for tr in block.xpath('.//tr'):
                # process
                term_name = block.xpath('preceding::h3[1]/text()').get()
                term_description = tr.xpath('.//td[1]//text()').getall()
                term_description = ' '.join(term_description)
                term_dates = tr.xpath('.//td[2]//text()').get('')
                if term_dates != '':
                    self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_URL,
                    "Term Name": term_name,
                    "Term Date": term_dates,
                    "Term Date Description": term_description
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




 