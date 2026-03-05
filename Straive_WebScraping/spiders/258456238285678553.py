import re
import io
import scrapy
import pdfplumber
import urllib.parse
import pandas as pd
from ..utils import save_df
from parsel import Selector
from playwright.sync_api import sync_playwright

class LmunetSpider(scrapy.Spider):
    """
    Spider to scrape Course, Directory, and Academic Calendar data from LMU.
    Uses Playwright for dynamic section details and pdfplumber for calendar PDFs.
    """

    name = "lmunet"
    institution_id = 258456238285678553
    course_rows = []

    course_url = "https://coursecatalog.lmunet.edu/lists/courses/coursesbysubject.aspx"
    course_payload = "_wpcmWpid=&wpcmVal=&MSOWebPartPage_PostbackSource=&MSOTlPn_SelectedWpId=&MSOTlPn_View=0&MSOTlPn_ShowSettings=False&MSOGallery_SelectedLibrary=&MSOGallery_FilterString=&MSOTlPn_Button=none&__EVENTTARGET=&__EVENTARGUMENT=&__REQUESTDIGEST=0xBE1867094D82635B7508FE302D06B555D31EA3BED7F4D2EA8F9A5EFC9DB8CFB270097B16B7F4324D1D307CA7FC7C3C4DD8AE5B39DFF83FF2CD648A3B305DA322%2C31%20Jan%202026%2007%3A38%3A34%20-0000&MSOSPWebPartManager_DisplayModeName=Browse&MSOSPWebPartManager_ExitingDesignMode=false&MSOWebPartPage_Shared=&MSOLayout_LayoutChanges=&MSOLayout_InDesignMode=&_wpSelected=MSOZoneCell_WebPartWPQ2&_wzSelected=&MSOSPWebPartManager_OldDisplayModeName=Browse&MSOSPWebPartManager_StartWebPartEditingName=false&MSOSPWebPartManager_EndWebPartEditing=false&_maintainWorkspaceScrollPosition=0&__VIEWSTATE=%2FwEPDwUBMA9kFgJmD2QWAgIBD2QWBAIBD2QWAgIpD2QWAgIBD2QWAmYPPCsABgBkAgkPZBYEAgEPZBYCAgEPZBYCBSZnXzVlMjExYjJlX2EzNTZfNGQ3OF9iMWExX2NlODFiYjAzNTQxYxAPFggeCkNocm9tZVR5cGUCAh4KRm9sZGVyQ1RJRAUIMHgwMTIwMDEeDGZpbHRlcnN0cmluZ2UeD29sZGZpbHRlcnN0cmluZ2VkZGQCCQ9kFgwCAQ9kFgJmD2QWAmYPFgQeBGhyZWYFAS8eBXRpdGxlBRJMTVUgQ291cnNlIENhdGFsb2cWAgIBDxYEHgNzcmMFHS9TaXRlQXNzZXRzL015IExNVSBQb3J0YWwucG5nHgNhbHQFE0VsbHVjaWFuIFVuaXZlcnNpdHlkAgUPZBYCAgMPZBYCAgMPFgIeB1Zpc2libGVoFgJmD2QWBAICD2QWBgIBDxYCHwhoZAIDDxYCHwhoZAIFDxYCHwhoZAIDDw8WAh4JQWNjZXNzS2V5BQEvZGQCBw9kFgICAw9kFgICAQ9kFgQCAw9kFgICAQ8PFgQeBF8hU0ICAh4IQ3NzQ2xhc3MFF21zLXByb21vdGVkQWN0aW9uQnV0dG9uZGQCBQ8PFgYfCGgfCgICHwsFF21zLXByb21vdGVkQWN0aW9uQnV0dG9uZGQCEQ88KwAFAQAPFgIeD1NpdGVNYXBQcm92aWRlcgUaQ29tYmluZWROYXZTaXRlTWFwUHJvdmlkZXJkZAITD2QWAgIBD2QWAgIBDzwrAAUBAA8WAh4VUGFyZW50TGV2ZWxzRGlzcGxheWVkZmRkAhkPZBYCAgEPZBYGAgcPZBYCAgMPDxYCHwhoZGQCCQ9kFgICAQ8WAh8IaGQCEQ8PFgIfCGhkFgICAw9kFgICAw9kFgICAQ88KwAJAQAPFgIeDU5ldmVyRXhwYW5kZWRnZGQYAQUsY3RsMDAkUGxhY2VIb2xkZXJUb3BOYXZCYXIkVG9wTmF2aWdhdGlvbk1lbnUPD2QFEkxNVSBDb3Vyc2UgQ2F0YWxvZ2QCbBUBwIm2HPqTlY9TBKKa5PIQ0L0KT5RIF3fR0EqSJw%3D%3D&__VIEWSTATEGENERATOR=8C686B5A&__SCROLLPOSITIONX=0&__SCROLLPOSITIONY=0&ctl00%24SPRibbon1=&ellucian-globalnav-search=&=Search%20Courses&=&=&__CALLBACKID=ctl00%24SPWebPartManager1%24g_5e211b2e_a356_4d78_b1a1_ce81bb03541c&__CALLBACKPARAM=GroupString%3D%253b%2523Art%253b%2523%26ConnectionFilterString%3D&__EVENTVALIDATION=%2FwEdAAJVjzL%2Fg1YMUWQWGHnatbmuMTZk9qLGncDjImqxppn1M5zalhem9fDqAQLUNbH07VUPBfYRZkNY1GAIM5T0XcpC"
    course_headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://coursecatalog.lmunet.edu',
        'Referer': 'https://coursecatalog.lmunet.edu/lists/courses/coursesbysubject.aspx'
        }

    directory_url = "https://www.lmunet.edu/directory/"
    calendar_url = "https://www.lmunet.edu/academics/academic-calendar"

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':

            # Comprehensive list of subjects for the course catalog
            payload_lists = ["Accounting","Allied Health Sciences","Anatomy","Appalachian Studies","Applied Statistical Analysis","Art","Athletic Training","Behavioral Science","Biochemistry","Biology","Broadcasting Communications","Business","Business Analytics","Career Development","Chemistry","Chinese Language","Civics","Civil Engineering","College of Veterinary Medicine","Communication Arts","Computer Science","Conflict Mgmt/Dispute Resolutn","Conservation Biology","Counseling","Criminal Justice","Curriculum and Instruction","DCOM Clinical","DCOM Clinical Rotation","DCOM Elective","DCOM Selective","Dental Hygiene","DO Scholar","Doctor of Busn. Admin.","Doctor of Education","Doctor of Med in Dentistry","Doctor of Medical Science","Doctor of Osteopathy","DVM, Orange Park","Early Child Development","Economics","Ed.D. Curriculum, Instruction","EdD Human Resource Developmnt","Educ Admin and Supervision","Education","Education Core","Educational Leadership","Engineering","Engineering Science","English","English As Second Language","English Language Institute","English Language Learners","Environmental Science","Executive Ldrshp-EDEL","Executive Ldrshp-EDL","Executive Leadership","Exercise Science","Finance","Forensic Dentistry","French","General Studies","Geography","Health","Health Care Administration","Higher Education","History","Honors","Human Resource Management","Humanities","Humanities and Fine Arts","Information Literacy","Information Systems","Instructional Ldrshp-EDIL","Instructional Ldrshp-IL","Instructional Leadership","Instructional Practices","Inter-Professional Education","Into to Phys Sci","Japanese Language","JFWA","Law","Leadership Bridge","Leadership Core","Life Science","Lincoln's Life","Management","Marketing","Master of Busn Admin","Master of Healthcare Admin","Master of Veterinary Education","Mathematics","Mechanical Engineering","Media Communication","Medical Laboratory Science","Mgmt and Leadership Studies","MS Busn. Analytics","MS Veterinary Clinical Care","Music","Nursing","Nursing Home Administration"]
            
            # Base POST payload for SharePoint callback requests
            base_payload = (
                "_wpcmWpid=&wpcmVal=&MSOWebPartPage_PostbackSource=&MSOTlPn_SelectedWpId=&MSOTlPn_View=0"
                "&MSOTlPn_ShowSettings=False&MSOGallery_SelectedLibrary=&MSOGallery_FilterString="
                "&MSOTlPn_Button=none&__EVENTTARGET=&__EVENTARGUMENT="
                "&__REQUESTDIGEST=0xBE1867094D82635B7508FE302D06B555D31EA3BED7F4D2EA8F9A5EFC9DB8CFB270097B16B7F4324D1D307CA7FC7C3C4DD8AE5B39DFF83FF2CD648A3B305DA322"
                "&MSOSPWebPartManager_DisplayModeName=Browse"
                "&MSOSPWebPartManager_ExitingDesignMode=false"
                "&_maintainWorkspaceScrollPosition=0"
                "&__CALLBACKID=ctl00%24SPWebPartManager1%24g_5e211b2e_a356_4d78_b1a1_ce81bb03541c"
                "&__CALLBACKPARAM={callback_param}"
            )
            for payload_list in payload_lists:
                encoded_subject = urllib.parse.quote(payload_list)
                callback_param = (f"GroupString%3D%253b%2523{encoded_subject}%253b%2523%26ConnectionFilterString%3D")
                course_payload = base_payload.format(callback_param=callback_param)
                yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=course_payload,callback=self.parse_course, dont_filter=True)
        
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:

            # Comprehensive list of subjects for the course catalog
            payload_lists = ["Accounting","Allied Health Sciences","Anatomy","Appalachian Studies","Applied Statistical Analysis","Art","Athletic Training","Behavioral Science","Biochemistry","Biology","Broadcasting Communications","Business","Business Analytics","Career Development","Chemistry","Chinese Language","Civics","Civil Engineering","College of Veterinary Medicine","Communication Arts","Computer Science","Conflict Mgmt/Dispute Resolutn","Conservation Biology","Counseling","Criminal Justice","Curriculum and Instruction","DCOM Clinical","DCOM Clinical Rotation","DCOM Elective","DCOM Selective","Dental Hygiene","DO Scholar","Doctor of Busn. Admin.","Doctor of Education","Doctor of Med in Dentistry","Doctor of Medical Science","Doctor of Osteopathy","DVM, Orange Park","Early Child Development","Economics","Ed.D. Curriculum, Instruction","EdD Human Resource Developmnt","Educ Admin and Supervision","Education","Education Core","Educational Leadership","Engineering","Engineering Science","English","English As Second Language","English Language Institute","English Language Learners","Environmental Science","Executive Ldrshp-EDEL","Executive Ldrshp-EDL","Executive Leadership","Exercise Science","Finance","Forensic Dentistry","French","General Studies","Geography","Health","Health Care Administration","Higher Education","History","Honors","Human Resource Management","Humanities","Humanities and Fine Arts","Information Literacy","Information Systems","Instructional Ldrshp-EDIL","Instructional Ldrshp-IL","Instructional Leadership","Instructional Practices","Inter-Professional Education","Into to Phys Sci","Japanese Language","JFWA","Law","Leadership Bridge","Leadership Core","Life Science","Lincoln's Life","Management","Marketing","Master of Busn Admin","Master of Healthcare Admin","Master of Veterinary Education","Mathematics","Mechanical Engineering","Media Communication","Medical Laboratory Science","Mgmt and Leadership Studies","MS Busn. Analytics","MS Veterinary Clinical Care","Music","Nursing","Nursing Home Administration"]
            
            # Base POST payload for SharePoint callback requests
            base_payload = (
                "_wpcmWpid=&wpcmVal=&MSOWebPartPage_PostbackSource=&MSOTlPn_SelectedWpId=&MSOTlPn_View=0"
                "&MSOTlPn_ShowSettings=False&MSOGallery_SelectedLibrary=&MSOGallery_FilterString="
                "&MSOTlPn_Button=none&__EVENTTARGET=&__EVENTARGUMENT="
                "&__REQUESTDIGEST=0xBE1867094D82635B7508FE302D06B555D31EA3BED7F4D2EA8F9A5EFC9DB8CFB270097B16B7F4324D1D307CA7FC7C3C4DD8AE5B39DFF83FF2CD648A3B305DA322"
                "&MSOSPWebPartManager_DisplayModeName=Browse"
                "&MSOSPWebPartManager_ExitingDesignMode=false"
                "&_maintainWorkspaceScrollPosition=0"
                "&__CALLBACKID=ctl00%24SPWebPartManager1%24g_5e211b2e_a356_4d78_b1a1_ce81bb03541c"
                "&__CALLBACKPARAM={callback_param}"
            )
            for payload_list in payload_lists:
                encoded_subject = urllib.parse.quote(payload_list)
                callback_param = (f"GroupString%3D%253b%2523{encoded_subject}%253b%2523%26ConnectionFilterString%3D")
                course_payload = base_payload.format(callback_param=callback_param)
                yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=course_payload,callback=self.parse_course, dont_filter=True)
            
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:

            # Comprehensive list of subjects for the course catalog
            payload_lists = ["Accounting","Allied Health Sciences","Anatomy","Appalachian Studies","Applied Statistical Analysis","Art","Athletic Training","Behavioral Science","Biochemistry","Biology","Broadcasting Communications","Business","Business Analytics","Career Development","Chemistry","Chinese Language","Civics","Civil Engineering","College of Veterinary Medicine","Communication Arts","Computer Science","Conflict Mgmt/Dispute Resolutn","Conservation Biology","Counseling","Criminal Justice","Curriculum and Instruction","DCOM Clinical","DCOM Clinical Rotation","DCOM Elective","DCOM Selective","Dental Hygiene","DO Scholar","Doctor of Busn. Admin.","Doctor of Education","Doctor of Med in Dentistry","Doctor of Medical Science","Doctor of Osteopathy","DVM, Orange Park","Early Child Development","Economics","Ed.D. Curriculum, Instruction","EdD Human Resource Developmnt","Educ Admin and Supervision","Education","Education Core","Educational Leadership","Engineering","Engineering Science","English","English As Second Language","English Language Institute","English Language Learners","Environmental Science","Executive Ldrshp-EDEL","Executive Ldrshp-EDL","Executive Leadership","Exercise Science","Finance","Forensic Dentistry","French","General Studies","Geography","Health","Health Care Administration","Higher Education","History","Honors","Human Resource Management","Humanities","Humanities and Fine Arts","Information Literacy","Information Systems","Instructional Ldrshp-EDIL","Instructional Ldrshp-IL","Instructional Leadership","Instructional Practices","Inter-Professional Education","Into to Phys Sci","Japanese Language","JFWA","Law","Leadership Bridge","Leadership Core","Life Science","Lincoln's Life","Management","Marketing","Master of Busn Admin","Master of Healthcare Admin","Master of Veterinary Education","Mathematics","Mechanical Engineering","Media Communication","Medical Laboratory Science","Mgmt and Leadership Studies","MS Busn. Analytics","MS Veterinary Clinical Care","Music","Nursing","Nursing Home Administration"]
            
            # Base POST payload for SharePoint callback requests
            base_payload = (
                "_wpcmWpid=&wpcmVal=&MSOWebPartPage_PostbackSource=&MSOTlPn_SelectedWpId=&MSOTlPn_View=0"
                "&MSOTlPn_ShowSettings=False&MSOGallery_SelectedLibrary=&MSOGallery_FilterString="
                "&MSOTlPn_Button=none&__EVENTTARGET=&__EVENTARGUMENT="
                "&__REQUESTDIGEST=0xBE1867094D82635B7508FE302D06B555D31EA3BED7F4D2EA8F9A5EFC9DB8CFB270097B16B7F4324D1D307CA7FC7C3C4DD8AE5B39DFF83FF2CD648A3B305DA322"
                "&MSOSPWebPartManager_DisplayModeName=Browse"
                "&MSOSPWebPartManager_ExitingDesignMode=false"
                "&_maintainWorkspaceScrollPosition=0"
                "&__CALLBACKID=ctl00%24SPWebPartManager1%24g_5e211b2e_a356_4d78_b1a1_ce81bb03541c"
                "&__CALLBACKPARAM={callback_param}"
            )
            for payload_list in payload_lists:
                encoded_subject = urllib.parse.quote(payload_list)
                callback_param = (f"GroupString%3D%253b%2523{encoded_subject}%253b%2523%26ConnectionFilterString%3D")
                course_payload = base_payload.format(callback_param=callback_param)
                yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=course_payload,callback=self.parse_course, dont_filter=True)
            
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:

            # Comprehensive list of subjects for the course catalog
            payload_lists = ["Accounting","Allied Health Sciences","Anatomy","Appalachian Studies","Applied Statistical Analysis","Art","Athletic Training","Behavioral Science","Biochemistry","Biology","Broadcasting Communications","Business","Business Analytics","Career Development","Chemistry","Chinese Language","Civics","Civil Engineering","College of Veterinary Medicine","Communication Arts","Computer Science","Conflict Mgmt/Dispute Resolutn","Conservation Biology","Counseling","Criminal Justice","Curriculum and Instruction","DCOM Clinical","DCOM Clinical Rotation","DCOM Elective","DCOM Selective","Dental Hygiene","DO Scholar","Doctor of Busn. Admin.","Doctor of Education","Doctor of Med in Dentistry","Doctor of Medical Science","Doctor of Osteopathy","DVM, Orange Park","Early Child Development","Economics","Ed.D. Curriculum, Instruction","EdD Human Resource Developmnt","Educ Admin and Supervision","Education","Education Core","Educational Leadership","Engineering","Engineering Science","English","English As Second Language","English Language Institute","English Language Learners","Environmental Science","Executive Ldrshp-EDEL","Executive Ldrshp-EDL","Executive Leadership","Exercise Science","Finance","Forensic Dentistry","French","General Studies","Geography","Health","Health Care Administration","Higher Education","History","Honors","Human Resource Management","Humanities","Humanities and Fine Arts","Information Literacy","Information Systems","Instructional Ldrshp-EDIL","Instructional Ldrshp-IL","Instructional Leadership","Instructional Practices","Inter-Professional Education","Into to Phys Sci","Japanese Language","JFWA","Law","Leadership Bridge","Leadership Core","Life Science","Lincoln's Life","Management","Marketing","Master of Busn Admin","Master of Healthcare Admin","Master of Veterinary Education","Mathematics","Mechanical Engineering","Media Communication","Medical Laboratory Science","Mgmt and Leadership Studies","MS Busn. Analytics","MS Veterinary Clinical Care","Music","Nursing","Nursing Home Administration"]
            
            # Base POST payload for SharePoint callback requests
            base_payload = (
                "_wpcmWpid=&wpcmVal=&MSOWebPartPage_PostbackSource=&MSOTlPn_SelectedWpId=&MSOTlPn_View=0"
                "&MSOTlPn_ShowSettings=False&MSOGallery_SelectedLibrary=&MSOGallery_FilterString="
                "&MSOTlPn_Button=none&__EVENTTARGET=&__EVENTARGUMENT="
                "&__REQUESTDIGEST=0xBE1867094D82635B7508FE302D06B555D31EA3BED7F4D2EA8F9A5EFC9DB8CFB270097B16B7F4324D1D307CA7FC7C3C4DD8AE5B39DFF83FF2CD648A3B305DA322"
                "&MSOSPWebPartManager_DisplayModeName=Browse"
                "&MSOSPWebPartManager_ExitingDesignMode=false"
                "&_maintainWorkspaceScrollPosition=0"
                "&__CALLBACKID=ctl00%24SPWebPartManager1%24g_5e211b2e_a356_4d78_b1a1_ce81bb03541c"
                "&__CALLBACKPARAM={callback_param}"
            )
            for payload_list in payload_lists:
                encoded_subject = urllib.parse.quote(payload_list)
                callback_param = (f"GroupString%3D%253b%2523{encoded_subject}%253b%2523%26ConnectionFilterString%3D")
                course_payload = base_payload.format(callback_param=callback_param)
                yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=course_payload,callback=self.parse_course, dont_filter=True)
            
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)


    def parse_course(self, response):
        blocks = response.xpath("//tr")
        try:
            for i, block in enumerate(blocks):
                course_code = block.xpath("./td[@class='ms-vb2'][1]/text()").get('')
                description = block.xpath("./td[@class='ms-vb2'][2]//text()").get('')
                title = block.xpath("./td[@class='ms-vb-title']//a/text()").get('')
                course_name = course_code + ' ' + title
                title_link = response.urljoin(block.xpath("./td[@class='ms-vb-title']//a/@href").get(''))
                location = ''
                istructor = ''
                course_date = ''
                section_found = False

                # Using Playwright for dynamic section detail expansion
                try:
                    with sync_playwright() as p:
                        browser = p.chromium.launch(headless=True)
                        page = browser.new_page()
                        page.goto(title_link)
                        import time
                        time.sleep(3)
                        ins_response = Selector(text=page.content())
                        section_blocks = ins_response.xpath('//div[@class="ellucian-coursecatalog-sectiondetail"]')
                        if section_blocks:
                            section_found = True
                            for section_block in section_blocks:
                                location = section_block.xpath('.//span[@data-bind="text: $data.Location"]/text()').get('')
                                sections = section_block.xpath('.//h4[@data-bind="text: Name"]/text()').get('')
                                sections = sections.split('-')[-1]
                                istructor = section_block.xpath('.//span[@data-bind="text: Faculty"]/text()').get('')
                                start_date = section_block.xpath('.//span[@data-bind="shortDate: StartDate"]/text()').get('')
                                end_date = section_block.xpath('.//span[@data-bind="shortDate: EndDate"]/text()').get('')
                                course_date = f"{start_date} - {end_date}"
                                self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": title_link,
                                    "Course Name": course_name,
                                    "Course Description": description,
                                    "Class Number": course_code,
                                    "Section": sections,
                                    "Instructor": istructor,
                                    "Enrollment": "",
                                    "Course Dates": course_date,
                                    "Location": location,
                                    "Textbook/Course Materials": "",
                                })
                except Exception as e:
                    pass

                    if not section_found:
                        # If no section blocks, still append one record
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": title_link,
                            "Course Name": course_name,
                            "Course Description": description,
                            "Class Number": course_code,
                            "Section": "",
                            "Instructor": "",
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": "",
                            "Textbook/Course Materials": "",
                        })
        except Exception as e:
            pass
    
        # SAVE
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")
        
    def parse_directory(self, response):
        """
        Parse directory using Scrapy response.

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Name
        - Title
        - Email
        - Phone Number
        """

        rows = []

        people = response.xpath('//div[contains(@class,"facultyLink")]')
        for person in people:
            name_parts = person.xpath('.//div[@class="facultyName"]//text()').getall()
            title = person.xpath('.//div[@class="facultyTitle"]/text()').get()
            email = person.xpath('.//div[@class="email"]/a/text()').get()
            phone = person.xpath('.//div[@class="phone"]/a/text()').get()

            # clean whitespace
            if title:
                title = ' '.join(title.split())
            name = " ".join(p.strip() for p in name_parts if p.strip())
            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone
            })

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        Parse academic calendar PDFs in ONE function.
        """

        rows = []

        #Extract PDF links
        pdf_links = response.xpath('//a[contains(@href, ".pdf")]/@href').getall()
        pdf_urls = [response.urljoin(link) for link in pdf_links]

        #Download + parse each PDF (blocking)
        for pdf_url in pdf_urls:
            pdf_response = yield scrapy.Request(pdf_url, dont_filter=True)
            with pdfplumber.open(io.BytesIO(pdf_response.body)) as pdf:
                current_term = None
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue

                    lines = [l.strip() for l in text.splitlines() if l.strip()]
                    for line in lines:
                        # TERM HEADER
                        if re.match(r'^(Fall|Spring|Summer)\s+(Semester|Term)\s+\d{4}', line):
                            # current_term = line.replace('*', '').strip()/
                            current_term = re.split(r'\.{2,}', line)[0].replace('*', '').strip()
                            continue

                        if not current_term:
                            continue

                        # SKIP NON-DATA
                        if line.startswith("LINCOLN MEMORIAL"):
                            continue
                        if line.startswith("Official University Holidays"):
                            continue
                        if line.startswith("During the"):
                            continue
                        if line.startswith("("):
                            continue

                        # EVENT LINE: description .... date
                        parts = re.split(r'\.{2,}', line)
                        if len(parts) < 2:
                            continue

                        term_desc = parts[0].strip()
                        term_date = parts[-1].strip()

                        if not term_desc or not term_date:
                            continue

                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_url,
                            "Term Name": current_term,
                            "Term Date": term_date,
                            "Term Date Description": term_desc
                        })

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "calendar")
