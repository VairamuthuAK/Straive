import scrapy
import requests
import pandas as pd
from ..utils import *
from ..utils import save_df
from parsel import Selector

class NjcSpider(scrapy.Spider):
    """
    Spider for Northeastern Junior College (NJC).
    Scrapes Courses (via Banner self-service), Staff Directory, and Academic Calendars.
    """

    name = "njc"
    institution_id = 258442879830091740

    # In-memory storage to accumulate items before saving in closed() or at end of parse
    course_rows = []
    calendar_rows = []
    directory_rows = []

    course_url = 'https://erpdnssb.cccs.edu/PRODNJC/bwckschd.p_get_crse_unsec'
    course_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://erpdnssb.cccs.edu',
            'Referer': 'https://erpdnssb.cccs.edu/PRODNJC/bwckgens.p_proc_term_date'
            }
    # Target URLs
    directory_url = "https://www.njc.edu/directory?firstname=&dept=&lastname=&aos="
    directory_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': 'https://www.njc.edu/directory?firstname=&dept=&lastname=&aos=',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        }

    calendar_URL = "https://www.njc.edu/academic-calendar/term?term=419"

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            term_lists = ['202630','202620','202610','202530']
            for term_list in term_lists:
                course_payload = f'term_in={term_list}&sel_subj=dummy&sel_day=dummy&sel_schd=dummy&sel_insm=dummy&sel_camp=dummy&sel_levl=dummy&sel_sess=dummy&sel_instr=dummy&sel_ptrm=dummy&sel_attr=dummy&sel_subj=ACC&sel_subj=AAA&sel_subj=AGR&sel_subj=AGB&sel_subj=ABM&sel_subj=AGY&sel_subj=AGE&sel_subj=AME&sel_subj=AGP&sel_subj=ASC&sel_subj=ANT&sel_subj=ART&sel_subj=AST&sel_subj=ASE&sel_subj=BIO&sel_subj=BUS&sel_subj=CHE&sel_subj=COM&sel_subj=CNG&sel_subj=CIS&sel_subj=CSC&sel_subj=CWB&sel_subj=COS&sel_subj=CRJ&sel_subj=DPM&sel_subj=ECE&sel_subj=ECO&sel_subj=EDU&sel_subj=EIC&sel_subj=ELT&sel_subj=EMS&sel_subj=ENG&sel_subj=ENV&sel_subj=EQM&sel_subj=EQT&sel_subj=EST&sel_subj=FIN&sel_subj=FST&sel_subj=FSW&sel_subj=GEO&sel_subj=GEY&sel_subj=GER&sel_subj=HWE&sel_subj=HPR&sel_subj=HIS&sel_subj=HNR&sel_subj=HPE&sel_subj=HUM&sel_subj=IMA&sel_subj=JOU&sel_subj=LIT&sel_subj=MAN&sel_subj=MAR&sel_subj=MAT&sel_subj=MIL&sel_subj=MGD&sel_subj=MUS&sel_subj=NAT&sel_subj=NUR&sel_subj=NUA&sel_subj=PHI&sel_subj=PED&sel_subj=PHY&sel_subj=PSC&sel_subj=PSY&sel_subj=RAM&sel_subj=REA&sel_subj=REC&sel_subj=SCI&sel_subj=SOC&sel_subj=SPA&sel_subj=THE&sel_subj=WEL&sel_subj=WTG&sel_subj=WST&sel_crse=&sel_title=&sel_schd=%25&sel_insm=%25&sel_from_cred=&sel_to_cred=&sel_camp=&sel_levl=%25&sel_ptrm=%25&sel_instr=%25&sel_sess=%25&sel_attr=%25&begin_hh=0&begin_mi=0&begin_ap=a&end_hh=0&end_mi=0&end_ap=a'
                yield scrapy.Request(url=self.course_url,headers=self.course_headers,body=course_payload,method="POST",callback=self.parse_course, dont_filter=True)
        
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,callback=self.parse_directory,dont_filter=True)
        
        elif mode == 'calendar':
            term_lists = ['419','418','417','416','415','414','420','404','405','403','402']
            for term_list in term_lists:
                calendar_url = f"https://www.njc.edu/academic-calendar/term?term={term_list}"
                yield scrapy.Request(url=calendar_url,callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            term_lists = ['202630','202620','202610','202530']
            for term_list in term_lists:
                course_payload = f'term_in={term_list}&sel_subj=dummy&sel_day=dummy&sel_schd=dummy&sel_insm=dummy&sel_camp=dummy&sel_levl=dummy&sel_sess=dummy&sel_instr=dummy&sel_ptrm=dummy&sel_attr=dummy&sel_subj=ACC&sel_subj=AAA&sel_subj=AGR&sel_subj=AGB&sel_subj=ABM&sel_subj=AGY&sel_subj=AGE&sel_subj=AME&sel_subj=AGP&sel_subj=ASC&sel_subj=ANT&sel_subj=ART&sel_subj=AST&sel_subj=ASE&sel_subj=BIO&sel_subj=BUS&sel_subj=CHE&sel_subj=COM&sel_subj=CNG&sel_subj=CIS&sel_subj=CSC&sel_subj=CWB&sel_subj=COS&sel_subj=CRJ&sel_subj=DPM&sel_subj=ECE&sel_subj=ECO&sel_subj=EDU&sel_subj=EIC&sel_subj=ELT&sel_subj=EMS&sel_subj=ENG&sel_subj=ENV&sel_subj=EQM&sel_subj=EQT&sel_subj=EST&sel_subj=FIN&sel_subj=FST&sel_subj=FSW&sel_subj=GEO&sel_subj=GEY&sel_subj=GER&sel_subj=HWE&sel_subj=HPR&sel_subj=HIS&sel_subj=HNR&sel_subj=HPE&sel_subj=HUM&sel_subj=IMA&sel_subj=JOU&sel_subj=LIT&sel_subj=MAN&sel_subj=MAR&sel_subj=MAT&sel_subj=MIL&sel_subj=MGD&sel_subj=MUS&sel_subj=NAT&sel_subj=NUR&sel_subj=NUA&sel_subj=PHI&sel_subj=PED&sel_subj=PHY&sel_subj=PSC&sel_subj=PSY&sel_subj=RAM&sel_subj=REA&sel_subj=REC&sel_subj=SCI&sel_subj=SOC&sel_subj=SPA&sel_subj=THE&sel_subj=WEL&sel_subj=WTG&sel_subj=WST&sel_crse=&sel_title=&sel_schd=%25&sel_insm=%25&sel_from_cred=&sel_to_cred=&sel_camp=&sel_levl=%25&sel_ptrm=%25&sel_instr=%25&sel_sess=%25&sel_attr=%25&begin_hh=0&begin_mi=0&begin_ap=a&end_hh=0&end_mi=0&end_ap=a'
                yield scrapy.Request(url=self.course_url,headers=self.course_headers,body=course_payload,method="POST",callback=self.parse_course, dont_filter=True)
            
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,callback=self.parse_directory,dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            term_lists = ['202630','202620','202610','202530']
            for term_list in term_lists:
                course_payload = f'term_in={term_list}&sel_subj=dummy&sel_day=dummy&sel_schd=dummy&sel_insm=dummy&sel_camp=dummy&sel_levl=dummy&sel_sess=dummy&sel_instr=dummy&sel_ptrm=dummy&sel_attr=dummy&sel_subj=ACC&sel_subj=AAA&sel_subj=AGR&sel_subj=AGB&sel_subj=ABM&sel_subj=AGY&sel_subj=AGE&sel_subj=AME&sel_subj=AGP&sel_subj=ASC&sel_subj=ANT&sel_subj=ART&sel_subj=AST&sel_subj=ASE&sel_subj=BIO&sel_subj=BUS&sel_subj=CHE&sel_subj=COM&sel_subj=CNG&sel_subj=CIS&sel_subj=CSC&sel_subj=CWB&sel_subj=COS&sel_subj=CRJ&sel_subj=DPM&sel_subj=ECE&sel_subj=ECO&sel_subj=EDU&sel_subj=EIC&sel_subj=ELT&sel_subj=EMS&sel_subj=ENG&sel_subj=ENV&sel_subj=EQM&sel_subj=EQT&sel_subj=EST&sel_subj=FIN&sel_subj=FST&sel_subj=FSW&sel_subj=GEO&sel_subj=GEY&sel_subj=GER&sel_subj=HWE&sel_subj=HPR&sel_subj=HIS&sel_subj=HNR&sel_subj=HPE&sel_subj=HUM&sel_subj=IMA&sel_subj=JOU&sel_subj=LIT&sel_subj=MAN&sel_subj=MAR&sel_subj=MAT&sel_subj=MIL&sel_subj=MGD&sel_subj=MUS&sel_subj=NAT&sel_subj=NUR&sel_subj=NUA&sel_subj=PHI&sel_subj=PED&sel_subj=PHY&sel_subj=PSC&sel_subj=PSY&sel_subj=RAM&sel_subj=REA&sel_subj=REC&sel_subj=SCI&sel_subj=SOC&sel_subj=SPA&sel_subj=THE&sel_subj=WEL&sel_subj=WTG&sel_subj=WST&sel_crse=&sel_title=&sel_schd=%25&sel_insm=%25&sel_from_cred=&sel_to_cred=&sel_camp=&sel_levl=%25&sel_ptrm=%25&sel_instr=%25&sel_sess=%25&sel_attr=%25&begin_hh=0&begin_mi=0&begin_ap=a&end_hh=0&end_mi=0&end_ap=a'
                yield scrapy.Request(url=self.course_url,headers=self.course_headers,body=course_payload,method="POST",callback=self.parse_course, dont_filter=True)
            
            term_lists = ['419','418','417','416','415','414','420','404','405','403','402']
            for term_list in term_lists:
                calendar_url = f"https://www.njc.edu/academic-calendar/term?term={term_list}"
                yield scrapy.Request(url=calendar_url,callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            term_lists = ['419','418','417','416','415','414','420','404','405','403','402']
            for term_list in term_lists:
                calendar_url = f"https://www.njc.edu/academic-calendar/term?term={term_list}"
                yield scrapy.Request(url=calendar_url,callback=self.parse_calendar)
            
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,callback=self.parse_directory,dont_filter=True)
        
        # All three (default)
        else:
            term_lists = ['202630','202620','202610','202530']
            for term_list in term_lists:
                course_payload = f'term_in={term_list}&sel_subj=dummy&sel_day=dummy&sel_schd=dummy&sel_insm=dummy&sel_camp=dummy&sel_levl=dummy&sel_sess=dummy&sel_instr=dummy&sel_ptrm=dummy&sel_attr=dummy&sel_subj=ACC&sel_subj=AAA&sel_subj=AGR&sel_subj=AGB&sel_subj=ABM&sel_subj=AGY&sel_subj=AGE&sel_subj=AME&sel_subj=AGP&sel_subj=ASC&sel_subj=ANT&sel_subj=ART&sel_subj=AST&sel_subj=ASE&sel_subj=BIO&sel_subj=BUS&sel_subj=CHE&sel_subj=COM&sel_subj=CNG&sel_subj=CIS&sel_subj=CSC&sel_subj=CWB&sel_subj=COS&sel_subj=CRJ&sel_subj=DPM&sel_subj=ECE&sel_subj=ECO&sel_subj=EDU&sel_subj=EIC&sel_subj=ELT&sel_subj=EMS&sel_subj=ENG&sel_subj=ENV&sel_subj=EQM&sel_subj=EQT&sel_subj=EST&sel_subj=FIN&sel_subj=FST&sel_subj=FSW&sel_subj=GEO&sel_subj=GEY&sel_subj=GER&sel_subj=HWE&sel_subj=HPR&sel_subj=HIS&sel_subj=HNR&sel_subj=HPE&sel_subj=HUM&sel_subj=IMA&sel_subj=JOU&sel_subj=LIT&sel_subj=MAN&sel_subj=MAR&sel_subj=MAT&sel_subj=MIL&sel_subj=MGD&sel_subj=MUS&sel_subj=NAT&sel_subj=NUR&sel_subj=NUA&sel_subj=PHI&sel_subj=PED&sel_subj=PHY&sel_subj=PSC&sel_subj=PSY&sel_subj=RAM&sel_subj=REA&sel_subj=REC&sel_subj=SCI&sel_subj=SOC&sel_subj=SPA&sel_subj=THE&sel_subj=WEL&sel_subj=WTG&sel_subj=WST&sel_crse=&sel_title=&sel_schd=%25&sel_insm=%25&sel_from_cred=&sel_to_cred=&sel_camp=&sel_levl=%25&sel_ptrm=%25&sel_instr=%25&sel_sess=%25&sel_attr=%25&begin_hh=0&begin_mi=0&begin_ap=a&end_hh=0&end_mi=0&end_ap=a'
                yield scrapy.Request(url=self.course_url,headers=self.course_headers,body=course_payload,method="POST",callback=self.parse_course, dont_filter=True)
            
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,callback=self.parse_directory,dont_filter=True)
            
            term_lists = ['419','418','417','416','415','414','420','404','405','403','402']
            for term_list in term_lists:
                calendar_url = f"https://www.njc.edu/academic-calendar/term?term={term_list}"
                yield scrapy.Request(url=calendar_url,callback=self.parse_calendar)
       

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
        
        # Course titles are stored in <th> elements with class 'ddtitle'
        blocks = response.xpath('//th[@class="ddtitle"]')
        for i, block in enumerate(blocks):
            title = block.xpath('./a/text()').get(default="").strip()
            course_link = response.urljoin(block.xpath('./a/@href').get(default=""))
            course_res = requests.get(course_link)
            course_response = Selector(text=course_res.text)
            cap = course_response.xpath('//table[@summary="This layout table is used to present the seating numbers."]//tr[2]/td[1]/text()').get('')
            rem = course_response.xpath('//table[@summary="This layout table is used to present the seating numbers."]//tr[2]/td[2]/text()').get('')
            enroll = rem + '/' + cap

            # Banner titles are usually: "Course Name - CRN - Subject/Number - Section"
            parts = [p.strip() for p in title.split(' -')]
            name = " - ".join(parts[:-3])
            class_number = parts[-3]
            class_name_number = parts[-2]
            section = parts[-1]
            course_name = f"{class_name_number} {name}"

            # Meeting info is in the row immediately following the title row
            detail_row = block.xpath('./parent::tr/following-sibling::tr[1]')
            meeting_rows = detail_row.xpath(
                './/table[@summary="This table lists the scheduled meeting times and assigned instructors for this class.."]//tr[position()>1]'
            )

            # Extract description, excluding rows that are just semester names
            course_description = detail_row.xpath('./td/text()[normalize-space()]').get(default="").strip()
            if course_description.lower().startswith(("fall", "spring", "summer", "winter")):
                course_description = ""

            #One output per meeting row
            for row in meeting_rows:
                time = row.xpath('./td[2]/text()').get(default="").strip()
                days = row.xpath('./td[3]/text()').get(default="").strip()
                location = row.xpath('./td[4]/text()').get(default="").strip()
                course_dates = row.xpath('./td[5]/text()').get(default="").strip()
                schedule_type = row.xpath('./td[6]/text()').get(default="").strip()
                instructor = row.xpath('./td[7]//text()').getall()
                instructor = ''.join(instructor) 
                
                self.course_rows.append(
                    {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": "https://erpdnssb.cccs.edu/PRODNJC/bwckschd.p_disp_dyn_sched",
                    "Course Name": course_name,
                    "Course Description": course_description,
                    "Class Number": class_number,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": enroll,
                    "Course Dates": course_dates,
                    "Location": '',
                    "Textbook/Course Materials": "",
                    }
                )

        #SAVE OUTPUT CSV
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
        
        blocks = response.xpath('//div[@class="directory-card clearfix"]')
        for block in blocks:
            # Extract and join text components to handle nested HTML tags
            name = block.xpath('.//div[@class="field-name"]//text()').getall()
            name = ''.join([e.strip() for e in name if e.strip()])
            title = block.xpath('.//div[@class="field-job-title"]//text()').getall()
            title = ''.join([e.strip() for e in title if e.strip()])

            # Phone handling: Clean up 'mobile' labels and trailing commas
            phone = block.xpath('.//div[@class="field-icon field-telephone"]//text()').getall()
            phone = ', '.join([e.strip() for e in phone if e.strip()]).replace('(mobile)', '').rstrip(',')
            email = block.xpath('.//div[@class="field-icon field-email-address"]//text()').getall()
            email = ''.join([e.strip() for e in email if e.strip()])
            self.directory_rows.append(
                {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                    }
                )

    def parse_calendar(self,response):
       
        blocks = response.xpath('//div[@class="col col-12 animated views-row"]')
        for block in blocks:
            term_name = ''.join(e.strip() for e in block.xpath('.//div[@class="views-field views-field-field-year-group"]/div[@class="field-content"]//text()').getall() if e.strip())
            # Date is usually formatted as Month-Day-Year via joining pieces
            term_date = '-'.join(block.xpath('.//div[@class="event-date"]//text()').getall())
            term_description = block.xpath('.//div[@class="views-field views-field-title"]/span/a/text()').get('')
        
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": term_name,
                "Term Date": term_date,
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




 