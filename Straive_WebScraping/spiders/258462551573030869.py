import re
import scrapy
import base64
import pdfplumber
import urllib.parse
import pandas as pd
from ..utils import *
from ..utils import save_df
from datetime import datetime


def parse_directory_decode_olc_email(captcha_id):
    """
    Decodes an obfuscated email address from a CAPTCHA-based ID string.
    
    The function extracts a Base64 encoded segment, fixes padding, 
    decodes it from Latin-1, and applies specific domain replacements 
    relevant to the OLC directory.
    """

    try:
        # Extract the encoded part by removing the prefix and skipping the first 32 chars
        encoded_str = captcha_id.replace('jquerycaptcha-', '')[32:]

        # Base64 strings must be multiples of 4; add padding if necessary
        missing_padding = len(encoded_str) % 4
        if missing_padding:
            encoded_str += '=' * (4 - missing_padding)
        decoded_bytes = base64.b64decode(encoded_str)
        decoded_text = decoded_bytes.decode('latin-1')

        # Data cleaning: OLC uses '*' as a placeholder for '.' in their obfuscation
        email = decoded_text.replace('*', '.')
        if email.startswith('@'):
            email = email[1:]
        email = urllib.parse.unquote(email)

        # Specific fix for the 'edugo.com' typo/obfuscation in the source
        email = email.replace('edugo.com', 'edu')
        return email
    except Exception:
        return None


class OlcSpider(scrapy.Spider):
    name = "olc"
    institution_id = 258462551573030869
    
    # Buffers to hold data during the crawl session
    calendar_rows = []
    directory_rows = []

    course_url = "https://olcedu43303.sharepoint.com/sites/OLCPublicDocuments/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FOLCPublicDocuments%2FShared%20Documents%2FCollege%20Catalogs%2FOLC_Catalog_25-26%2Epdf&parent=%2Fsites%2FOLCPublicDocuments%2FShared%20Documents%2FCollege%20Catalogs&p=true&ga=1"
    directory_urls = "https://www.olc.edu/staff/list/"
    calendar_url = "https://www.olc.edu/olc-calendar/api/month/02/year/2026/?_=1768629206643"

    
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
           pagess = ['1','2','3','4','5','6','7']
           for pages in pagess:
                directory_url = f"https://www.olc.edu/staff/list/page/{pages}/"
                yield scrapy.Request(url=directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            lists_months = ['01','02','03','04','05','06','07','08','09','10','11','12']
            for lists_month in lists_months:
                calender_url1 = f"https://www.olc.edu/olc-calendar/api/month/{lists_month}/year/2026/?_=1768629206643"
                yield scrapy.Request(url=calender_url1, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            pagess = ['1','2','3','4','5','6','7']
            for pages in pagess:
                directory_url = f"https://www.olc.edu/staff/list/page/{pages}/"
                yield scrapy.Request(url=directory_url, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            lists_months = ['01','02','03','04','05','06','07','08','09','10','11','12']
            for lists_month in lists_months:
                calender_url1 = f"https://www.olc.edu/olc-calendar/api/month/{lists_month}/year/2026/?_=1768629206643"
                yield scrapy.Request(url=calender_url1, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            lists_months = ['01','02','03','04','05','06','07','08','09','10','11','12']
            for lists_month in lists_months:
                calender_url1 = f"https://www.olc.edu/olc-calendar/api/month/{lists_month}/year/2026/?_=1768629206643"
                yield scrapy.Request(url=calender_url1, callback=self.parse_calendar)
            pagess = ['1','2','3','4','5','6','7']
            for pages in pagess:
                directory_url = f"https://www.olc.edu/staff/list/page/{pages}/"
                yield scrapy.Request(url=directory_url, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            pagess = ['1','2','3','4','5','6','7']
            for pages in pagess:
                directory_url = f"https://www.olc.edu/staff/list/page/{pages}/"
                yield scrapy.Request(url=directory_url, callback=self.parse_directory, dont_filter=True)
            lists_months = ['01','02','03','04','05','06','07','08','09','10','11','12']
            for lists_month in lists_months:
                calender_url1 = f"https://www.olc.edu/olc-calendar/api/month/{lists_month}/year/2026/?_=1768629206643"
                yield scrapy.Request(url=calender_url1, callback=self.parse_calendar)
       

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
        # Page ranges defined based on the PDF's Table of Contents for course descriptions 
        course_rows = []
        page_ranges = [
        (66, 71), (97, 108), (114, 123), (132, 138), 
        (155, 173), (179, 183), (188, 193), (202, 208) ,(215,216) ,(217,223)
        ]

        all_courses = []
        
        # Regex to identify the start of a course (e.g., ACCT 103)
        # course_start_re = re.compile(r'^([A-Z][a-zA-Z]{1,3}\s\d{3})')
        course_start_re = re.compile(r'^([A-Z][a-zA-Z]{1,7}\s+\d{3}[*]?)')
        pdf_path = 'OLC_Catalog_25-26.pdf'
        with pdfplumber.open(pdf_path) as pdf:
            for start, end in page_ranges:
                for i in range(start, end):
                    # Ensure we don't go out of bounds
                    if i >= len(pdf.pages):
                        continue
                        
                    page = pdf.pages[i]
                    text = page.extract_text()
                    if not text:
                        continue

                    lines = text.split('\n')
                    current_course = None

                    for line in lines:
                        line = line.strip()
                        # Check if line starts with a course code (e.g., "BAd 423")
                        match = course_start_re.match(line)
                        
                        if match:
                            # Save previous course before starting a new one
                            if current_course:
                                all_courses.append(current_course)
                            
                            # Split by 'credits' to separate Name from Description
                            if 'credits' in line.lower():
                                parts = re.split(r'(\d+\s+credits)', line, flags=re.IGNORECASE)
                                # parts[0] = Name, parts[1] = Credits, parts[2] = Start of Description
                                name = parts[0].strip()
                                desc = parts[2].strip() if len(parts) > 2 else ""
                            else:
                                name = line
                                desc = ""
                            
                            current_course = {"course_name": name, "description": desc}
                        
                        elif current_course:
                            # Append continuation lines to the description
                            if 'credits' in line.lower() and not current_course["description"]:
                                # Handle cases where "credits" is on the second line
                                parts = re.split(r'(\d+\s+credits)', line, flags=re.IGNORECASE)
                                current_course["description"] = parts[2].strip() if len(parts) > 2 else ""
                            else:
                                current_course["description"] += " " + line

                    # Add the final course of the page
                    if current_course:
                        all_courses.append(current_course)

            for course in all_courses:
                # Clean up the description (remove double spaces and extra "Prerequisite" text)
                clean_desc = re.sub(r'\s+', ' ', course["description"]).strip()
                clean_desc = clean_desc.lstrip('. ')
                # If the description starts with "Prerequisite:", we keep it as it's part of the data
                class_number = ' '.join(course["course_name"].split(' ')[0:2])
                
                course_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": course["course_name"],
                        "Course Description": clean_desc,
                        "Class Number": class_number,
                        "Section": "",
                        "Instructor": "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": "",
                    }
                )

            # SAVE
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
        
        blocks = response.xpath('//div[@class="member-card card"]')
        for block in blocks:
            name = block.xpath('.//h3/text()').get('').replace('(Fax)','').replace('(DLRFax)','').replace('(DLR Fax)','').replace('(YRS)','').strip()
            title = block.xpath('.//h3/small/text()').get('').replace('(Fax)','').replace('(DLRFax)','').replace('(DLR Fax)','').replace('(YRS)','').strip()
            
            # Email addresses are hidden in the ID of a captcha-related link
            captcha_id = block.xpath('.//a[contains(@id, "jquerycaptcha")]/@id').get('')
            email = ''
            if captcha_id:
                email = parse_directory_decode_olc_email(captcha_id)
                if email:
                    # Final correction for specific character encoding artifacts
                    email = email.replace('a@','').replace('.olc�ugo.com','@olc.edu')

            # Extract phone numbers; handles multiple numbers per person
            phones = block.xpath(".//a[starts-with(@href, 'tel:')]/text()").getall()
            if len(phones) > 1:
                phone = ', '.join([p.replace('tel:', '').strip() for p in phones])
            elif phones:
                phone = phones[0].replace('tel:', '').strip()
                if phone == '':
                    phone = block.xpath(".//a[starts-with(@href, 'tel:')]/text()").get('')
            else:
                phone = ''

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

        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):
        """
        Parse academic calendar PDF and extract:
        - Term Name
        - Term Date
        - Term Date Description
        """
        blocks = response.xpath('//table[@class="days reservations table-responsive-lg"]//tr//li')
        for block in blocks:
            # Parse month and year from the API URL
            # Example URL: .../month/02/year/2026/
            part = response.url.split('month')[-1].split('?')[0].strip('/')
            month_num, _, year = part.split('/')
            month_name = datetime.strptime(month_num, "%m").strftime("%B")
            result = f"{month_name} {year}"

            # Extract description and specific day number
            term_description = block.xpath('./strong/text()|/a/strong/text()').get('')
            term_date =  block.xpath('.//ancestor::td/div[@class="day-number"]/text()').get('')
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": 'https://www.olc.edu/olc-calendar/',
                "Term Name": result,
                "Term Date": term_date,
                "Term Date Description": term_description
            })


        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
