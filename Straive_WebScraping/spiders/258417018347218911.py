import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
import cloudscraper
from ..utils import save_df
from parsel import Selector


def parse_directory_decode_cloudflare_email(encoded_string):
    """
    Decodes Cloudflare protected email addresses.

    Cloudflare protects emails by encoding them in hexadecimal format.
    This function decodes that hex string back into a readable email.

    :param encoded_string: Hex encoded email string from data-cfemail attribute
    :return: Decoded email address (string)
    """
    r = int(encoded_string[:2], 16)
    email = ''.join([chr(int(encoded_string[i:i+2], 16) ^ r) for i in range(2, len(encoded_string), 2)])
    return email

class BresciaSpider(scrapy.Spider):
    """
    Scrapy Spider for scraping:

    1. Course schedules (PDF parsing)
    2. Directory (faculty/staff information)
    3. Academic calendar events

    Data is saved using save_df utility function.
    """

    name = "brescia"

    # Unique institution identifier used across all datasets
    institution_id = 258417018347218911

     # Course PDF URLs (On-ground and Online)
    course_pdf_urls = [
    "https://www.brescia.edu/wp-content/uploads/Spring-2026-On-Ground-Schedule_-5.pdf",
    "https://www.brescia.edu/wp-content/uploads/Spring-2026-Online-Course-Schedule_-5.pdf"
    ]
    # Headers for PDF requests
    course_headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # Directory and Calendar URLs
    directory_url = "https://www.brescia.edu/directory/admissions/"
    calendar_url = "https://www.brescia.edu/academic-calendar/"

    def __init__(self, *args, **kwargs):
        """
        Initializes data storage containers for scraped data.
        """
        super().__init__(*args, **kwargs)

        # Storage for scraped data
        self.course_rows = []       # Stores all course data
        self.directory_rows = []    # Stores all directory (faculty/staff) data
        self.calendar_rows = []     # Stores all calendar events data

    def start_requests(self):
        """
        Entry point for the spider.
        Scrape mode can be controlled using SCRAPE_MODE setting.
        """
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')  # Determine mode

        if mode == 'course':
            self.parse_course()

        elif mode == 'directory':
            # Only scrape directory data
            self.parse_directory()
        elif mode == 'calendar':
            self.parse_calendar()

        else:
            # Default: scrape course, directory, and calendar
            self.parse_course()
            self.parse_directory()
            self.parse_calendar()


    def parse_course(self):
        """
        Downloads and parses course schedule PDFs.

        Extracts:
        - Course ID
        - Class Number
        - Section
        - Instructor
        - Location
        - Course Dates
        """

        for url in self.course_pdf_urls:
            response = requests.get(url,headers=self.course_headers, timeout=20)
            response.raise_for_status()

            # Regular expressions for extracting patterns
            onground_id = re.compile(r'([A-Z][a-z]{1,3}\s\d{3}[A-Z]?-\d+)')
            online_id = re.compile(r'([A-Z][a-z]{0,3}\s\d{3}[A-Z]?OL-\d+)')
            date_re = re.compile(r'(\d{1,2}/\d{1,2}-\d{1,2}/\d{1,2})')
            room_re = re.compile(r'([A-Z]{1,2}\s?\d{3}|Online|TBA)')

            # Open PDF in memory
            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text(layout=True)
                    if not text:
                        continue
                    
                    # Normalize and split lines
                    lines = text.replace('\xa0', ' ').split('\n')
                    for line in lines:

                        # Detect online vs onground automatically
                        if "OL-" in line:
                            id_match = online_id.search(line)
                            mode = "online"
                        else:
                            id_match = onground_id.search(line)
                            mode = "onground"

                        if not id_match:
                            continue

                        course_id = id_match.group(1)
                        class_number = course_id.split('-')[0]
                        section = course_id.split('-')[-1]

                        # Online handling
                        if mode == "online":
                            date_match = date_re.search(line)
                            course_dates = date_match.group(1) if date_match else ""
                            location = "Online"

                            instructor = "Staff"
                            if date_match:
                                after_date = line[date_match.end():].strip()
                                if " OL" in after_date:
                                    instructor = after_date.split(" OL")[0].strip()

                            name_part = line[id_match.end():].strip()
                            name_match = re.search(r'^([^0-9$]+)', name_part)
                            course_name = name_match.group(1).strip() if name_match else ""

                        # On-ground handling
                        else:
                            room_match = room_re.search(line)
                            location = room_match.group(1) if room_match else ""
                            course_dates = ""

                            mid_part = line[id_match.end():line.find(location) if location else None].strip()

                            instr_match = re.findall(r'([A-Z][a-z]+|[A-Z]\.\s?[A-Z][a-z]+)', mid_part)
                            instructor = instr_match[-1] if instr_match else "Staff"

                            name_match = re.search(r'^([A-Za-z\s&/]+)\s+\d', mid_part)
                            course_name = name_match.group(1).strip() if name_match else ""

                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Course Name": f"{class_number} {course_name}",
                            "Course Description": "",
                            "Class Number": class_number,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": "",
                            "Course Dates": course_dates,
                            "Location": location,
                            "Textbook/Course Materials": "",
                        })


        # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")


    def parse_directory(self):
        """
        Scrapes directory data grouped by departments.

        Extracts:
        - Name
        - Title
        - Email (decoded from Cloudflare)
        - Phone Number
        """
        
        scraper = cloudscraper.create_scraper()
        res = scraper.get(self.directory_url)
        response = Selector(text=res.text)
        
        blocks = response.xpath('//nav[@class="nav screen-only nodrop collapsible current--ancestor current--parent current-menu-ancestor current-menu-parent sidebar  rps-menu-"]/ul//li//@href').getall()
        for block in blocks:
            product_res = scraper.get(block)
            product_response = Selector(text=product_res.text)
            peoples = product_response.xpath('//h1[@class="entry-title p-name has-link"]/a/@href').getall()
            for people in peoples:
                people_res = scraper.get(people)
                people_response = Selector(text=people_res.text)
                name = people_response.xpath('//h1[@class="entry-title p-name"]/text()').get('').strip()
                title1 = people_response.xpath('//dd[@class="rps-contact-item rps-contact-item-value rps-contact-item-type-company-title"]/text()').get('').strip()
                title2 = people_response.xpath('//nav[@class="breadcrumb rps-menu-breadcrumb referring-link"]//a/text()').get('').strip()
                title = title1 + ', ' + title2
                
                phone = ''.join(people_response.xpath('//dd[@class="rps-contact-item rps-contact-item-value rps-contact-item-type-phone"]//text()').getall()).replace('\n','').replace('call','').strip()
                encoded_val = people_response.xpath('//a[@data-cfemail]/@data-cfemail | //span[@data-cfemail]/@data-cfemail').get()
                if encoded_val:
                    email = parse_directory_decode_cloudflare_email(encoded_val)
                else:
                    email = ''
                
                self.directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": people,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                })

        # # Save updated DataFrame
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")


    def parse_calendar(self):
        """
        Extracts academic calendar events from HTML table.

        Extracts:
        - Term Date
        - Term Date Description
        """
        scraper = cloudscraper.create_scraper()
        response = scraper.get(self.calendar_url)
        res = Selector(text=response.text)
        blocks = res.xpath('//table[@id="tablepress-59"]//tbody//tr')
        for block in blocks:
            term_date = ''.join(block.xpath('.//td[1]//text()').getall())
            term_desc = ''.join(block.xpath('.//td[2]//text()').getall())
        
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.directory_url,
                "Term Name": '',
                "Term Date": term_date,
                "Term Date Description": term_desc,
            })

        # Save calendar events
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")