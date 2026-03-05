import re
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from ..utils import save_df

class TfcSpider(scrapy.Spider):
    """
    Spider for Toccoa Falls College (TFC) to extract course catalogs,
    staff directories, and academic calendars.
    """

    name = "tfc"
    institution_id = 258430098204551120

    # In-memory storage to accumulate items before saving in closed() or at end of parse
    course_rows = []
    course_rows = []
    calendar_rows = []
    directory_rows = []
    
    # Target URLs
    course_url = "https://courses.tfc.edu/course/search.php?search=a&perpage=all"
    
    directory_url = "https://tfc.edu/about/contact/directory/"
    directory_headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/144.0.0.0 Safari/537.36",
                  "Upgrade-Insecure-Requests": "1"
}

    calendar_URL = "https://tfc.edu/wp-content/uploads/2024/10/TFC-Cat-25-04-Academic-Calendar-2024-10-21.pdf"

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
            for letter in letters:
                course_url = f"https://courses.tfc.edu/course/search.php?search={letter}&perpage=all"
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,callback=self.parse_directory,dont_filter=True)
        
        elif mode == 'calendar':
            self.parse_calendar()
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
            for letter in letters:
                course_url = f"https://courses.tfc.edu/course/search.php?search={letter}&perpage=all"
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)
                yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,callback=self.parse_directory,dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
            for letter in letters:
                course_url = f"https://courses.tfc.edu/course/search.php?search={letter}&perpage=all"
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)
            self.parse_calendar()

        elif mode in ['directory_calendar', 'calendar_directory']:
            self.parse_calendar()
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,callback=self.parse_directory,dont_filter=True)
        
        # All three (default)
        else:
            letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
            for letter in letters:
                course_url = f"https://courses.tfc.edu/course/search.php?search={letter}&perpage=all"
                yield scrapy.Request(url=course_url, callback=self.parse_course, dont_filter=True)
                yield scrapy.Request(url=self.directory_url,headers=self.directory_headers,callback=self.parse_directory,dont_filter=True)
                self.parse_calendar()
       

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
        
        blocks = response.xpath("//div[contains(@class, 'coursebox') and contains(@class, 'clearfix')]")
        for block in blocks:
            course_name = block.xpath('.//a[@class="aalink"]//text()').getall()
            course_name = ''.join(course_name)
            instructor = block.xpath('.//span[contains(text(),"Professor: ")]/following::a[1]//text()').get('')
            class_number = course_name.split('-')[0]
            sectionee = ''
            try:
                sectionee = course_name.split('-')[1]
            except:
                pass
            self.course_rows.append(
                {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": course_name,
                "Course Description": '',
                "Class Number": class_number,
                "Section": sectionee,
                "Instructor": instructor,
                "Enrollment": '',
                "Course Dates": '',
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
        
        blocks = response.xpath('//table[@class="tfc-table"]//tr')
        for block in blocks:
            name = block.xpath('./td[1]/text()').get('')
            title = block.xpath('./td[2]/text()').get('')
            phone = ''
            email = block.xpath('./td[3]/a/text()').get('')
            if name != '':
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

    def parse_calendar(self):

        response = requests.get(self.calendar_URL)
        data = []
        
        # Regex to capture the day/range at the start of a line and the description
        date_pattern = re.compile(r'^(\d{1,2}(?:-\d{1,2})?)\s+(.*)')
        
        months = ["AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER", 
                "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY"]

        with pdfplumber.open(BytesIO(response.content)) as pdf:
            current_term = "FALL SEMESTER 2025"
            current_month = ""

            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                for line in text.split('\n'):
                    line = line.strip()
                    
                    # Update term context
                    if "FALL SEMESTER" in line.upper():
                        current_term = "FALL SEMESTER 2025"
                    elif "SPRING SEMESTER" in line.upper():
                        current_term = "SPRING SEMESTER 2026"

                    # Update month context
                    if line.upper() in months:
                        current_month = line.capitalize()
                        continue

                    # Match date and description
                    match = date_pattern.match(line)
                    if match and current_month:
                        day_part = match.group(1)
                        description = match.group(2)
                        
                        # Create the date string (e.g., Aug-01)
                        formatted_date = f"{current_month[:3]}-{day_part.zfill(2)}"
                        
                        data.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_URL,
                            "Term Name": current_term,
                            "Term Date": formatted_date,
                            "Term Date Description": description
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




 