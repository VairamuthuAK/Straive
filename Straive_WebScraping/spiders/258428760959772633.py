import re
import scrapy
import pandas as pd
from ..utils import save_df

class SienaheightsSpider(scrapy.Spider):
    """
    Scrapy spider to extract Course Catalog, Faculty/Staff Directory,
    and Academic Calendar data from Siena Heights University website.

    The spider supports three scrape modes:
        - course
        - directory
        - calendar
        - all (default)

    Data is stored in memory and saved using save_df utility.
    """

    name = "sienaheights"

    # Unique institution identifier used across all datasets
    institution_id = 258428760959772633

    # Base URLs
    course_url = ""
    directory_url = "https://www.sienaheights.edu/who-we-are/faculty-staff/"
    calendar_url = "https://www.sienaheights.edu/who-we-are/resources/academic-calendar/"

    def __init__(self, *args, **kwargs):
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
            # Loop through paginated course catalog pages
            for i in range(1, 13):  
                course_url = f'https://catalog.sienaheights.edu/content.php?catoid=10&catoid=10&navoid=634&filter%5Bitem_type%5D=3&filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D={i}'
                course_headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
                yield scrapy.Request(course_url,headers=course_headers, callback=self.parse_course_details, dont_filter=True)

        elif mode == 'directory':
            # Only scrape directory data
            headers = {
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'x-requested-with': 'XMLHttpRequest'
            }
            yield scrapy.Request(
                self.directory_url,
                headers=headers,
                callback=self.parse_directory,
                dont_filter=True
            )

        elif mode == 'calendar':
            # Only scrape academic calendar
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default: scrape course, directory, and calendar
            for i in range(1, 13):  
                course_url = f'https://catalog.sienaheights.edu/content.php?catoid=10&catoid=10&navoid=634&filter%5Bitem_type%5D=3&filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D={i}'
                course_headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
                yield scrapy.Request(course_url,headers=course_headers, callback=self.parse_course_details, dont_filter=True)

            headers = {
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'x-requested-with': 'XMLHttpRequest'
            }
            yield scrapy.Request(
                self.directory_url,
                headers=headers,
                callback=self.parse_directory,
                dont_filter=True
            )
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    def parse_course(self, response):
        """
        Extract course detail page links and follow them.
        """
        # Get all location options from dropdown
        blocks = response.xpath('//a[contains(@href,"preview_course_nopop.php")]/@href').getall()
        for block in blocks:
            link = response.urljoin(block)
            yield scrapy.Request(link, callback=self.parse_course_details)

    def parse_course_details(self, response):
        """
        Extract individual course details including:
            - Course Name
            - Class Number
            - Description
        """
        course_name = response.xpath('//h1/text()').get('').strip()
        class_number = " ".join(course_name.split(' ')[0:2]).strip()
        raw = response.xpath('//h1/following::text()[normalize-space() and ancestor::td = //h1/ancestor::td[1]]').getall()
         # Clean and normalize description text
        clean = [x.strip() for x in raw]
        description = ' '.join(clean).strip()
        # Remove unwanted footer text
        description = description.replace('Back to Top | Print-Friendly Page (opens a new window) Facebook this Page (opens a new window) Tweet this Page (opens a new window) Add to Favorites (opens a new window)','').strip()
        description = re.sub(r'\s+', ' ', description).strip()
    
        # Append course data to list
        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Course Name": course_name,
            "Course Description": description,
            "Class Number": class_number,
            "Section": '',
            "Instructor": '',
            "Enrollment": '',
            "Course Dates": '',
            "Location": '',
            "Textbook/Course Materials": '',
        })

        # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    def parse_directory(self, response):
        """
        Extract faculty/staff profile links from directory table.
        """
        
        # Get all department options from dropdown
        blocks = response.xpath('//table[@id="myTable"]//tr//th[1]//a/@href')
        for block in blocks:
            request_url = response.urljoin(block.get())
            yield scrapy.Request(request_url, callback=self.parse_directory_details)

    def parse_directory_details(self, response):
        """
        Extract faculty/staff details:
            - Name
            - Title
            - Email
            - Phone Number
        """
        name = response.xpath('//span[@class="staff-text staff-texti name_staff"]/text()').get('').replace('\xa0',' ').strip()
        try:
            title = re.search(r'getElementsByClassName\("entry-title"\)\[0\]\.innerHTML="([^"]+)"', response.text).group(1)
            title = title.replace('\xa0',' ').replace('\\','').strip() 
        except:
            title = ''
        phone = response.xpath('//span[@class="staff-text staff-texti staff_phone"]/text()').get('').replace('\xa0',' ').strip()
        email = response.xpath('//span[@class="staff-text staff-texti staff_email"]/text()').get('').replace('\xa0',' ').strip()
        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

        # # Save updated DataFrame
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")


    def parse_calendar(self, response):
        """
        Extract academic calendar events.

        Each event contains:
            - Term Name
            - Term Date
            - Term Date Description
        """
        blocks = response.xpath('//div[@class="elementor-element elementor-element-e37d6da elementor-widget elementor-widget-text-editor"]//ul//li')
        for block in blocks:
            term_name = 'Winter Semester - 2026'
            dates = block.xpath('.//span/text()|.//span/a/text()').getall()
            dates = ''.join(dates).replace('\xa0',' ').strip() 
            dates = dates.replace('—', '–')
            term_date, *term_desc = dates.split('–')
            term_date = term_date.strip()
            term_desc = ' '.join(term_desc).strip()
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": term_name,
                "Term Date": term_date,
                "Term Date Description": term_desc,
            })

        # # Save calendar events
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")