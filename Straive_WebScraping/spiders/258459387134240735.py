import re
import scrapy
import pandas as pd
import cloudscraper
from ..utils import save_df
from parsel import Selector


def parse_course_clean_text(text):
    """
    Clean course description text.

    - Removes extra whitespace
    - Normalizes multiple spaces into a single space
    - Strips leading and trailing spaces
    """

    if not text:
        return ''
    return re.sub(r'\s+', ' ', text).strip()

class BjuSpider(scrapy.Spider):
    """
    Scrapy spider for scraping academic data from 
    Bob Jones University (BJU).

    Website: https://www.bju.edu/

    This spider collects:
    - Course data
    - Faculty directory data
    - Academic calendar data

    Scrape mode can be controlled using SCRAPE_MODE setting:
        - "course"
        - "directory"
        - "calendar"
        - "all" (default)
    """

    name = "bju"

    # Unique institution identifier used across all datasets
    institution_id = 258459387134240735

    # Base URLs
    course_url = "https://www.bju.edu/academics/courses/"
    directory_url = "https://www.bju.edu/academics/faculty/"
    calendar_url = "https://www.bju.edu/events/calendar/year-overview.php"

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
            # Only scrape course data
            self.parse_course()

        elif mode == 'directory':
            # Only scrape directory data
            self.parse_directory()

        elif mode == 'calendar':
            # Only scrape academic calendar
            self.parse_calendar()

        else:
            # Default: scrape course, directory, and calendar
            self.parse_course()
            self.parse_directory()
            self.parse_calendar()


    def parse_course(self):
        """
        Scrape course information from the BJU course catalog.

        Steps:
        1. Fetch all subject links.
        2. Call AJAX fetch endpoint for each subject.
        3. Extract course name, description, and class number.
        4. Store results into course_rows list.
        """

        scraper = cloudscraper.create_scraper() 
        res = scraper.get(self.course_url)
        response = Selector(text=res.text) 

        # Extract subject links
        blocks = response.xpath('//div[@id="all-subjects"]//ul//li//a/@href').getall()
        for block in blocks:
            # Clean and construct product URL
            product_url = block.replace('.','')
            product_url = 'https://www.bju.edu/academics/courses'+ product_url

            # Extract subject parameter for AJAX fetch
            link = block.split('!')[-1]
            link = f'https://www.bju.edu/academics/courses/fetch.php?nocache=1770814757491&subject={link}'

            # Fetch subject-specific courses
            course_res = scraper.get(link)
            course_response = Selector(text=course_res.text)
            course_blocks = course_response.xpath('//div[@class="row course"]') 
            for course_block in course_blocks:
                course_name = course_block.xpath('.//h2//text()').get('').strip()
                description = course_block.xpath('.//div[@class="half"][1]/p[1]/text()').get('').strip()
                description = parse_course_clean_text(description)
                class_number = course_name.split(":")[0]
            
                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": product_url,
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

        # # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    def parse_directory(self):
        """
        Scrape faculty directory data.

        Steps:
        1. Extract profile links.
        2. Visit each faculty profile page.
        3. Extract name, title, and email.
        4. Store results into directory_rows list.
        """

        scraper = cloudscraper.create_scraper() 
        res = scraper.get(self.directory_url)
        response = Selector(text=res.text) 

        # Extract faculty profile links
        person_links = response.xpath('//div[@id="switchname"]//ul//li/a/@href').getall()
        for index, person_link in enumerate(person_links):
            person_url = 'https://www.bju.edu/academics/faculty/' + person_link
            person_res = scraper.get(person_url)
            person_response = Selector(text=person_res.text) 
            name = person_response.xpath('//h1/text()').get('').strip()
            title = ' '.join(person_response.xpath('//div[@class="fac_profile fac_full"]//figcaption//em//text()').getall())

            # Email is partially hidden (username only)
            email_username = person_response.xpath('//a[@class="mailto"]/@data-user').get()
            if email_username:
                email = f"{email_username}@bju.edu"
            else:
                email = ""
        
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": person_url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": '',
            })

        # # Save updated DataFrame
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    def parse_calendar(self):
        """
        Scrape academic calendar information.

        Logic:
        - Table contains header rows with term names.
        - Subsequent rows contain event description and date.
        - If term name row appears, update current term_name.
        - Associate following rows with the last known term_name.
        """
        
        scraper = cloudscraper.create_scraper() 
        res = scraper.get(self.calendar_url)
        response = Selector(text=res.text) 
        blocks = response.xpath('//div[@class="switchable"]//tr')
        term_name = ''
        for block in blocks:
            term_part1 = block.xpath('./th[1]//text()').get('').strip()
            term_part2 = block.xpath('./th[2]//text()').get('').strip()
            if term_part1 and term_part2:
                term_name = f"{term_part1.strip()} {term_part2.strip()}"
                continue   # skip header row
            

            term_desc = block.xpath('.//td[1]//text()').get('')
            term_date = ''.join(block.xpath('.//td[2]//text()').getall())

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": term_name,
                "Term Date": term_date,
                "Term Date Description": term_desc,
            })

        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")