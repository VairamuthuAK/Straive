import scrapy
import pandas as pd
from ..utils import save_df

class MariettaSpider(scrapy.Spider):
    """
    Scrapy spider for scraping academic data from
    Marietta College.

    Website: https://www.marietta.edu/

    This spider collects:
    - Course schedule data (multiple term URLs)
    - Faculty directory data
    - Academic calendar data

    SCRAPE_MODE options:
        - "course"
        - "directory"
        - "calendar"
        - "all" (default)
    """

    name = "marietta"

    # Unique institution identifier used across all datasets
    institution_id = 258438723748784099

    # Course schedule URLs (multiple academic terms)
    course_urls = ['https://isweb.marietta.edu/acadsched/course.schedule.25fl.html','https://isweb.marietta.edu/acadsched/course.schedule.25wi.html','https://isweb.marietta.edu/acadsched/course.schedule.26sp.html','https://isweb.marietta.edu/acadsched/course.schedule.25sp.html','https://isweb.marietta.edu/acadsched/course.schedule.25sm.html']
    
    # Academic calendar URL
    calendar_url = "https://www.marietta.edu/academic-calendar#acad_cal"

    def __init__(self, *args, **kwargs):
        """
        Initialize in-memory storage lists for datasets.
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
            # Only scrape course data
            for course_url in self.course_urls:
                yield scrapy.Request(course_url, callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            # Only scrape directory data
            for page in range (0,14):
                directory_url = f'https://www.marietta.edu/directory?name=&az=All&items_per_page=24&sort_by=name&page={page}'
                yield scrapy.Request(directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            # Only scrape academic calendar
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default: scrape course, directory, and calendar
            for course_url in self.course_urls:
                yield scrapy.Request(course_url, callback=self.parse_course, dont_filter=True)

            for page in range (0,14):
                directory_url = f'https://www.marietta.edu/directory?name=&az=All&items_per_page=24&sort_by=name&page={page}'
                yield scrapy.Request(directory_url, callback=self.parse_directory, dont_filter=True)

            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)


    def parse_course(self, response):
        """
        Parse course schedule page.

        Extract:
        - Course title & code
        - Section
        - Instructor
        - Seat availability
        - Location
        - Additional information
        - Textbook link
        """
        blocks = response.xpath('//div[@class="accordion-item course-entry"]')
        for block in blocks:
            # Extract title block (e.g., "BIOL 101 A")
            titles = block.xpath('.//div[@class="col-5"]/b/text()').get('')
            title = titles.split(' ')[0:2]  # Course code (e.g., BIOL 101)
            title = ' '.join(title)
            section = titles.split(' ')[-1] # Section (e.g., A)
            name = block.xpath('.//div[@class="col-5"]//b/following::p[1]/text()').get('')
            instructor = block.xpath('.//div[@class="col-4"]//p[1]/text()').get('')
            instructor = instructor.replace('Acadeum,','Acadeum')
            seats = ''.join(block.xpath('.//div[@class="col-4"]//span[@title="(USED/CAPACITY) [WAITLISTED] not shown if 0"]//text()').getall())
            seats = seats.replace('- (','').replace(')','').strip()
            location = block.xpath('.//div[b[contains(normalize-space(.), "Location")]]//text()').getall()
            location = ','.join(d.strip() for d in location if d.strip() and 'Location' not in d)
            
            desc_list  = block.xpath('.//div[b[contains(normalize-space(.), "Additional Information")]]//text()').getall()
            description = ''.join(d.strip() for d in desc_list if d.strip() and 'Additional Information' not in d)
            
            # Textbook link
            text_book = block.xpath('.//div[@class="col-4"]//a/@href').get('')
            course_name = title + ' ' + name
            
            # Append course data to list
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": course_name,
                "Course Description": '',
                "Class Number": title,
                "Section": section,
                "Instructor": instructor,
                "Enrollment": seats,
                "Course Dates": '',
                "Location": location,
                "Textbook/Course Materials": text_book,
            })

        # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    def parse_directory(self, response):
        """
        Parse directory listing page.

        Extract profile links and follow each one.
        """
        links = response.xpath('//div[@class="views-row"]/article//div[@class="first-name"]/a/@href').getall()
        for link in links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_directory_profile)

    def parse_directory_profile(self, response):
        """
        Extract individual faculty profile details.
        """
        name = response.xpath('//h1[@class="page-title"]/span/text()').get('').strip()
        title1 = ', '.join(response.xpath('//div[@class="field field--name-field-job-title field--type-string field--label-hidden field__items"]/div/text()').getall()).strip()
        title2 = ', '.join(response.xpath('//div[@class="field field--name-field-person-department field--type-entity-reference field--label-above"]//a/text()').getall()).strip()
        title = ', '.join(filter(None, [title1, title2]))
        phone = response.xpath("//a[starts-with(@href, 'tel:')]/text()").get('')
        phone = phone.replace('1-800-331-7896','')
        email = response.xpath("//a[starts-with(@href, 'mailto:')]/text()").get('')
        # Append profile to list
        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

        # Save updated DataFrame
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        Parse academic calendar events.

        Extract:
        - Term name
        - Date
        - Description
        """
        
        terms = response.xpath('//dl[@class="ckeditor-accordion"]/dd/table/tbody/tr')
        for term in terms:
            term_name = term.xpath('ancestor::dd/preceding-sibling::dt[1]/text()').get('').strip()
            term_date = term.xpath('.//th/text()').get('').strip()
            term_description = ' '.join(term.xpath('.//td//text()').getall()).strip()
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": term_name,
                "Term Date": term_date,
                "Term Date Description": term_description,
            })

        # Save calendar events
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")