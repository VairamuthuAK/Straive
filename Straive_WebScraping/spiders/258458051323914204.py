import scrapy
import pandas as pd
from ..utils import save_df

class BethelSpider(scrapy.Spider):
    name = "bethel"

    # Unique institution identifier used across all datasets
    institution_id = 258458051323914204

    # Base URLs
    course_url = "https://catalog.bethel.edu/adult-professional-studies/course-descriptions/"
    calendar_url = "https://betheluniversity.smartcatalogiq.com/en/2025-2026/2025-2026-catalog/calendar/"

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
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            for page in range (1,31):
                directory_url = "https://betheluniversity.edu/wp-admin/admin-ajax.php"
                directory_payload = f"value=&slug=&page={page}&generator_id=sptp-1434&action=team_pro_search_member&nonce=cf0be48ec4"
                directory_headers = {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'origin': 'https://betheluniversity.edu',
                    'referer': 'https://betheluniversity.edu/about/campus-directory/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
            
                yield scrapy.Request(
                    directory_url,
                    headers=directory_headers,
                    body=directory_payload,
                    method = "POST",
                    callback=self.parse_directory,
                    dont_filter=True
                )

        elif mode == 'calendar':
            # Only scrape academic calendar
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default: scrape course, directory, and calendar
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            for page in range (1,31):
                directory_url = "https://betheluniversity.edu/wp-admin/admin-ajax.php"
                directory_payload = f"value=&slug=&page={page}&generator_id=sptp-1434&action=team_pro_search_member&nonce=cf0be48ec4"
                directory_headers = {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'origin': 'https://betheluniversity.edu',
                    'referer': 'https://betheluniversity.edu/about/campus-directory/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
            
                yield scrapy.Request(
                    directory_url,
                    headers=directory_headers,
                    body=directory_payload,
                    method = "POST",
                    callback=self.parse_directory,
                    dont_filter=True
                )
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)


    def parse_course(self,response):

        blocks = response.xpath('//div[@class="courseblock"]')
        for block in blocks:
            course_number = block.xpath('./p/strong/text()').get('').strip().replace(" •  "," ").split(" ")
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": "https://catalog.bethel.edu/adult-professional-studies/course-descriptions/",
                "Course Name": block.xpath('./p/strong/text()').get('').strip().replace(" •  "," "),
                "Course Description":" ".join(block.xpath('./p[@class="courseblockdesc"]/text() | ./p[@class="courseblockdesc"]/a/text()').getall()).strip(),
                "Class Number":f"{course_number[0]} {course_number[1]}",
                "Section": "",
                "Instructor": "",
                "Enrollment": "",
                "Course Dates": "",
                "Location": "",
                "Textbook/Course Materials": ""
                    })

        # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")


    def parse_directory(self, response):
        
        blocks = response.xpath('//div[@class="sp-team-pro-item sptp-col-lg-4 sptp-col-md-4 sptp-col-sm-2 sptp-col-xs-1"]')
        for block in blocks:
            name = block.xpath('normalize-space(.//h2)').get('')
            name = name.strip().rstrip(',')
            title = block.xpath('.//h4/text()').get('')
            email = block.xpath('.//a[starts-with(@href, "mailto:")]/span/text()').get('')
            phone = block.xpath('.//a[starts-with(@href, "tel:")]/span/text()').getall()
            phone = ', '.join([p.strip() for p in phone if p.strip()])
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": 'https://betheluniversity.edu/about/campus-directory/',
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })

        # # Save updated DataFrame
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    def parse_calendar(self, response):
        
        term_name = ''
        blocks = response.xpath('//table//tbody//tr')
        for block in blocks:
            header = block.xpath('.//td[@colspan="2"]//strong/text()').get()
            if header:
                term_name = header.strip()
                continue
            tds = block.xpath('./td')
            if len(tds) == 2:
                
                term_desc = tds[0].xpath('normalize-space()').get('')
                term_date = tds[1].xpath('normalize-space()').get('')

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": term_date,
                    "Term Date Description": term_desc,
                })

        # # Save calendar events
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")