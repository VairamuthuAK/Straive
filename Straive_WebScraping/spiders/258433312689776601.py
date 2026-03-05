import re
import scrapy
import pandas as pd
from ..utils import save_df
from urllib.parse import quote_plus


class AndromedaSpider(scrapy.Spider):
    name = "andromeda"

    # Unique institution identifier used across all datasets
    institution_id = 258433312689776601

    # Base URLs
    course_url = "https://andromeda.ccv.vsc.edu/Learn/Grid/SiteList.cfm"
    directory_url = "https://andromeda.ccv.vsc.edu/Learn/Directories/"
    calendar_url = "https://ccv.edu/academics/academic-calendar/"

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
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)


    def parse_course(self, response):
        """
        Extracts all available campus/location values.
        Each location has its own course grid.
        """
        # Get all location options from dropdown
        options = response.xpath(
            '//form[@action="SectionGrid.cfm"]//option/@value'
        ).getall()

        # Loop through each campus/location to scrape course schedules
        for val in options:
            url = (
                "https://andromeda.ccv.vsc.edu/Learn/Grid/"
                f"VONLGrid.cfm?Location={val}&grid=Summer"
            )
            yield scrapy.Request(url, callback=self.parse_course_list)

    def parse_course_list(self, response):
        """
        Extracts individual course section links from the grid page.
        """
        # Get all links to individual course sections
        links = response.xpath(
            '//table[@align="center"][2]//tr/td/a/@href'
        ).getall()

        # Loop through each section link and scrape details
        for link in links:
            url = f"https://andromeda.ccv.vsc.edu/Learn/Grid/{link}"
            yield scrapy.Request(url, callback=self.parse_course_details)

    def parse_course_details(self, response):
        """
        Extracts detailed course information from section detail page.
        """
        title_text = response.xpath(
            '//h2[contains(text(),"Summer 2026 ")]/text()'
        ).get()

        if not title_text:
            return  # Skip if title not found

        # Extract course title and number
        title_text = title_text.split('| ')[1].strip()
        parts = title_text.split(' - ', 1)

        class_and_section = parts[0].strip()
        course_name = parts[1].strip() if len(parts) == 2 else ''

        if '-' in class_and_section:
            class_num, section = class_and_section.rsplit('-', 1)
        else:
            class_num, section = class_and_section, ''

        title = f"{class_num} - {course_name}"

        # Extract location, instructor, description, enrollment, dates, textbooks
        location = response.xpath(
            '//strong[normalize-space(text())="Location:"]/following-sibling::text()[1]'
        ).get('').strip()

        instructor = response.xpath(
            '//h2[contains(text(),"Faculty")]/following::strong/a/text()'
        ).get('').strip()

        description = response.xpath(
            '//h2[normalize-space()="Course Description"]/following-sibling::p[1]/text()'
        ).get('').strip()

        seats = response.xpath(
            '//strong[normalize-space(text())="Open Seats:"]/following-sibling::text()[1]'
        ).get('')
        enrollment = seats.split(' ')[0] if seats else ''

        date = response.xpath(
            '//strong[normalize-space()="Semester Dates:"]/following-sibling::text()[1]'
        ).get('').strip()

        textbook = response.xpath(
            "//a[normalize-space(.)='Link to Textbooks/Resources Information']/@href"
        ).get('')

        if not textbook:
            textbook = response.xpath(
                "//a[normalize-space(.)='Link to Textbooks']/@href"
            ).get('')

        # Append course data to list
        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Course Name": re.sub(r'\s+', ' ', title),
            "Course Description": re.sub(r'\s+', ' ', description),
            "Class Number": class_num.strip(),
            "Section": section.strip(),
            "Instructor": instructor,
            "Enrollment": enrollment,
            "Course Dates": date,
            "Location": location,
            "Textbook/Course Materials": textbook,
        })

        # Save updated DataFrame to CSV or database
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")


    def parse_directory(self, response):
        """
        Extracts department list from directory page.
        Directory data is grouped by department.
        """
        # Get all department options from dropdown
        departments = response.xpath(
            '//select[@name="dept"]//option/@value'
        ).getall()

        # Skip first placeholder option and loop through all departments
        for dept in departments[1:]:
            url = (
                "https://andromeda.ccv.vsc.edu/Learn/Directories/"
                f"DeptRes.cfm?dept={quote_plus(dept)}"
            )
            yield scrapy.Request(url, callback=self.parse_directory_details)

    def parse_directory_details(self, response):
        """
        Extracts profile links for faculty/staff within a department.
        """
        for row in response.xpath('//table[contains(@class,"table")]//tr'):
            user_id = row.xpath('.//input[@name="USERID"]/@value').get('')
            location = row.xpath('.//input[@name="location"]/@value').get('')

            if user_id and location:
                # Build URL for individual profile
                url = (
                    "https://andromeda.ccv.vsc.edu/Learn/Directories/"
                    f"Detail.cfm?USERID={user_id}&location={location}"
                )
                yield scrapy.Request(url, callback=self.parse_directory_profile)

    def parse_directory_profile(self, response):
        """
        Extracts individual faculty/staff profile details.
        """
        # Extract profile fields
        name = response.xpath('//h3/text()').get('')
        title = response.xpath('//h2/text()').get('')
        email = response.xpath(
            '//p[contains(text(),"Email")]/text()'
        ).re_first(r'Email:\s*(.*)')

        phone = response.xpath(
            '//p[contains(.,"Office Phone") or contains(.,"Academic Center")]//text()'
        ).re_first(r'([0-9\-]+)')

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
        Extracts academic calendar events by term.
        """
        terms = response.xpath('//h2[contains(@class,"wp-block-heading")]')
        current_year = None

        for term in terms:
            term_name = term.xpath('normalize-space(text())').get()
            if not term_name:
                continue

            # Update current year if term contains a year
            if any(char.isdigit() for char in term_name):
                current_year = term_name.split()[-1]

            # Get events under this term
            events = term.xpath('following-sibling::ul[1]/li')

            for event in events:
                title = event.xpath(
                    './/h4[@class="entry-title summary"]/a/text()'
                ).get()
                date = event.xpath(
                    './/span[@class="tribe-event-date-start"]/text()'
                ).get()

                if title and date and current_year:
                    # Append calendar event
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": "https://ccv.edu/academic-calendar/",
                        "Term Name": term_name,
                        "Term Date": f"{date} {current_year}",
                        "Term Date Description": title,
                    })

        # Save calendar events
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")
