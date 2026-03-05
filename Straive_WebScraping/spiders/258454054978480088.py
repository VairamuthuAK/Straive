import re
import scrapy
import pandas as pd
from parsel import Selector
from inline_requests import inline_requests
from ..utils import save_df
from datetime import datetime
from urllib.parse import urlparse, parse_qs


class CrcSpider(scrapy.Spider):
    
    # Spider name (used when running scrapy crawl crc)
    name = "crc"
    
    # Unique institution ID for Cengage
    institution_id = 258454054978480088
    
    # List to store all course rows
    courseData = []
    directory_rows = []
    
    # URLs for course, directory, and calendar pages
    course_url = "https://crc.losrios.edu/academics/search-class-schedules?crcFilter=true&openFilter=true&waitlistFilter=true&strm=1263,Spring%202026&link=true"
    directory_url = "https://crc.losrios.edu/about-us/contact-us/employee-directory?search=&college=CRC&sort=first&searchLocation=CRC&cmd=undefined&offset=0&type=undefined&dept=&link=true"
    calendar_url = "https://crc.losrios.edu/admissions/academic-calendar-and-deadlines"
    
    # Default HTTP headers used for normal page requests
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

    def start_requests(self):

        # Read scrape mode from settings (course / directory / calendar / combinations)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode Execution ----
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

    @inline_requests
    def parse_course(self, response):
        """
        Scrapes course data using AJAX pagination and modal API.
        inline_requests is used to wait for dependent API responses.

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
        
        offset = 0
        
        # Regex to extract parameters from onclick JS function
        pattern = re.compile(r"getModal\('([^']+)','([^']+)','([^']+)','([^']+)'\)")
        
        # Loop until no more courses are found
        while True:
            
            # Build paginated AJAX URL
            fullUrl = f"https://hub.losrios.edu/classSearch/getCourses.php?crcFilter=true&openFilter=true&waitlistFilter=true&strm=1263,Spring%202026&link=true&offset={offset}&first=2"
            
            # Request course list
            courseRes = yield scrapy.Request(url=fullUrl, headers=self.headers)
            
            # Extract onclick JS calls
            allLinks = courseRes.xpath('//div[@class="links"]//a/@onclick').getall()
            
            # Stop loop if no more courses
            if not allLinks:
                break
            
            # loop all links for course
            for link in allLinks:
                match = pattern.search(link)
                if not match:
                    continue
                
                # Extract modal parameters
                course_id, class_section, college, modal_strm = match.groups()
                
                # Build modal API URL
                modal_url = (
                    "https://hub.losrios.edu/classSearch/getModal.php"
                    f"?ClassSection={class_section}"
                    f"&CourseId={course_id}"
                    f"&college={college}"
                    f"&modalStrm={modal_strm}"
                )
                
                # Request modal data
                modalRes = yield scrapy.Request(url=modal_url, headers=self.headers)
                modalSel = Selector(text=modalRes.text)
                
                # Extract course title
                course_title = modalSel.xpath("normalize-space(//div[@id='print-area']//h2)").get()
                
                # Extract course description
                description = modalSel.xpath(
                    "normalize-space(//div[@id='print-area']/p[1])"
                ).get()
                
                # Each course can have multiple sections
                sections = modalSel.xpath("//div[@class='section-details']")
                
                # loop all section
                for section in sections:

                    # CLASS NUMBER → "LEC 13237" / "LAB 13238"
                    class_number = section.xpath(
                        "normalize-space(.//span[text()='Class Number:']/following-sibling::text())"
                    ).get()

                    # Extract raw date text
                    raw_dates = section.xpath(
                        "normalize-space(.//span[text()='Term:']/following-sibling::text())"
                    ).get()
                    
                    # Convert raw date to readable range
                    courseDates = ""
                    if raw_dates:
                        
                        m = re.search(
                                r"([A-Za-z]+\s+\d{1,2})\s+to\s+([A-Za-z]+\s+\d{1,2})",
                                raw_dates)
                        
                        if m:
                                courseDates = f"{m.group(1)} - {m.group(2)}"
                    
                    # Extract instructor names
                    instructor = " ".join(
                        t.strip()
                        for t in section.xpath(
                            ".//li[span[@class='label' and normalize-space()='Instructors:']]//text()"
                        ).getall()
                        if t.strip() and t.strip() != "Instructors:"
                    )
                    instructor = re.sub(r"\s*,\s*", ", ", instructor )
                    
                    # Extract textbook link if available
                    textbook_url = section.xpath(
                        ".//span[text()='Textbook:']/following-sibling::span/a/@href"
                    ).get(default="")

                    # Extract enrollment text
                    raw_enroll_nodes = section.xpath(
                        ".//li[span[normalize-space()='Enrollment Status:']]//text()"
                    ).getall()

                    enroll_text = " ".join(
                        t.strip()
                        for t in raw_enroll_nodes
                        if t.strip() and t.strip() != "Enrollment Status:"
                    )
                    
                    # Calculate enrollment numbers
                    enrollment = ""
                    m = re.search(r"(\d+)\s+open.*?out of\s+(\d+)", enroll_text, re.I)
                    if m:
                        open_seats = int(m.group(1))
                        total_seats = int(m.group(2))
                        enrollment = f"{total_seats - open_seats}/{total_seats}"
                    elif "waitlist" in enroll_text.lower():
                        enrollment = ""

                    # Append final course row
                    self.courseData.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": re.sub(r"\s+", " ",course_title),
                        "Course Description": re.sub(r"\s+", " ",description),
                        "Class Number": class_number,
                        "Section": "",
                        "Instructor": re.sub(r"\s+", " ",instructor),
                        "Enrollment": enrollment,
                        "Course Dates": re.sub(r"\s+", " ",courseDates),
                        "Location": "",
                        "Textbook/Course Materials": textbook_url
                    })
            
            # Move to next page
            offset += 1

        df = pd.DataFrame(self.courseData )
        save_df(df, self.institution_id, "course")
    
    @inline_requests
    def parse_directory(self, response):
        """
        Scrapes employee directory using paginated AJAX calls.
        
        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """
        
        # List to store all employee records
        directory_data = []
        
        # Base AJAX API URL for directory data
        base_url = "https://hub.losrios.edu/directory/profiles/directoryResults.php"
        
        # Offset is used for pagination (page number)
        offset = 0
        
        # Loop until no more directory pages are available
        while True:
            
            # Build AJAX URL with pagination parameters
            ajax_url = (
                f"{base_url}"
                f"?college=CRC"
                f"&sort=first"
                f"&searchLocation=CRC"
                f"&cmd=undefined"
                f"&offset={offset}"
                f"&type=undefined"
                f"&dept="
            )
            
            # Send request to get one page of directory results
            directRes = yield scrapy.Request(url=ajax_url, headers=self.headers)
            
            # Extract all staff profile links from the page
            rows = directRes.xpath("//li//a/@href").getall()
            
            # If no links found, stop pagination
            if not rows:
                break
            
            # Loop through each staff profile link
            for row in rows:
                
                # Extract query part from URL
                query = urlparse(row).query
                
                # Get employee ID from query string
                id_value = parse_qs(query)['id'][0]
                
                # Build public profile page URL
                directUrl = ("https://crc.losrios.edu/about-us/contact-us/employee-directory"+ row)
                
                # Build backend employee profile API URL
                staffUrl = f"https://hub.losrios.edu/directory/profiles/employeeProfile.php?wid={id_value}&college=crc.losrios.edu"
                
                # Request detailed employee profile
                staffRes = yield scrapy.Request(url=staffUrl, headers=self.headers)
                
                # Convert response text into Selector for XPath
                staffSel = Selector(text=staffRes.text)
                
                # Extract employee name
                name = staffSel.xpath("normalize-space(//h1/text())").get()
                
                # Extract all job roles for this employee
                job_roles = staffSel.xpath(
                    "//div[contains(@class,'accordion-btn') and contains(@class,'job-role')]"
                )
                
                # If multiple job roles exist
                if job_roles:
                    
                    for job in job_roles:
                        
                        # Extract job title text
                        title = " ".join(
                            t.strip()
                            for t in job.xpath(".//button//div/text()").getall()
                            if t.strip()
                        )

                        # Keep only CRC-related roles
                        if "crc" not in title.lower():
                            continue
                        
                        # Extract department name
                        department = " ".join(
                            t.strip()
                            for t in job.xpath(".//p[contains(@class,'margin-third')]/text()").getall()
                            if t.strip()
                        )
                        
                        # Extract email address
                        email = job.xpath(
                            ".//li[contains(@class,'bull-email')]/a/text()"
                        ).get(default="").strip()
                        
                        # Extract phone number
                        phone = job.xpath(
                            ".//li[contains(@class,'bull-phone')]/text()"
                        ).get()
                        phone = phone.strip() if phone else ""
                        
                        # Save one employee record
                        directory_data.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": directUrl,
                            "Name": name,
                            "Title": f"{title}, {department}" if department else title,
                            "Email": email,
                            "Phone Number": phone if re.search(r"\d", phone) else ""
                        })
                
                # If no structured job roles found
                else:
                    title = " ".join(
                        t.strip()
                        for t in staffSel.xpath(
                            "//div[contains(@class,'job-role')]"
                            "//*[self::div or self::p][contains(@class,'margin-third')]/text()"
                        ).getall()
                        if t.strip()
                    )
                    
                    # Extract email
                    email = staffSel.xpath(
                        "//li[contains(@class,'bull-email')]/a/text()"
                    ).get(default="").strip()

                    phone = staffSel.xpath(
                        "//li[contains(@class,'bull-phone')]/text()"
                    ).get(default="").strip()
                    
                    # If name is missing, fetch it from public page
                    if not name:
                        pageres = yield scrapy.Request(url=directUrl, headers=self.headers) 
                        page_name = pageres.xpath("//span[@id='current-page-name']/text()").get(default="").strip()
                        name = page_name
                    
                    directory_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": directUrl,
                        "Name": name,
                        "Title": title.strip() if title else "",
                        "Email": email.strip() if email else "",
                        "Phone Number": phone if re.search(r"\d", phone) else ""
                    })
            
            # Move to next page of directory results
            offset += 1
        
        df = pd.DataFrame(directory_data)
        save_df(df, self.institution_id, "campus")
        
    @inline_requests
    def parse_calendar(self, response):
        """
        This function collects academic calendar dates and descriptions

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        
        # List to store calendar records
        calendar_data = []
        
        # Get all term page links from calendar page
        termsLinks = response.xpath('.//main//div[@class="listing"]//a/@href').getall()
        
        # Loop through each term link
        for termLink in termsLinks:
            
            # Build full URL for term page
            fullUrl = response.urljoin(termLink)
            
            # Request term calendar page
            termRes = yield scrapy.Request(url=fullUrl, headers=self.headers)
            
            # Extract term name (example: Spring 2026)
            termName = termRes.xpath('//*[@id="current-page-name"]/text()').get()
            
            # Get all calendar table rows
            rows = termRes.xpath("//table/tbody/tr")
            
            # Loop through each calendar row
            for row in rows:
                
                # Extract date column
                termDate = row.xpath("./th/text()").get(default="").strip()
                
                # Extract description column
                termDesc = termDesc = " ".join(row.xpath("./td//text()").getall()).strip()
                
                calendar_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": termRes.url,
                        "Term Name": termName,
                        "Term Date": termDate,
                        "Term Date Description": termDesc
                    })

        df = pd.DataFrame(calendar_data)
        save_df(df, self.institution_id, "calendar")
