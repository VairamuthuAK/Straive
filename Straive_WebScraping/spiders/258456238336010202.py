import re
import scrapy
import pandas as pd
import requests
from parsel import Selector
from inline_requests import inline_requests
from ..utils import save_df


class TaylorSpider(scrapy.Spider):
    
    name = "taylor"
    institution_id = 258456238336010202
    
    # List to store all scraped course records
    courseData = []
    
    
    # urls
    BASE_URL = "https://expert.taylors.edu.my"
    SEARCH_URL = "https://expert.taylors.edu.my/vw_search_expert.php"
    course_url = "https://www.taylor.edu/about/offices/registrar/class-schedules"
    directory_url = "https://university.taylors.edu.my/en/get-in-touch/contact-information/staff-directory.html"
    calendar_url = "https://www.taylor.edu/about/offices/registrar/academic-calendar"
    eventUrl = "https://25livepub.collegenet.com/s.aspx?calendar=taylor-academic-calendar&widget=main&spudformat=xhr"
    
    # Default HTTP headers used for normal page requests
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
    
    proxies = {
        'http': 'Enter your proxy here',
        'https': 'Enter your proxy here',
    }

    def start_requests(self):

        # Read scrape mode from settings (course / directory / calendar / combinations)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode Execution ----
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        elif mode == 'directory':
            # yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_directory()

        elif mode == 'calendar':
            yield scrapy.Request(url=self.eventUrl, callback=self.parse_calendar)

        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            # yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_directory()

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.eventUrl, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.eventUrl, callback=self.parse_calendar)
            # yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_directory()

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            # yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            self.parse_directory()
            yield scrapy.Request(url=self.eventUrl, callback=self.parse_calendar)

    @inline_requests
    def parse_course(self, response):
        """
        This function collects course details from term → department → course table

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
        
        # Get all term page links from the main page
        termLinks = response.xpath('//*[@id="wysiwyg-1"]/div/div/div/ul/li/a/@href').getall()
        
        # Loop through each term link
        for termLink in termLinks:
            
            # Open term page
            termRes = yield scrapy.Request(url=termLink, headers=self.headers)
            
            # Get the URL of the right-side frame (actual course data is inside this)
            right_frame = termRes.xpath("//frame[@name='Right']/@src").get()

            # If frame is missing, stop processing
            if not right_frame:
                return
            
            # Build full URL for the right frame
            full_right_url = termRes.urljoin(right_frame)
            
            # Open the right frame page
            depRes = yield scrapy.Request(url=full_right_url, headers=self.headers)
            
            # Extract all department links
            departments = depRes.xpath("//a/@href").getall()
            
            # Loop through each department
            for dept in departments:
                
                # Build full department URL
                full_dept_url = depRes.urljoin(dept)
                
                # Open department course page
                courseRes = yield scrapy.Request(url=full_dept_url, headers=self.headers)
                
                # Extract header text that contains subject code
                header_text = courseRes.xpath("//h3[contains(normalize-space(.), 'Courses for')]//text()").getall()
                header_text = " ".join(
                    t.strip()
                    for t in courseRes.xpath(
                        "//h3[contains(normalize-space(.), 'Courses for')]//text()"
                    ).getall()
                    if t.strip()
                )
                
                # Extract subject code from header (inside brackets)
                subject_code = re.search(r"\((.*?)\)", header_text).group(1)
                
                # Get all course rows from the table
                rows = courseRes.xpath("//table/tbody/tr")
                
                # Loop through each course row
                for row in rows:
                    
                    # Extract CRN (class number)
                    crn = row.xpath("./td[1]//text()").get()
                    
                    # Extract section number
                    section = row.xpath("./td[2]/text()").get()
                    
                    # Extract course number
                    course = row.xpath("./td[3]/text()").get()
                    
                    # Extract course name
                    course_name = row.xpath("./td[4]/text()").get()
                    
                    # Extract instructor name
                    instructor = row.xpath("./td[5]/text()").get()
                    
                    # Extract enrolled count
                    sec_size = row.xpath("./td[6]/text()").get()
                    
                    # Extract capacity count
                    sec_cap = row.xpath("./td[7]/text()").get()
                    
                    # Extract course date text
                    course_dates = row.xpath("./td[8]/text()").get(default="").strip()
                    course_dates = re.sub(r"\s+", " - ", course_dates, count=1)
                    
                    # Build full course title
                    course_title = f"{subject_code} {course} {course_name}"
                    
                    # Build enrollment format (enrolled/total)
                    enrollment = f"{sec_size}/{sec_cap}"
                    
                    self.courseData.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": courseRes.url,
                        "Course Name": re.sub(r"\s+", " ", course_title) if course_title else "",
                        "Course Description": "",
                        "Class Number": crn,
                        "Section": section,
                        "Instructor": re.sub(r"\s+", " ", instructor) if instructor else "",
                        "Enrollment": enrollment,
                        "Course Dates": course_dates,
                        "Location": "",
                        "Textbook/Course Materials": ""
                    })

        df = pd.DataFrame(self.courseData )
        save_df(df, self.institution_id, "course")
        
    
    def parse_directory(self):
        
        # List to store all directory records
        directory_rows =[]
        
        # Send request to directory page and wait for response
        # (yield is used to get the page content)
        response = yield scrapy.Request(url=self.directory_url, headers=self.headers, dont_filter=True)
        
        # Select all profile cards that have a description attribute
        cards = response.xpath("//general-card[@description]")
        
        # Loop through each profile card
        for card in cards:
            
            # Extract staff name from card title attribute
            name = card.xpath("@card-title").get(default="").strip()
            
            # Extract role/designation from description attribute
            role = card.xpath("@description").get(default="").strip()
            
            # Skip record if role is missing
            if not role:
                continue
            
            # Save one staff record
            directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title":role,
                "Email": "",
                "Phone Number": "",
                })
            
        # Select additional text-based directory blocks
        blocks = response.xpath("//div[contains(@class,'text-component')]")
        
        # Loop through each text block
        for block in blocks:
            
            # Extract all bold names inside paragraph tags
            names = block.xpath(".//p/b/text()").getall()
            
            # Loop through each extracted name
            for name in names:
                
                # Get the role text that appears in the next paragraph after the name
                role = block.xpath(
                    ".//p[b[text()=$n]]/following-sibling::p[1]/i/text()",
                    n=name
                ).get(default="").strip()

                # Clean extra spaces from name
                name = name.strip()

                # Skip if role is missing
                if not role:
                    continue
                
                # Save one staff record
                directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": name,
                    "Title":role,
                    "Email": "",
                    "Phone Number": "",
                    })
        
        # Send GET request to base page to load faculty dropdown
        facultiesRes = requests.get(self.BASE_URL, proxies=self.proxies, verify=False)
        
        # Convert HTML response to Selector for XPath usage
        facultiesSel = Selector(text=facultiesRes.text)
        
        # Get all faculty options except empty values
        faculties = facultiesSel.xpath('//select[@id="selfac"]/option[normalize-space(@value)!=""]')

        # Loop through each faculty option
        for faculty_code in enumerate(faculties):

            # Build POST payload to search experts by faculty
            payload = {
                "faculty": faculty_code,
                "department": "",
                "keyword": ""
            }
            
            # Send POST request to search experts for this faculty
            facultyResponse = requests.post(self.SEARCH_URL, data=payload, proxies=self.proxies,verify=False)
            
            # Convert search response to Selector
            facultySel = Selector(text=facultyResponse.text)
            
            # Extract all rows from expert listing table
            rows = facultySel.xpath('//table[@id="list_search_expert"]//tr')
            
            # If no experts found, move to next faculty
            if not rows:
                continue 
            
            # Loop through each expert row
            for row in enumerate(rows):
                item = {}

                # Name
                item["name"] = row.xpath('.//div[@class="expert-title"]/p/text()').get(default="").strip()

                # Title (join <br> lines)
                title_parts = row.xpath('.//div[@class="col-sm-7"]/p//text()').getall()
                item["title"] = " | ".join(t.strip() for t in title_parts if t.strip())

                # Email
                item["email"] = row.xpath('.//a[starts-with(@href,"mailto:")]/text()').get(default="").strip()

                # Extract profile page URL
                profile_url = row.xpath('.//a[contains(text(),"view profile")]/@href').get()
    
                phone = ""
                payload = {}
                
                # If profile page exists, open it to get phone number
                if profile_url:
                    
                    try:
                        # Send request to profile page
                        prof_res = requests.get(profile_url, proxies=self.proxies, verify=False, timeout=120)
                        prof_sel = Selector(text=prof_res.text)
                        
                        # If CV page is blocked or unavailable
                        if "The CV that you are looking for is not available" in prof_res.text:
                            print("❌ CV blocked or unavailable")
                            item["url"] = self.BASE_URL
                        
                        else:
                            item["url"] = profile_url
                            
                        # Extract phone number from profile page
                        phone = prof_sel.xpath(
                            '//i[contains(@class,"fa-phone")]/parent::li/text()'
                        ).get(default="").strip()
                    
                    except Exception as e:
                        print(f"⚠️ Profile request failed: {profile_url}")
                        with open('failedprofile.txt','+a')as f:f.write(profile_url + '\n')
                        item["url"] = profile_url
                        phone = ""

                item["phone"] = phone
                
                # Append one directory record
                directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": item["url"],
                    "Name": item["name"],
                    "Title":item["title"],
                    "Email": item['email'],
                    "Phone Number": item['phone'],
                    })

        directory_df = pd.DataFrame(directory_rows)
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):
        """
        This function fetches academic calendar events and saves them
        """
        
        # List to store calendar event records
        calendar_data =[]
        
        # Select all event blocks from the response
        events = response.xpath("//div[@class='twTileGridCell']//div[contains(@class,'twTileGridEvent')]")
        
        # Loop through each calendar event
        for event  in events:
            
            # Extract event title (term name)
            title = event.xpath("normalize-space(.//a[contains(@class,'twTitle')]/text())").get()
            
            # Extract event date text
            term_date = event.xpath("normalize-space(.//div[contains(@class,'singleLine')][1]/span/text())").get()
            
            # Extract event description text
            desc = event.xpath("normalize-space(.//span[contains(@class,'multiLine')]//p/text())").get()
            
            # Clean extra spaces from description
            term_desc = re.sub(r"\s+", " ", desc).strip()
            
            # Save one calendar event record
            calendar_data.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": title,
                "Term Date": term_date.strip() if term_date else "",
                "Term Date Description":  term_desc
                })
            
        cleaned_df = pd.DataFrame(calendar_data)
        save_df(cleaned_df, self.institution_id, "calendar")
