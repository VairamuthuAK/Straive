import re
import json
import math
import time
import scrapy
import requests
import cloudscraper
import pandas as pd
from ..utils import *
from parsel import Selector
from urllib.parse import quote
from inline_requests import inline_requests



def get_with_retry(scraper, url, max_retries=3, wait_seconds=5, timeout=60):
    for attempt in range(1, max_retries + 1):
        try:
            resp = scraper.get(url, timeout=timeout)
            if resp.status_code == 200 and "Cloudflare Ray ID" not in resp.text:
                return resp
            else:
                print(f"⚠️ Staff attempt {attempt}: Status {resp.status_code}")
        except Exception as e:
            print(f"❌ Staff attempt {attempt}: Exception → {e}")

        if attempt < max_retries:
            time.sleep(wait_seconds)

    return None


class JhuSpider(scrapy.Spider):
    name = "jhu"
    institution_id = 258444487410345937

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://courses.jhu.edu/?terms=Spring+2026"
    directory_source_url = "https://publichealth.jhu.edu/faculty/directory/list?display_type=table"
    calendar_url = "https://registrar.jhu.edu/academic-calendar/"

   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            self.parse_directory()
        elif mode == 'calendar':
            self.parse_calendar()
            
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_calendar()
        elif mode in ['directory_calendar', 'calendar_directory']:
            self.parse_directory()
            self.parse_calendar()
        
        # # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
            self.parse_calendar()

    # Parse methods UNCHANGED from your original
    @inline_requests
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
        # List to store parsed course records
        course_rows = []
        
        # API endpoint to fetch course search configuration
        course_configuration_url = "https://api.sis.jhu.edu/api/coursesearch/configuration"
        
        course_configuration_response = yield scrapy.Request(course_configuration_url)
        json_data = json.loads(course_configuration_response.text)
        
        # Extract Typesense API key and nearest node
        typesenseApiKey = json_data['data'].get('typesenseApiKey','')
        typesenseNearestNode = json_data['data'].get('typesenseNearestNode','')
        
        # URL-encode the API key for safe usage in URL
        encoded_key = quote(typesenseApiKey, safe="")
        
        # Construct the Typesense multi-search API URL
        api_url = f"https://{typesenseNearestNode}/multi_search?x-typesense-api-key={encoded_key}"
        
        payload = "{\"searches\":[{\"query_by\":\"Title,OfferingName,OfferingVariations,SectionName,Description,InstructorsFullName\",\"infix\":\"always,off,off,off,off,off\",\"per_page\":30,\"num_typos\":\"2,0,0,0,0,2\",\"max_candidates\":1000,\"sort_by\":\"_text_match:desc,OfferingName:asc,SectionName:asc\",\"facet_return_parent\":\"Areas.Description\",\"stopwords\":\"stopwords\",\"highlight_full_fields\":\"Title,OfferingName,OfferingVariations,SectionName,Description,InstructorsFullName\",\"collection\":\"sections\",\"q\":\"*\",\"facet_by\":\"AllDepartments,Areas.Description,CMN_TermsID,Credits,DOW,HierarchicalTerm.lvl0,Level,LocationDelimited,SchoolName,Status,SubDepartment,TimeOfDay\",\"filter_by\":\"HierarchicalTerm.lvl0:=[`Spring 2026`]\",\"max_facet_values\":100,\"page\":1},{\"query_by\":\"Title,OfferingName,OfferingVariations,SectionName,Description,InstructorsFullName\",\"infix\":\"always,off,off,off,off,off\",\"per_page\":0,\"num_typos\":\"2,0,0,0,0,2\",\"max_candidates\":1000,\"sort_by\":\"_text_match:desc,OfferingName:asc,SectionName:asc\",\"facet_return_parent\":\"Areas.Description\",\"stopwords\":\"stopwords\",\"highlight_full_fields\":\"Title,OfferingName,OfferingVariations,SectionName,Description,InstructorsFullName\",\"collection\":\"sections\",\"q\":\"*\",\"facet_by\":\"HierarchicalTerm.lvl0\",\"max_facet_values\":100,\"page\":1}]}"
        headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'content-type': 'text/plain',
        'origin': 'https://courses.jhu.edu',
        'priority': 'u=1, i',
        'referer': 'https://courses.jhu.edu/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        }
        
        # Make POST request to get available term facets
        term_api_response = requests.request("POST", api_url, headers=headers, data=payload)
        
        term_json_data = json.loads(term_api_response.text)
        term_blocks = term_json_data['results'][1]['facet_counts'][0]['counts']
        for term_block in term_blocks:
            count = term_block.get('count','')
            term_name = term_block.get('value','')
            match = re.search(r'\b(20\d{2})\b', term_name)
            
            # Process only terms from year 2025 onward
            if match and int(match.group(1)) >= 2025:
                
                # Calculate total pages (250 records per page)
                pages = math.ceil((int(count) + 250) / 250)
                
                print(term_name)
                for page_no in range(1,pages,1):
                    
                    # Payload for paginated term-specific course data
                    term_payload = """{\"searches\":[{\"query_by\":\"Title,OfferingName,OfferingVariations,SectionName,Description,InstructorsFullName\",\"infix\":\"always,off,off,off,off,off\",\"per_page\":250,\"num_typos\":\"2,0,0,0,0,2\",\"max_candidates\":1000,\"sort_by\":\"_text_match:desc,OfferingName:asc,SectionName:asc\",\"facet_return_parent\":\"Areas.Description\",\"stopwords\":\"stopwords\",\"highlight_full_fields\":\"Title,OfferingName,OfferingVariations,SectionName,Description,InstructorsFullName\",\"collection\":\"sections\",\"q\":\"*\",\"facet_by\":\"AllDepartments,Areas.Description,CMN_TermsID,Credits,DOW,HierarchicalTerm.lvl0,Level,LocationDelimited,SchoolName,Status,SubDepartment,TimeOfDay\",\"filter_by\":\"HierarchicalTerm.lvl0:=[`"""+term_name+"""`]\",\"max_facet_values\":100,\"page\":"""+str(page_no)+"""},{\"query_by\":\"Title,OfferingName,OfferingVariations,SectionName,Description,InstructorsFullName\",\"infix\":\"always,off,off,off,off,off\",\"per_page\":0,\"num_typos\":\"2,0,0,0,0,2\",\"max_candidates\":1000,\"sort_by\":\"_text_match:desc,OfferingName:asc,SectionName:asc\",\"facet_return_parent\":\"Areas.Description\",\"stopwords\":\"stopwords\",\"highlight_full_fields\":\"Title,OfferingName,OfferingVariations,SectionName,Description,InstructorsFullName\",\"collection\":\"sections\",\"q\":\"*\",\"facet_by\":\"HierarchicalTerm.lvl0\",\"max_facet_values\":100,\"page\":1}]}"""
                    
                    # Send POST request for paginated course data
                    term_response = yield scrapy.Request(url=api_url,method="POST",headers=headers,body=term_payload)
                
                    term_data = json.loads(term_response.text)
                    data_blocks = term_data['results'][0]['hits']
                    for data_block in data_blocks:
                        source_url = f"https://courses.jhu.edu/?terms={term_name.replace(' ','+')}"
                        Description = data_block['document'].get('Description','')
                        Location = data_block['document'].get('Location','').replace('No Location','')
                        InstructorsFullName = data_block['document'].get('InstructorsFullName','')
                        SectionName = data_block['document'].get('SectionName','')
                        title = data_block['document'].get('Title','')
                        OfferingName = data_block['document'].get('OfferingName','')
                        
                        meetings = (
                            data_block
                                .get('document', {})
                                .get('SectionDetails', {})
                                .get('Meetings', [])
                        )

                        if meetings:
                            dates = meetings[0].get('Dates', '')
                        else:
                            dates = ''
                            
                        # Append structured course record
                        course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": source_url,
                            "Course Name": f"{title} {OfferingName}",
                            "Course Description": Description,
                            "Class Number": '',
                            "Section": SectionName,
                            "Instructor": InstructorsFullName,
                            "Enrollment": data_block['document'].get('SeatsAvailable',''),
                            "Course Dates": dates,
                            "Location": Location,   
                            "Textbook/Course Materials": '',
                        })
                        
        # Convert scraped course rows into a DataFrame and save it
        course_df = pd.DataFrame(course_rows)
        save_df(course_df,    self.institution_id, "course")
        
        
    def parse_directory(self):
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
        # List to store parsed directory records
        directory_rows = []

        # Base domain for building absolute URLs
        base_url = "https://publichealth.jhu.edu"
        
        # Initial directory listing URL with filters applied
        current_url = (
            f"{base_url}/faculty/directory/list"
            "?combine=&title=&departments=All"
            "&work_type%5BAll%5D=All"
            "&work_type%5Bprimary%5D=primary"
            "&work_type%5Bjoint%5D=joint"
            "&work_type%5Baffiliated%5D=affiliated"
            "&items_per_page=100"
            "&display_type=table"
        )

        # Create Cloudscraper instance to handle Cloudflare protection
        scraper = cloudscraper.create_scraper(
            browser={
                "browser": "chrome",
                "platform": "windows",
                "desktop": True
            }
        )

        page_no = 1
        
        # Loop through paginated directory pages
        while current_url:
            print(f"\n📄 Directory page {page_no}")
            
            # Fetch directory page with retry logic
            response = get_with_retry(
                    scraper,
                    current_url,
                    max_retries=3,
                    wait_seconds=5
                )


            if not response:
                print("🚫 Directory page failed after retries")
                break

            html_selector = Selector(text=response.text)

            staff_urls = [
                f"{base_url}{u}"
                for u in html_selector.xpath(
                    '//div[@class="view-content"]//tbody/tr/td[1]/a/@href'
                ).getall()
            ]

            for i, staff_url in enumerate(staff_urls, start=1):
                print(f"👤 {i}/{len(staff_urls)} → {staff_url}")

                staff_response = get_with_retry(
                    scraper,
                    staff_url,
                    max_retries=3,
                    wait_seconds=5
                )

                if not staff_response:
                    print(f"🚫 Skipped staff page: {staff_url}")
                    continue

                staff_selector = Selector(text=staff_response.text)

                cfemail = staff_selector.xpath(
                    '//span[@class="__cf_email__"]/@data-cfemail'
                ).get('').strip()
                
                # Decode Cloudflare email if present
                if cfemail:
                    r = int(cfemail[:2], 16)
                    email = ''.join(
                        chr(int(cfemail[i:i+2], 16) ^ r)
                        for i in range(2, len(cfemail), 2)
                    )
                else:
                    email = ''
                
                # Clean leading dot if present in decoded email
                if email.startswith("."):
                    email = email[1:]

                name = " ".join(
                    staff_selector.xpath(
                        '//h1[@class="header__title"]/div//text()'
                    ).getall()
                ).strip()

                desc = staff_selector.xpath(
                    '//div[@class="header__rank"]/div/text()'
                ).get('').strip()

                dept = staff_selector.xpath(
                    '//div[@class="departmental-affiliations"]/div/a/text()'
                ).get('').strip()

                phone = "".join(
                    staff_selector.xpath(
                        '//div[contains(@class,"field-name--field-phone")]//text()'
                    ).getall()
                ).strip()

                full_title = f"{desc} - {dept}" if desc and dept else desc

                # Append structured staff data
                directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": staff_url,
                    "Name": name,
                    "Title": re.sub(r'\s+', ' ', full_title),
                    "Email": email,
                    "Phone Number": phone,
                })

            # Extract next page URL if available
            next_page = html_selector.xpath(
                '//a[@title="Go to next page"]/@href'
            ).get()

            if next_page:
                current_url = f"https://publichealth.jhu.edu/faculty/directory/list{next_page}"
                print(current_url)
                page_no += 1
                
                # Polite delay between page requests
                time.sleep(2)  
            else:
                # Exit loop when no further pages exist
                print("✅ No more pages")
                break

        # Convert collected directory data into a DataFrame
        directory_df = pd.DataFrame(directory_rows)
        
        # Save the DataFrame using the provided utility
        save_df(directory_df, self.institution_id, "campus")
    
        
        
    def parse_calendar(self):
        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        # List to store parsed calendar rows
        calendar_rows = []
        
        # Create a Cloudscraper instance to bypass anti-bot protections
        scraper = cloudscraper.create_scraper(
        browser={
                "browser": "chrome",
                "platform": "windows",
                "desktop": True
            }
        )
        # Fetch the calendar page with retry logic
        response = get_with_retry(
                    scraper,
                    self.calendar_url,
                    max_retries=3,
                    wait_seconds=5
                )
        # Initialize Scrapy Selector with the fetched HTML content
        html_selector = Selector(text=response.text)
        
        # Variable to keep track of the current term name
        term_name = ''
        
        # Select all table rows inside the calendar table body
        blocks = html_selector.xpath('//tbody[@class="row-hover"]/tr')
        
        # Iterate over each row except the last one
        for block in blocks[:-1]:
            
            # Try to extract the term name (usually in <strong> tag)
            name = block.xpath('./td[1]/strong/text()').get('').strip()
            if name:
                term_name = name
            else:
                event = block.xpath('./td[1]/text()').get('').strip()
                date = " ".join(block.xpath('./td[2]//text()').getall()).strip()
                
                # Skip rows containing standard date footnotes
                if "*Standard dates apply" not in event:
                    
                    # Append parsed data to the calendar rows list
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_url,
                        "Term Name": term_name,
                        "Term Date": date,
                        "Term Date Description": event,
                    })
    
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
