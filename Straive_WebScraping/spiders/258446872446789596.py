import re
import io
import tabula
import scrapy
import requests
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class NnmcSpider(scrapy.Spider):
    name = "nnmc"
    institution_id = 258446872446789596

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://nnmc.edu/academics/schedule-of-classes.html"
    directory_source_url = "https://nnmc.edu/employees/index.html"
    calendar_url = "https://nnmc.edu/academics/Academic-Calendar.html"

   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)

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
        # Initialize list to store extracted course records
        course_rows = []
        
        # STEP 1: Extract all "Schedule of Classes" PDF URLs
        pdf_urls = [f"https://nnmc.edu{link}" for link in response.xpath('//span[contains(text(),"Schedule of Classes")]/parent::h2/following-sibling::ul/li//a/@href').getall()]
        for pdf_url in pdf_urls:
            
            # STEP 2: Download PDF file
            response = requests.get(pdf_url)
            response.raise_for_status()

            pdf_bytes = io.BytesIO(response.content)

            # STEP 3: Extract tables from PDF using Tabula
            tables = tabula.read_pdf(
                pdf_bytes,
                pages="all",
                multiple_tables=True,
                lattice=True,          # Try lattice mode first
                stream=False,
                guess=True,
                java_options="-Dfile.encoding=UTF8 -Xmx2g",
                pandas_options={"dtype": str}
            )

            # If no tables found, stop processing
            if not tables:
                raise Exception("No tables found!")

            # STEP 4: Combine all extracted tables
            df = pd.concat(tables, ignore_index=True)
            df.columns = [str(col).strip() for col in df.columns]

            # STEP 5: Iterate through each row in DataFrame
            for index, row in df.iterrows():

                # Skip completely empty rows
                if row.isna().all():
                    continue
                
                # Extract required fields from PDF table
                course_no = row["CRSE"] 
                sub = row.iloc[3]
                sec = row["SEC"] 
                crn = row["CRN"] 
                title = row["TITLE"] 
                ins = row['INSTRUCTOR_L']
                start = str(row['START']).strip()
                end = str(row['END']).strip()
                
                # Some rows may store start/end dates in alternate columns
                # If START value is invalid, fallback to BEGIN/END_
                if start and not start[0].isdigit():
                    start = row['BEGIN']
                    end = row['END_']
                    
                # Skip rows where key fields are missing
                if pd.isna(course_no) or pd.isna(sub) or pd.isna(title)or pd.isna(start):
                    continue

               # STEP 6: Append structured course record
                course_rows.append( {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": pdf_url,
                    "Course Name": f"{sub} {course_no} {title}".strip(),
                    "Course Description": '',
                    "Class Number": crn,
                    "Section": '' if pd.isna(sec) else str(sec),
                    "Instructor": '' if pd.isna(ins) else str(ins),
                    "Enrollment": '',
                    "Course Dates": f"{start} - {end}" if pd.notna(start) and pd.notna(end) else '',
                    "Location": '',
                    "Textbook/Course Materials": '',
                })
       
        # Convert scraped course rows into a DataFrame and save it
        course_df = pd.DataFrame(course_rows)
        save_df(course_df,    self.institution_id, "course")
        
    @inline_requests
    def parse_directory(self, response):
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
        # List to store extracted staff records
        directory_rows = []
        
        blocks = response.xpath('//table[@id="directory-list"]/tbody/tr')
        for block in blocks:
            url = block.xpath('./td[@headers="name"]/a/@href').get('').strip()
            staff_url = f"https://nnmc.edu{url}"
            
            staff_res = yield scrapy.Request(staff_url)
            name = block.xpath('./td[@headers="name"]/a/text()').get('').strip()
            if not name:
                name = staff_url.split('/')[-1].replace('.html','').replace('_', ', ').strip().title()
            title = block.xpath('./td[@headers="title"]//text()').get('').strip()
            dept = block.xpath('./td[@headers="department"]//text()').get('').strip()
            if title and dept:
                full_title = f"{title} - {dept}"
            else:
                full_title = title
                
            # Append extracted staff data to the results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": staff_url,
                        "Name": name,
                        "Title": full_title,
                        "Email": staff_res.xpath('//*[contains(text(),"Email:")]/a/text()').get('').strip(),
                        "Phone Number": block.xpath('./td[@headers="phone"]//text()').get('').strip(),
                    }
                )
        # Convert collected records into a DataFrame
        directory_df = pd.DataFrame(directory_rows)
        
        # Save the DataFrame using a custom helper function
        save_df(directory_df, self.institution_id, "campus")
        
        
    @inline_requests
    def parse_calendar(self, response):
        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        # Initialize list to store extracted calendar records
        calendar_rows = []
        
        # STEP 1: Extract all term names (tab buttons)
        terms = response.xpath('//div[@class="tabs__tablist tabs__tablist--mobile-collapsed"]//button/text()').getall()
        
        blocks = response.xpath('//div[@class="tabs__items"]/div')
        
        # STEP 2: Loop through each tab (term)
        for i, block in enumerate(blocks):
            inner_blocks = block.xpath('.//ul/li')
            term_name = terms[i]
            
            # STEP 3: Loop through each calendar entry
            for inner_block in inner_blocks:
                text = " ".join(inner_block.xpath('.//text()').getall()).strip()
                
                # Check if '...' occurs 2 or more times
                if text.count("...") >= 2:

                    # This regex matches any non-greedy text ending with '...' and following words
                    pattern = r'(.*?\s*\.\.\.\s*(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s*\w+\s*\d+)'
                    items = re.findall(pattern, text)

                    # Strip leading/trailing spaces
                    items = [item.strip() for item in items]

                    # Convert to numbered list and print
                    for idx, item in enumerate(items, 1):
                        print(f"{idx}. {item}")
                        desc = item.split('...')[0]
                        date = item.split('...')[1]
                        
                        # Append structured data
                        calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_url,
                            "Term Name": term_name,
                            "Term Date": date.strip(),
                            "Term Date Description": desc.strip(),
                        })
                else:
                    desc = text.split('...')[0]
                    date = text.split('...')[1]
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_url,
                        "Term Name": term_name,
                        "Term Date": date.strip(),
                        "Term Date Description": desc.strip(),
                    })
        
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
