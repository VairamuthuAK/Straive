
import os
import scrapy
import unicodedata
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class MaylandSpider(scrapy.Spider):
    name ="mayland"
    institution_id =258425765270415321

    # Course url and headers
    course_url = "https://www.mayland.edu/academics/"
    course_headers = {
                'Referer': 'https://www.mayland.edu/academics/',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
                }
    
    # Directory url and headers
    directory_url = "https://www.mayland.edu/faculty-staff/directory/"
    directory_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        }

    calendar_url = "https://www.mayland.edu/academics/"

    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        # If only calendar data is required
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url,headers=self.course_headers, callback=self.parse_calendar)

        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url,headers=self.course_headers, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url,headers=self.course_headers, callback=self.parse_calendar)

        else:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url,headers=self.course_headers, callback=self.parse_calendar)

    @inline_requests
    def parse_course(self,response):

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
         
        blocks = response.xpath('//div[@class="ct-div-block pageTile"]//ul/li/a[contains(text(),"All Courses")]/@href').getall()
        rows =[]
        for block in blocks:
            headers = {
                'Referer': 'https://www.mayland.edu/academics/',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
                }
            response = yield scrapy.Request(block,headers=headers,dont_filter=True)
            data_blocks = response.xpath('//table[@bgcolor="#e3e3e3"]/tr')
            for data_block in data_blocks[1:]:

                section = data_block.xpath('./td[1]/span/text()').get('').strip()
                title = data_block.xpath('./td[2]/span/text()').get('').strip()
                course_number = data_block.xpath('./td[3]/span/text()').get('').strip()
                available = data_block.xpath('./td[6]/text()').get('').strip()
                capacity = data_block.xpath('./td[5]/span/text()').get('').strip()
                enrollment = ""

                try:
                    available = int(available)   # works for -2
                    capacity = int(capacity)

                    enrolled = capacity - available

                    # 🔒 avoid negative enrollment
                    if enrolled < 0:
                        enrolled = 0

                    enrollment = f"{enrolled}/{capacity}"

                except (ValueError, TypeError):
                    enrollment = ""

                section_split = section.split("-")
                course_name_first = f"{section_split[0]}-{section_split[1]}"
                course_title = f"{course_name_first} {title}"
                rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL":response.url,
                        "Course Name": course_title,
                        "Course Description": "",
                        "Class Number": course_number,
                        "Section": section,
                        "Instructor":", ".join(dict.fromkeys(t.strip() for t in data_block.xpath('./td[13]/span/text()').getall() if t.strip())),
                        "Enrollment": enrollment,
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": data_block.xpath('./td[14]/a/@href').get('').strip() 
                    })
        if rows:
            course_df = pd.DataFrame(rows)
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

        blocks = response.xpath('//table[@id="directory"]//td/text()').getall()
        rows=[]

        for i in range(0, len(blocks), 6):
            row = blocks[i:i+6]
            if len(row) == 6:
                first_name = row[0].strip()
                second_name = row[1].strip()
                title = row[2].strip()
                dept = row[3].strip()
                title_clean = title.strip()
                dept_clean = dept.strip()

                # split title into comma-separated parts
                title_parts = [p.strip().lower() for p in title_clean.split(",")]

                if dept_clean.lower() in title_parts:
                    course_title = title_clean
                else:
                    course_title = f"{title_clean}, {dept_clean}".strip(", ")

                rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Name": f"{first_name} {second_name}",
                            "Title": course_title,
                            "Email":row[4].strip(),
                            "Phone Number":row[5].strip(),
                        })
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")

    @inline_requests
    def parse_calendar(self,response):

        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        
        blocks = response.xpath('//div[@class="ct-div-block"]/a[(contains(@href,"2025") or contains(@href,"2026")) and not(contains(@href,"2024")) ]/@href').getall()
        rows =[]
        for block in blocks:
            response = yield scrapy.Request(block,headers=self.course_headers,dont_filter=True)
            blogs = response.xpath('//table[@class="aosTable"]//tr')
            for block in blocks:
                response = yield scrapy.Request(
                    block,
                    headers=self.course_headers,
                    dont_filter=True
                )

                blogs = response.xpath('//table[@class="aosTable"]//tr')

                for blog in blogs:

                    term_name = blog.xpath('ancestor::table/preceding::p[strong][1]/strong/text()').get('').strip()

                    desc_nodes = blog.xpath('./td[1]/p')
                    date_nodes = blog.xpath('./td[2]/p')

                    last_term_date = ""   # 🔥 carry-forward date

                    max_len = max(len(desc_nodes), len(date_nodes))

                    for i in range(max_len):

                        desc = (
                            " ".join(
                                t.strip()
                                for t in desc_nodes[i].xpath('./text() | ./sup/text()').getall()
                                if t.strip()
                            )
                            if i < len(desc_nodes)
                            else ""
                        )


                        term_date = date_nodes[i].xpath('normalize-space(text())').get('') \
                            if i < len(date_nodes) else ""

                        # ❌ description is mandatory
                        if not desc:
                            continue

                        # 🔥 reuse previous date if missing
                        if term_date:
                            last_term_date = term_date
                        else:
                            term_date = last_term_date

                        if not term_date:
                            continue  # still empty → skip safely

                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": term_name,
                            "Term Date": term_date.replace("‐", "-").replace("–", "-"),
                            "Term Date Description": desc.replace("‐", "-").replace("–", "-"),
                        })

        if rows:
            calendar_df = pd.DataFrame(rows)  # load to dataframe
            save_df(calendar_df, self.institution_id, "calendar") 
            