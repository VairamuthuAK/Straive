import time
import scrapy
import urllib.parse
import cloudscraper
import pandas as pd
import unicodedata
from ..utils import *
from parsel import Selector
from requests.exceptions import RequestException
from inline_requests import inline_requests

def safe_text(sel, xpath):
    return sel.xpath(xpath).get(default="").strip()

def decode_email_from_script(script_text):
    ml_match = re.search(r'ml="([^"]+)"', script_text)
    mi_match = re.search(r'mi="([^"]+)"', script_text)

    if not ml_match or not mi_match:
        return ""

    ml = ml_match.group(1)
    mi = mi_match.group(1)

    o = "".join(ml[ord(ch) - 48] for ch in mi)
    return urllib.parse.unquote(o)


class MaryMountSpider(scrapy.Spider):

    name = "mary"
    institution_id = 258455463899719643

    course_url = "https://marymount.smartcatalogiq.com/en/2025-2026/catalog/marymount-university-catalog-2025-26/courses"

    directory_url = "https://marymount.edu/?_search_directory%5Bfirst_name%5D=&_search_directory%5Blast_name%5D=&_search_directory%5Bstaff_category%5D=-1&s=&_search_directory%5Bsearch_mode%5D=advanced&post_type=staff-member&_search_directory%5Border_by%5D=last_name&_search_directory%5Border%5D=ASC"
    directory_payload = "action=fetch_employees&department=&search="
    directory_headers = {
        'accept': '*/*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://fhtc.edu',
        'priority': 'u=1, i',
        'referer': 'https://fhtc.edu/about/employee-directory/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
        }


    calendar_url = "https://marymount.edu/academics/services-resources/registrar-s-office/academic-calendar/"

    def start_requests(self):

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            self.parse_directory()

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_calendar()

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            self.parse_directory()
            self.parse_calendar()

        # All three (default)
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
            self.parse_calendar()

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
        
        links = response.xpath('//ul[@class="sc-child-item-links"]/li/a/@href').getall()
        rows=[]
        for link in links:
            url = f"https://marymount.smartcatalogiq.com{link}"
            response = yield scrapy.Request(url,dont_filter=True)
            course_links = response.xpath('//div[@id="main"]/ul/li/a/@href').getall()

            for course_link in course_links:
                url = f"https://marymount.smartcatalogiq.com{course_link}"
                response = yield scrapy.Request(url,dont_filter=True)

                title = response.xpath('//h1/text() | //h1/span/text()').getall()
                title = " ".join([t.strip() for t in title if t.strip()])
                title = re.sub(r"\s+"," ",title)

                class_number =""
                if title:
                    class_number = f'{title.split(" ")[0]} {title.split(" ")[1]}' 

                descrip = response.xpath('//div[@class="desc"]/p/span/text() | //div[@class="desc"]/div/div//p/text() | //div[@class="desc"]/div/div//p/a/text() | //div[@class="desc"]/div/p/text() | //div[@class="desc"]/div/p/a/text() | //div[@class="desc"]/p/text() | //div[@class="desc"]/p/a/text() | //div[@class="desc"]/span/text() |  //div[@class="desc"]/span/a/text()').getall()
                desc = " ".join([d.strip() for d in descrip if d.strip()]).strip()
                if desc=="":
                    desc = response.xpath('//div[@class="desc"]/text()').get("").strip()
                    
                decode_desc = unicodedata.normalize("NFKD", desc).encode("ascii", "ignore").decode()
                course_desc = re.sub(r"\r\n","",decode_desc)

                rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Course Name": title or "",
                        "Course Description": course_desc or "",
                        "Class Number": class_number or "",
                        "Section": "",
                        "Instructor": "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": '',
                    })
                
        if rows:
            course_df = pd.DataFrame(rows)
            save_df(course_df, self.institution_id, "course")

    
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

        scraper = cloudscraper.create_scraper()
        response = scraper.get(self.directory_url,timeout=30)
        resp = Selector(text=response.text)
        links = resp.xpath('//div[@class="staff-member"]/div[@class="staff-photo"]/a/@href').getall()
        rows=[]

        for link in links:

            try:
                time.sleep(1.5) 
                response = scraper.get(link,timeout=30)
                resp = Selector(text=response.text)
                script_text = resp.xpath('//p[@class="staff-member-email"]//script/text()').get("")
                match = re.search(r'decodeURIComponent\("([^"]+)"\)', script_text)

                email =""
                if match:
                    encoded = match.group(1)
                    decoded = urllib.parse.unquote(encoded)
                    email = decoded.strip("'")
                else:
                    email = decode_email_from_script(script_text)
                
                phone = resp.xpath('//p[@class="staff-member-phone"]/text()').get("").strip()
                if phone == ".":
                    phone = ""
                elif phone == '-':
                    phone = phone

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": resp.xpath('//h1/text()').get("").strip(),
                    "Title":re.sub(r"\s+"," ",", ".join(resp.xpath('//p[@class="staff-member-title"]/text() | //p[@class="staff-member-department"]/text()').getall()).strip()).replace("&nbsp;",""),
                    "Email":email,
                    "Phone Number": resp.xpath('//p[@class="staff-member-phone"]/text()').get("").strip(),
                })

            except RequestException as e:
                self.logger.warning(f"Failed to fetch {link}: {e}")
                continue

        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")
            
    
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

        scraper = cloudscraper.create_scraper()
        response = scraper.get("https://marymount.edu/academics/services-resources/registrar-s-office/academic-calendar/")
        resp = Selector(text=response.text)
        rows=[]
        links = resp.xpath('//div[@class="bialty-container"]//p/a[contains(text(),"2025") or contains(text(),"2026")]/@href').getall()

        for link in links:
            response = scraper.get(link)
            resp =Selector(text=response.text)
            blocks = resp.xpath('//h2/following-sibling::table/thead/tr/th[contains(text(),"Full Semester Course")]/parent::tr/parent::thead/parent::table/tbody/tr')
            second_blocks = resp.xpath('//div[@class="panel-body"]//table/thead/tr/th[contains(text(),"Full Semester Course")]/parent::tr/parent::thead/parent::table/thead/tr')

            if blocks:

                for block in blocks:
                    
                    term_dates = block.xpath('./td[1]/text()').get("").strip()
                    if term_dates:
                        
                        term_date = term_dates
                        term_name = block.xpath('./parent::tbody/parent::table/parent::div/h2/text()').get("").replace("Standard Academic Calendar","").strip()
                        term_desc =  block.xpath('./th/text()').get("").strip()
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": term_name,
                            "Term Date": term_date,
                            "Term Date Description": term_desc
                        })

            elif second_blocks:
                for second_block in second_blocks[1:]:
                    term_date = second_block.xpath('./td[1]/text()').get("").strip()
                    if term_date:

                        rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Term Name": second_block.xpath('./parent::thead/parent::table/parent::div/parent::div//parent::div/div/h2/text()').get("").replace('View the ',"").replace("Standard Academic Calendar","").strip(),
                                "Term Date": second_block.xpath('./td[1]/text()').get("").replace("See the","").strip(),
                                "Term Date Description": second_block.xpath('./th/text()').get("").strip()
                            })
                    
            else:
                third_blocks = resp.xpath('//div[@class="panel-body"]//table/thead/tr/th[contains(text(),"Session")]')
                count = len(third_blocks)
                for index in range(1,count+1):
                    blocks = resp.xpath('//div[@class="panel-body"]//table/thead/tr/th[contains(text(),"Session")]/parent::tr/parent::thead/parent::table/thead/parent::table/tbody/tr')
                    for block in blocks:
                        if index == 1:
                            session = "Session I"
                        elif index == 2:
                            session = "Session II"
                        else:
                            session = "Session III"
                        
                        term_date = block.xpath(f'./td[{index}]/text()').get("")
                        term_desc = block.xpath('./th/text()').get("").strip()
                        if term_date != "N/A":
                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": response.url,
                                "Term Name": block.xpath('./parent::tbody/parent::table/parent::div/parent::div/parent::div//h2/text()').get("").replace('View the ',"").replace("Standard Academic Calendar","").strip(),
                                "Term Date": term_date,
                                "Term Date Description":f"{session} - {term_desc}"
                            })
                        
        if rows:
            calendar_df = pd.DataFrame(rows)
            save_df(calendar_df, self.institution_id, "calendar")
            
       
    