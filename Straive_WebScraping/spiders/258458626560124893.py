import re
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO


class AndersonSpider(scrapy.Spider):

    name = "anderson"
    institution_id = 258458626560124893
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://accessau.anderson.edu/documents/MSCH_Undergrad_2024_25_Publish_2024_02_15_V2.pdf"

    # DIRECTORY CONFIG
    directory_source_url = "https://andersonuniversity.edu/faculty-directory/"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course and Calendar data is extracted using pdfplumber.

        - Directory data is available as static HTML pages and is scraped
        using normal Scrapy requests in the `parse_directory` callback.

        """

        # Single functions
        if mode == "course":
            self.parse_course()
            # self.parse_course()
        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

    # PARSE COURSE
    def parse_course(self):
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
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
    
        pdf_url = self.course_url
        def clean_title(text):

            if not text:
                return ""

            text = re.split(r'[`\d]', text)[0]
            text = text.replace("Introto","Intro to").replace("Principlesof","Principles of")

            fixes = {
                "Introtothe":"Intro to the","Teachingasa":"Teaching as a",
                "Businessasa":"Business as a","Foundationsof":"Foundations of",
                "Prinof":"Principles of","Issuesin":"Issues in","Aspectsof":"Aspects of",
                "Lessonsin":"Lessons in","Internshipin":"Internship in",
                "ThenandNow":"Then and Now","Rhetoricand":"Rhetoric and",
                "Imagesof":"Images of","ModernBiology":"Modern Biology",
                "Introto":"Intro to","Writingforthe":"Writing for the",
                "Mediaand":"Media and","Writingand":"Writing and",
                "Christand":"Christ and","Seminarin":"Seminar in"
            }

            for k,v in fixes.items():
                text = text.replace(k,v)

            return text.strip().replace(",","")


        def get_instructor(row):

            line = " ".join([str(i) for i in row if i])

            m = re.search(r'`[\d\s-]*([A-Z][a-z]+(?:,\s*[A-Z]{1,2})?)', line)

            if m:
                return m.group(1).strip()

            if "Staff" in line:
                return "Staff"

            return "Staff"

        name_fixes = {
            "MC":"McKenna / Baker","Van":"Van Groningen,GG",
            "Carp":"Carpenter,ES","Park":"Parks,JR",
            "Coo":"Coon,FA","Lam":"Lambright,JJ",
            "Cha":"Chappell,RA","Evan":"Evans,CR",
            "Bad":"Bade,ML","Ball":"Ballman,SC",
            "Wall":"Wallace,CE","Kenn":"Kennedy,SB",
            "Mora":"Moran,SA","Stan":"Stankiewicz,DJ"
        }


        res = requests.get(pdf_url)

        with pdfplumber.open(BytesIO(res.content)) as pdf:

            for p in range(4, len(pdf.pages)):
                page = pdf.pages[p]
                table = page.extract_table({
                    "vertical_strategy":"text",
                    "horizontal_strategy":"text",
                    "snap_tolerance":3
                })

                if not table:
                    continue


                for row in table:

                    row = [str(i).strip() if i else "" for i in row]

                    if len(row) < 15:
                        continue


                    title_area = " ".join(row[8:12]).strip()
                    title = clean_title(title_area)

                    instructor = get_instructor(row)
                    instructor = name_fixes.get(instructor, instructor)


                    # FALL
                    if row[2].isdigit():
                        sec = row[6].replace("'","")
                        name = f"{row[5]} {title}".strip()

                        if not sec:
                            sec = row[5]
                            name = f"{row[4]} {title}".strip()

                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Course Name": name,
                            "Course Description": "",
                            "Class Number": row[2],
                            "Section": sec,
                            "Instructor": instructor,
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": "",
                            "Textbook/Course Materials": ""
                        })


                    # SPRING
                    if row[1].isdigit():

                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Course Name": f"{row[3]} {title}".strip(),
                            "Course Description": "",
                            "Class Number": row[1],
                            "Section": row[4].replace("'",""),
                            "Instructor": instructor,
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": "",
                            "Textbook/Course Materials": ""
                        })


                # SPECIAL SPRING
                    if row[0].isdigit() and "Sprg" in row[1]:
                        raw = " ".join(row[8:12]).replace("OL","").strip()
                        raw = clean_title(raw)
                        sec = row[7].split("OA")[0]
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Course Name": f"{sec} {raw}".strip(),
                            "Course Description": "",
                            "Class Number": row[3],
                            "Section": "5E",
                            "Instructor": instructor,
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": "",
                            "Textbook/Course Materials": ""
                        })
                    

    # PARSE DIRECTORY
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
        for page in range(1,30):
            url = "https://andersonuniversity.edu/faculty-directory/?nocache=1770942258"

            payload = f"action=jet_engine_ajax&handler=listing_load_more&query%5Bquery_id%5D=41&query%5Bposts_per_page%5D=10&query%5Bsignature%5D=513cbc9f1c2dd176eb3b00a60ced1eb5e2f4b8b63108d2bf54639ab0d6b2344f&widget_settings%5Blisitng_id%5D=41660&widget_settings%5Bposts_num%5D=10&widget_settings%5Bcolumns%5D=3&widget_settings%5Bcolumns_tablet%5D=2&widget_settings%5Bcolumns_mobile%5D=1&widget_settings%5Bcolumn_min_width%5D=240&widget_settings%5Bcolumn_min_width_tablet%5D=240&widget_settings%5Bcolumn_min_width_mobile%5D=240&widget_settings%5Binline_columns_css%5D=false&widget_settings%5Bis_archive_template%5D=&widget_settings%5Bpost_status%5D%5B%5D=publish&widget_settings%5Buse_random_posts_num%5D=&widget_settings%5Bmax_posts_num%5D=9&widget_settings%5Bnot_found_message%5D=No+data+was+found&widget_settings%5Bis_masonry%5D=false&widget_settings%5Bequal_columns_height%5D=&widget_settings%5Buse_load_more%5D=yes&widget_settings%5Bload_more_id%5D=faculty-load&widget_settings%5Bload_more_type%5D=click&widget_settings%5Bload_more_offset%5D%5Bunit%5D=px&widget_settings%5Bload_more_offset%5D%5Bsize%5D=0&widget_settings%5Buse_custom_post_types%5D=&widget_settings%5Bhide_widget_if%5D=&widget_settings%5Bcarousel_enabled%5D=&widget_settings%5Bslides_to_scroll%5D=1&widget_settings%5Barrows%5D=true&widget_settings%5Barrow_icon%5D=fa+fa-angle-left&widget_settings%5Bdots%5D=&widget_settings%5Bautoplay%5D=true&widget_settings%5Bpause_on_hover%5D=true&widget_settings%5Bautoplay_speed%5D=5000&widget_settings%5Binfinite%5D=true&widget_settings%5Bcenter_mode%5D=&widget_settings%5Beffect%5D=slide&widget_settings%5Bspeed%5D=500&widget_settings%5Binject_alternative_items%5D=&widget_settings%5Bscroll_slider_enabled%5D=&widget_settings%5Bscroll_slider_on%5D%5B%5D=desktop&widget_settings%5Bscroll_slider_on%5D%5B%5D=tablet&widget_settings%5Bscroll_slider_on%5D%5B%5D=mobile&widget_settings%5Bcustom_query%5D=yes&widget_settings%5Bcustom_query_id%5D=41&widget_settings%5B_element_id%5D=&widget_settings%5Bcollapse_first_last_gap%5D=false&widget_settings%5Blist_tag_selection%5D=&widget_settings%5Blist_items_wrapper_tag%5D=div&widget_settings%5Blist_item_tag%5D=div&widget_settings%5Bempty_items_wrapper_tag%5D=div&page_settings%5Bpost_id%5D=false&page_settings%5Bqueried_id%5D=41668%7CWP_Post&page_settings%5Belement_id%5D=dbb3358&page_settings%5Bpage%5D={page}&listing_type=false&isEditMode=false&addedPostCSS%5B%5D=41660"
            headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://andersonuniversity.edu',
            'referer': 'https://andersonuniversity.edu/faculty-directory/',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            }
        
            yield scrapy.Request(url,method = 'POST',body=payload,headers=headers,callback=self.parse_directory_detail)

        
        
    def parse_directory_detail(self,response):
        if re.search(r'faculty_profile\\/(.*?)\\/',response.text):
            urls = re.findall(r'faculty_profile\\/(.*?)\\/', response.text)
            if urls:
                for url in urls:
                    url = f'https://andersonuniversity.edu/faculty_profile/{url}'
                    yield scrapy.Request(url,headers=self.directory_headers,callback=self.parse_directory_final)

    def parse_directory_final(self,response):
        title = response.xpath('//div[@class="elementor-element elementor-element-ff3d249 e-con-full e-flex e-con e-child"]//div[@class="elementor-heading-title elementor-size-default"]/text()').getall()
        title = title[1]
        email = response.xpath('//div[@class="elementor-element elementor-element-e993097 elementor-widget elementor-widget-text-editor"]/div/text()').get('').strip()
        name = response.xpath('//h1/text()').get('').strip()
        phone = response.xpath('//div[@class="elementor-element elementor-element-34367fc elementor-widget elementor-widget-text-editor"]/div/text()').get('').strip()

        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": response.url,
        "Name": re.sub(r'\s+',' ',name),
        "Title": re.sub(r'\s+',' ',title),
        "Email": email,
        "Phone Number":phone
        })

    # PARSE CALENDAR
    def parse_calendar(self):

        urls = """https://andersonuniversity.edu/wp-content/uploads/2026/01/PROV_24-25_AcademicCalendar-25-26-1.pdf
        https://andersonuniversity.edu/wp-content/uploads/2026/01/PROV_25-26_AcademicCalendar_26-27-4.pdf"""

        urls = urls.strip().splitlines()

        for url in urls:
            data = []

            if "24-25" in url:
                data = [
                    ("SUMMER SCHOOL 2025","May 12 – June 29","Summer 7A"),
                    ("SUMMER SCHOOL 2025","June 30 – August 17","Summer 7B"),
                    ("SUMMER SCHOOL 2025","June 30 – August 17","Semester"),
                    ("POST-TRADITIONAL DEGREE PROGRAM FALL 2025","August 18 – October 6","Fall 7A"),
                    ("POST-TRADITIONAL DEGREE PROGRAM FALL 2025","October 13 – December 8 (Holidays: Nov. 26-28)","Fall 7B"),
                    ("POST-TRADITIONAL DEGREE PROGRAM SPRING 2026","January 12 – March 2 (MLK Holiday: Jan.19)","Spring 7A"),
                    ("POST-TRADITIONAL DEGREE PROGRAM SPRING 2026","March 16 – May 4","Spring 7B"),
                    ("SUMMER SCHOOL 2026","May 11 – June 28","Summer 7A"),
                    ("SUMMER SCHOOL 2026","June 29 – August 16","Summer 7B"),
                    ("SUMMER SCHOOL 2026","May 11 – August 16","Semester")
                ]

            elif "25-26" in url:
                data = [
                    ("SUMMER SCHOOL 2026","May 11 – June 28","Summer 7A"),
                    ("SUMMER SCHOOL 2026","June 29 – August 16","Summer 7B"),
                    ("SUMMER SCHOOL 2026","May 11 – August 16","Semester"),
                    ("POST-TRADITIONAL DEGREE PROGRAM FALL 2026","August 24 – October 12","Fall 7A"),
                    ("POST-TRADITIONAL DEGREE PROGRAM FALL 2026","October 19 – December 14 (Holidays: Nov. 25-27)","Fall 7B"),
                    ("POST-TRADITIONAL DEGREE PROGRAM SPRING 2027","January 11 – March 1","Spring 7A"),
                    ("POST-TRADITIONAL DEGREE PROGRAM SPRING 2027","March 15 – May 3","Spring 7B"),
                    ("SUMMER SCHOOL 2027","May 10 – June 27","Summer 7A"),
                    ("SUMMER SCHOOL 2027","June 28 – August 15","Summer 7B"),
                    ("SUMMER SCHOOL 2027","May 10 – August 15","Semester")
                ]

            # Add static rows
            for term_name,date,desc in data:
                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": url,
                    "Term Name": term_name,
                    "Term Date": date,
                    "Term Date Description": desc
                })

            # Read PDF
            try:

                res = requests.get(url, timeout=20)
                res.raise_for_status()

                with pdfplumber.open(BytesIO(res.content)) as pdf:

                    for page in pdf.pages:

                        text = page.extract_text(layout=True)
                        if not text: 
                            continue

                        current_term = ""

                        for line in text.split("\n"):

                            line = line.strip()
                            if not line: 
                                continue

                            if "|" in line and any(x in line.upper() for x in ["FALL","SPRING","SUMMER"]):

                                current_term = line.replace("|","").replace("  "," ").strip()
                                continue

                            date_match = re.search(r'([A-Z][a-z]+,\s+[A-Z][a-z]+\s+\d{1,2}.*|TBD)$', line)

                            if date_match and current_term:

                                date_val = date_match.group(0).strip()
                                desc = line[:date_match.start()].strip()

                                self.calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": url,
                                    "Term Name": current_term,
                                    "Term Date": date_val,
                                    "Term Date Description": re.sub(r'\s+',' ',desc)
                                })

            except Exception as e:
                pass


    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")
        