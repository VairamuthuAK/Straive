import re
import json
import scrapy
import pandas as pd
from datetime import datetime
from ..utils import save_df


proxy = 'YOUR_UNBLOCKER_PROXY_HERE'

class EastSpider(scrapy.Spider):
    name = "east"

    # Unique institution ID used for all datasets
    institution_id = 258435633024886744

    # Base URLs
    base_url = "https://sisjee.iu.edu/sisigps-prd/web/igps/course/search/"
    course_url = base_url
    directory_url = "https://directory.iu.edu/dept"
    directory_headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
            'content-type': 'application/json',
            'origin': 'https://directory.iu.edu/dept',
            'priority': 'u=1, i',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            }
    calendar_url = "https://east.iu.edu/red-wolf-central/calendars/academic-calendar.html"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.directory_rows = []
        self.calendar_rows = []
        self.course_rows = []

    # ENTRY POINT – Decide What To Scrape Based On SCRAPE_MODE
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, headers=self.directory_headers, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default → scrape everything
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE SCRAPING
    def parse_course(self, response):
        """
        Step 1:
        Loop through campus codes and fetch available academic terms.
        """
        campus_codes = [
            "IUBLA", "IUCOA", "IUEAA", "IUFTW",
            "IUINA", "IUKOA", "IUNWA", "IUSBA", "IUSEA"
        ]

        for campus in campus_codes:
            terms_url = f"{self.base_url}terms.json?inst={campus}"

            yield scrapy.Request(
                url=terms_url,
                callback=self.parse_course_links,
                meta={"inst": campus}
            )

    def parse_course_links(self, response):
        """
        Step 2:
        For each term returned, request course list (POST API).
        """
        inst = response.meta["inst"]
        terms = json.loads(response.text)

        for term in terms:
            strm = term.get("strm")

            payload = {
                "inst": inst,
                "strm": strm,
                "filters": {},
                "from": 0
            }

            yield scrapy.Request(
                url=f"{self.base_url}courses.json",
                method="POST",
                headers={"Content-Type": "application/json"},
                body=json.dumps(payload),
                callback=self.parse_course_details,
                meta={"inst": inst, "strm": strm, "offset": 0}
            )

    def parse_course_details(self, response):
        """
        Step 3:
        Parse course list and handle pagination.
        """
        data = json.loads(response.text)
        courses = data.get("courses", [])

        inst = response.meta["inst"]
        strm = response.meta["strm"]
        offset = response.meta["offset"]

        # Loop through each course returned
        for course in courses:
            course_id = course.get("courseId")
            effdt = course.get("effdt")
            subject = course.get("subject", "")
            catalog = course.get("catalogNumber", "")
            title = course.get("title", "")
            car = course.get("car", "")

            name = f"{subject} {catalog} - {title}"

            raw_desc = (course.get("courseDetails") or {}).get("description")
            description = re.sub(r"\s+", " ", raw_desc or "").strip()

            # Request section details for each course
            class_url = (
                f"{self.base_url}classes.json?"
                f"courseId={course_id}&courseOfferNumber=1"
                f"&courseTopicId=0&effdt={effdt}"
                f"&strm={strm}&inst={inst}&car={car}"
            )

            yield scrapy.Request(
                url=class_url,
                callback=self.parse_course_sections,
                meta={
                    "name": name,
                    "description": description,
                    "catalog": catalog,
                    "strm": strm
                }
            )

        # -------- Pagination Logic --------
        if courses:
            new_offset = offset + len(courses)

            payload = {
                "inst": inst,
                "strm": strm,
                "filters": {},
                "from": new_offset
            }

            yield scrapy.Request(
                url=f"{self.base_url}courses.json",
                method="POST",
                headers={"Content-Type": "application/json"},
                body=json.dumps(payload),
                callback=self.parse_course_details,
                meta={"inst": inst, "strm": strm, "offset": new_offset}
            )

    def parse_course_sections(self, response):
        """
        Step 4:
        Extract class section-level details.
        """
        data = json.loads(response.text)
        classes = data.get("classes", [])

        for cls in classes:
            instructors = ", ".join(
                inst.get("fullName", "")
                for inst in cls.get("primaryInstructors", [])
                if inst.get("fullName")
            )

            meetings = cls.get("meetings") or []
            if meetings:
                meet = meetings[0]
                date_range = f"{meet.get('beginDateString','')} - {meet.get('endDateString','')}"
            else:
                date_range = ""

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_url,
                "Course Name": response.meta["name"],
                "Course Description": response.meta["description"],
                "Class Number": cls.get("classNbr", ""),
                "Instructor": instructors,
                "Enrollment": f"{cls.get('openSeats',0)}/{cls.get('totalSeats',0)}",
                "Course Dates": date_range,
                "Location": cls.get("locationDescription", "")
            })

        # Save course data
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY SCRAPING
    def parse_directory(self, response):
        """
        Step 1:
        Loop through campus directory links.
        """
        urls = response.xpath('//h2[contains(text(),"Please select a campus.")]/parent::div/div/a/@href').getall()
        for url in urls:
            url = response.urljoin(url)
            yield response.follow(
            url,
            callback=self.parse_directory_details,
            meta={"proxy": proxy}
        )
            
    def parse_directory_details(self,response):
        if re.search(r'"WORKGROUP":"(.*?)"', response.text):
            urls = re.findall(r'"WORKGROUP":"(.*?)"', response.text)
            for url in urls:
                response_url = response.url.replace('list','details')
                url = f'{response_url}/{url}'
                yield response.follow(
                    url,
                    callback=self.parse_directory_final,
                    meta={"proxy": proxy}
                )

    def parse_directory_final(self,response):
        match = re.search(
            r'deptListings:\s*(\[[\s\S]*?\])\s*}',
            response.text
        )

        if match:
            json_text = match.group(1)

            dept_data = json.loads(json_text)
            last_row = None

            for block in dept_data:
                name = block.get('NAME') or ''
                entry = block.get('ENTRY') or ''
                phone = block.get('PHONE') or ''
                
                if '@' in entry and not name and last_row:
                    last_row['Email'] = entry.strip()
                    continue 

                if 'href=' in name or '@' in name:
                    continue

                if name:
                    email = ''
                    name = re.sub(r'^\s*-\s*', '', name)
                    if '@' in phone:
                        phone = ''
                        email = phone
                    new_entry = {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Name": re.sub(r'\s+',' ', name).strip(),
                        "Title": re.sub(r'\s+',' ', entry).strip() if entry else '',
                        "Email": email if email else '',
                        "Phone Number": phone if phone else ''
                    }
                    
                    self.directory_rows.append(new_entry)
                    
                    last_row = new_entry
                else:
                    last_row = None

        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # ACADEMIC CALENDAR SCRAPING
    def parse_calendar(self, response):
        """
        Extract term-wise academic calendar details.
        """
        for section in response.xpath("//h2[@class='section-title']"):
            term_name = section.xpath("normalize-space(.)").get()
            accordion = section.xpath("following-sibling::div[@class='accordion'][1]")

            for row in accordion.xpath(".//table//tbody//tr"):
                date = row.xpath("./td[2]//text()").get()
                desc = row.xpath("./td[3]//text()").get()

                if date and desc:
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": date.strip(),
                        "Term Description": desc.strip()
                    })

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")