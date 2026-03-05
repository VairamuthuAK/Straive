import re
import io
import scrapy
import pdfplumber
import pandas as pd
from ..utils import save_df


def parse_calendar_clean_text(text):
    """
    Clean extracted calendar description text.

    This function:
    - Removes sequences of dots or ellipsis.
    - Removes leftover punctuation at the start of descriptions.
    - Normalizes whitespace.

    Args:
        text (str): Raw description text extracted from PDF.

    Returns:
        str: Cleaned description text.
    """
    # Remove sequences of dots or ellipses
    text = re.sub(r'\.{2,}', '', text)
    text = text.replace('…', '')

    # Remove leading commas, dashes, or dots often left over from the date split
    text = re.sub(r'^[,\-\.\s]+', '', text)

    # Normalize spaces
    return ' '.join(text.split()).strip()


class PittSpider(scrapy.Spider):
    """
    Scrapy Spider to extract:
    1. Course data (via Pitt class search API)
    2. Directory data (faculty/staff listing)
    3. Academic calendar data (PDF parsing)

    Supports configurable SCRAPE_MODE:
        - course
        - calendar
        - directory
        - combinations (course_calendar, etc.)
        - all (default)
    """
    name = "pitt"
    institution_id = 258433196176205786
    
    course_rows = []
    directory_row = []
    calendar_rows = []

    # API endpoint for course search
    course_url = "https://pitcsprd.csps.pitt.edu/psc/pitcsprd/EMPLOYEE/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch?institution=UPITT&term=2264&campus=PIT&page=1"
    course_headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://pitcsprd.csps.pitt.edu/psc/pitcsprd/EMPLOYEE/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_Main?institution=UPITT&term=2264&campus=PIT&page=1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # Directory and Calendar URLs
    directory_url = "https://www.greensburg.pitt.edu/people"
    calendar_url = "https://www.greensburg.pitt.edu/sites/default/files/assets/Calendar%20of%20Important%20Dates%20FALL%202025%203-18-25.pdf"


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            terms = {
            "2261": "Fall",
            "2264": "Spring",
            "2267": "Summer",
            }

            campuses = ["PIT", "UPG"]  # Pittsburgh + Greensburg

            for term in terms:
                for campus in campuses:
                    first_page_url = (
                        "https://pitcsprd.csps.pitt.edu/psc/pitcsprd/EMPLOYEE/SA/s/"
                        "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                        f"?institution=UPITT&term={term}&campus={campus}&page=1"
                    )
                    yield scrapy.Request(
                        url=first_page_url,
                        headers=self.course_headers,
                        callback=self.parse_course,
                        meta={
                            "term": term,
                            "campus": campus,
                            "page": 1
                        },
                        dont_filter=True
                    )
                
            
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            terms = {
                    "2261": "Fall",
                    "2264": "Spring",
                    "2267": "Summer",
                    }

            campuses = ["PIT", "UPG"]  # Pittsburgh + Greensburg

            for term in terms:
                for campus in campuses:
                    first_page_url = (
                        "https://pitcsprd.csps.pitt.edu/psc/pitcsprd/EMPLOYEE/SA/s/"
                        "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                        f"?institution=UPITT&term={term}&campus={campus}&page=1"
                    )
                    yield scrapy.Request(
                        url=first_page_url,
                        headers=self.course_headers,
                        callback=self.parse_course,
                        meta={
                            "term": term,
                            "campus": campus,
                            "page": 1
                        },
                        dont_filter=True
                    )
        
        elif mode in ['course_calendar', 'calendar_course']:
            terms = {
                    "2261": "Fall",
                    "2264": "Spring",
                    "2267": "Summer",
                    }

            campuses = ["PIT", "UPG"]  # Pittsburgh + Greensburg

            for term in terms:
                for campus in campuses:
                    first_page_url = (
                        "https://pitcsprd.csps.pitt.edu/psc/pitcsprd/EMPLOYEE/SA/s/"
                        "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                        f"?institution=UPITT&term={term}&campus={campus}&page=1"
                    )
                    yield scrapy.Request(
                        url=first_page_url,
                        headers=self.course_headers,
                        callback=self.parse_course,
                        meta={
                            "term": term,
                            "campus": campus,
                            "page": 1
                        },
                        dont_filter=True
                    )
            
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            terms = {
                    "2261": "Fall",
                    "2264": "Spring",
                    "2267": "Summer",
                    }

            campuses = ["PIT", "UPG"]  # Pittsburgh + Greensburg

            for term in terms:
                for campus in campuses:
                    first_page_url = (
                        "https://pitcsprd.csps.pitt.edu/psc/pitcsprd/EMPLOYEE/SA/s/"
                        "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                        f"?institution=UPITT&term={term}&campus={campus}&page=1"
                    )
                    yield scrapy.Request(
                        url=first_page_url,
                        headers=self.course_headers,
                        callback=self.parse_course,
                        meta={
                            "term": term,
                            "campus": campus,
                            "page": 1
                        },
                        dont_filter=True
                    )
            
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)


    

    def parse_course(self, response):
        """
        Parse course list API response and queue requests for course descriptions.
        Also handles pagination.
        """

        self.logger.info(f"Parsing courses from: {response.url}")
        datas = response.json()

        term = response.meta["term"]
        campus = response.meta["campus"]
        current_page = response.meta["page"]
        for data in datas.get('classes', []):
            # Basic course metadata extraction
            subject = data.get('subject', '')
            catalog_nbr = data.get('catalog_nbr', '')
            descr = data.get('descr', '')
            sec = data.get('class_section', '')
            component = data.get('component', '')

            section = f"{sec} - {component} ({catalog_nbr})"
            course_name = f"{subject} {catalog_nbr} {descr}"
            class_number = data.get('class_nbr', '')
            start_dt = data.get('start_dt', '')
            end_dt = data.get('end_dt', '')
            course_date = f"{start_dt} - {end_dt}"

            # Instructor parsing
            instructors = [
                i.get('name').strip()
                for i in data.get('instructors', [])
                if i.get('name') and i.get('name').strip() != '-'
            ]

            instructor = ', '.join(instructors)
            enrollment_total = data.get('enrollment_total', '')
            class_capacity = data.get('class_capacity', '')
            enrollment = f"{enrollment_total} / {class_capacity}"
    
            location = data.get('location_descr', '')

            # Course description endpoint
            descr_url = (
                "https://pitcsprd.csps.pitt.edu/psc/pitcsprd/EMPLOYEE/SA/s/"
                "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassDetails"
                f"?institution=UPITT&class_nbr={class_number}&term={term}"
            )
        
            yield scrapy.Request(
                url=descr_url,
                headers=self.course_headers,
                callback=self.parse_course_desc,
                meta={
                    "course_name": course_name,
                    "class_number": class_number,
                    "section": section,
                    "instructor": instructor,
                    "enrollment": enrollment,
                    "course_date": course_date,
                    "location": location,
                    "source_url": 'https://pitcsprd.csps.pitt.edu/psp/pitcsprd/EMPLOYEE/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_Main?institution=UPITT'
                }
            )

        # Pagination handling
        total_pages = datas.get("totalPages") or datas.get("total_pages")
        if total_pages and current_page < total_pages:
            next_page = current_page + 1
            next_url = (
                "https://pitcsprd.csps.pitt.edu/psc/pitcsprd/EMPLOYEE/SA/s/"
                "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                f"?institution=UPITT&term={term}&campus={campus}&page={next_page}"
            )

            yield scrapy.Request(
                url=next_url,
                headers=self.course_headers,
                callback=self.parse_course,
                meta={
                    "term": term,
                    "campus": campus,
                    "page": next_page
                },
                dont_filter=True
            )

    def parse_course_desc(self, response):
        """
        Extract course description from course detail API.
        """

        description = ""
        if response.status == 200:
            try:
                desc_data = response.json()
                description = (
                    desc_data
                    .get("section_info", {})
                    .get("catalog_descr", {})
                    .get("crse_catalog_description", "")
                )
            except Exception:
                description = ""
       
        # Append parsed row
        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.meta["source_url"],
            "Course Name": response.meta["course_name"],
            "Course Description": description,
            "Class Number": response.meta["class_number"],
            "Section": response.meta["section"],
            "Instructor": response.meta["instructor"],
            "Enrollment": response.meta["enrollment"],
            "Course Dates": response.meta["course_date"],
            "Location": response.meta["location"],
            "Textbook/Course Materials": "",
        })


    def parse_directory(self, response):
        """
        Parse faculty/staff directory listing.
        """
        people = response.xpath('//div[@class="views-row"]')
        for person in people:
            name = ''.join(person.xpath('.//div[@class="views-field-title"]//a/text()').getall()).replace('»','').strip()
            title = person.xpath('.//div[@class="views-field views-field-field-title"]/div/text()').get('').strip()
            phone = person.xpath('.//div[@class="views-field views-field-field-phone"]/div/a/text()').get('').strip()
            email = person.xpath(".//div[@class='views-field views-field-field-email']/div/a/text()").get('')
            self.directory_row.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone
            })

    def parse_calendar(self, response):
        """
        Parse academic calendar PDF and extract date events.
        """
        # Pattern matches month/day or TBA entries
        date_pattern = re.compile(
        r'^((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+(?:[\s\-\–\&,]+\d+)?|TBA)', 
            re.IGNORECASE
            )
        with pdfplumber.open(io.BytesIO(response.body)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text: continue

                for line in text.split('\n'):
                    line = line.strip()
                    match = date_pattern.match(line)
                    
                    if match:
                        termdate = match.group()
                        # Extract description text after date
                        raw_desc = line[len(termdate):].strip()
                        
                        # Clean the description and the date string
                        cleaned_date = termdate.strip()
                        cleaned_desc = parse_calendar_clean_text(raw_desc)
                        
                        if cleaned_desc:
                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": self.calendar_url,
                                "Term Name": 'FALL 2025',
                                "Term Date": cleaned_date,
                                "Term Date Description": cleaned_desc
                            })

    
    def closed(self, reason):
        """
        Runs when spider finishes.
        Converts stored data into DataFrames and saves output files.
        """
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")

        df = pd.DataFrame(self.directory_row)
        save_df(df, self.institution_id, "campus")
