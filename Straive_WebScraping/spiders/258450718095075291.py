import math
import scrapy
import pandas as pd
from ..utils import save_df


class DukeSpider(scrapy.Spider):
    """
    Spider to scrape Course, Directory, and Academic Calendar data from Duke University.
    
    This spider interacts with:
    1. DukeHub API: For real-time course listings and section details.
    2. OIT Solr API: For staff directory information.
    3. Registrar Webpage: For academic calendar events.
    """

    name = "duke"
    institution_id = 258450718095075291

    # Term codes corresponding to specific academic semesters (e.g., 1950 = Spring 2026)
    COURSE_TERM_CODES = [
        "1970",  # 2026 Summer II
        "1965",  # 2026 Summer I
        "1950",  # 2026 Spring
        "1945",  # 2026 Winter
    ]

    # Data containers for different scrape targets
    course_rows = []
    directory_row = []
    calendar_rows = []


    course_url = 'https://dukehub.duke.edu/psc/CSPRD01/EMPLOYEE/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch?institution=DUKEU&term=1945&campus=DUKE&enrl_stat=O&crse_attr=&crse_attr_value=&page=1'
    course_headers = {
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'https://dukehub.duke.edu/psc/CSPRD01/EMPLOYEE/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_Main?institution=DUKEU&term=1945&campus=DUKE&enrl_stat=O&crse_attr=&crse_attr_value=&page=1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }

    # API and Web Endpoints
    directory_url = "https://oit.duke.edu/solrsearch?q=*&qf=twm_X3b_en_federated_title_sort%20tm_X3b_en_rendered_item%20tm_X3b_und_rendered_item&bq=twm_X3b_en_federated_title_sort%5E2&pf=twm_X3b_en_federated_title_sort%5E2&pf2=twm_X3b_en_federated_title_sort%5E4&pf3=twm_X3b_en_federated_title_sort%5E4&facet=on&facet.limit=-1&facet.sort=count&hl=on&hl.fl=tm_X3b_en_rendered_item%2Ctm_X3b_und_rendered_item&hl.usePhraseHighlighter=true&hl.fragsize=85&hl.snippets=3&rows=34&start=0&defType=edismax&fl=id%2Css_federated_type%2Css_federated_content%2Ctwm_X3b_en_federated_content%2Ctwm_X3b_und_federated_content%2Ctwm_X3b_en_federated_title_sort%2Csm_federated_terms%2Css_federated_title%2Css_federated_title_override%2Css_federated_title_sort%2Css_federated_subtitle%2Css_federated_location%2Cds_federated_date%2Css_urls%2Css_field_location_text%2Css_additional_url%2Css_field_location_link%2Ctm_X3b_en_rendered_item%2Ctm_X3b_und_rendered_item%2Css_federated_feed_image%2Css_federated_feed_image_alt%2Css_federated_image%2Css_federated_image_alt%2Css_federated_author%2Css_federated_email%2Css_federated_phone%2Css_federated_source%2Css_custom_ha_service_summary%2Css_custom_ha_service_title%2Css_custom_ha_service_url%2Css_custom_ha_service_image%2Css_custom_ha_service_image_alt&sort=ss_federated_title_sort_string%20asc%2C%20ss_federated_title_sort_string%20asc&facet.field=%7B%21ex%3Dsm_federated_terms%7Dsm_federated_terms&fq=%7B%21tag%3Dss_federated_type%7Dss_federated_type%3A%28%22Profile%22%29&fq=%7B%21tag%3D-ss_federated_source%7D-ss_federated_source%3A%28%221%22%29"
    calendar_url = "https://registrar.duke.edu/2025-2026-academic-calendar/"


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':

            for term in self.COURSE_TERM_CODES:
                url = (
                "https://dukehub.duke.edu/psc/CSPRD01/EMPLOYEE/SA/s/"
                "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                f"?institution=DUKEU&term={term}"
                "&campus=DUKE&location=DURHAM"
                "&enrl_stat=O&crse_attr=&crse_attr_value=&page=1"
                )

                yield scrapy.Request(
                url=url,
                headers=self.course_headers,
                callback=self.parse_pagination,
                meta={"term": term},
                dont_filter=True,
            )
                
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
                
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            for term in self.COURSE_TERM_CODES:
                url = (
                "https://dukehub.duke.edu/psc/CSPRD01/EMPLOYEE/SA/s/"
                "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                f"?institution=DUKEU&term={term}"
                "&campus=DUKE&location=DURHAM"
                "&enrl_stat=O&crse_attr=&crse_attr_value=&page=1"
                )

                yield scrapy.Request(
                url=url,
                headers=self.course_headers,
                callback=self.parse_pagination,
                meta={"term": term},
                dont_filter=True,
            )

            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            
        elif mode in ['course_calendar', 'calendar_course']:
            for term in self.COURSE_TERM_CODES:
                url = (
                "https://dukehub.duke.edu/psc/CSPRD01/EMPLOYEE/SA/s/"
                "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                f"?institution=DUKEU&term={term}"
                "&campus=DUKE&location=DURHAM"
                "&enrl_stat=O&crse_attr=&crse_attr_value=&page=1"
                )

                yield scrapy.Request(
                url=url,
                headers=self.course_headers,
                callback=self.parse_pagination,
                meta={"term": term},
                dont_filter=True,
            )
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
            
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # # All three (default)
        else:
            for term in self.COURSE_TERM_CODES:
                url = (
                "https://dukehub.duke.edu/psc/CSPRD01/EMPLOYEE/SA/s/"
                "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassSearch"
                f"?institution=DUKEU&term={term}"
                "&campus=DUKE&location=DURHAM"
                "&enrl_stat=O&crse_attr=&crse_attr_value=&page=1"
                )

                yield scrapy.Request(
                url=url,
                headers=self.course_headers,
                callback=self.parse_pagination,
                meta={"term": term},
                dont_filter=True,
            )
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)


    # DETECT TOTAL PAGES DYNAMICALLY
    def parse_pagination(self, response):
        """
        Parses the initial API response to calculate total records and 
        generate requests for all remaining pages.
        """

        term = response.meta["term"]
        data = response.json()

        total_records = int(data.get("TotalRowCount", 0))
        per_page = 100  # DukeHub API returns 100 records per page
        total_pages = math.ceil(total_records / per_page)

        self.logger.info(f"Term {term} → {total_records} records → {total_pages} pages")

        for page in range(1, total_pages + 1):
            url = response.url.replace("page=1", f"page={page}")
            yield scrapy.Request(
                url=url,
                headers=self.course_headers,
                callback=self.parse_course,
                meta={"term": term},
                dont_filter=True,
            )


    def parse_course(self, response):
        """
        Iterates through the list of classes in a page and requests 
        detailed info (description and textbooks) for each.
        """

        term = response.meta["term"]
        datas = response.json()
        datas = response.json()
        for data in datas.get('classes', []):
            # Extract basic course identifiers
            subject = data.get('subject', '')
            catalog_nbr = data.get('catalog_nbr', '')
            descr = data.get('descr', '')
            sec = data.get('class_section', '')
            component = data.get('component', '')
            course_name = f"{subject} {catalog_nbr} {descr}"
            class_number = data.get('class_nbr', '')

            section = f"{sec} - {component} ({class_number})"
            start_dt = data.get('start_dt', '')
            end_dt = data.get('end_dt', '')
            course_date = f"{start_dt} - {end_dt}"

            # Format instructor list
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
           
            # Construct URL for the secondary API call to get class details (Description/Books)
            descr_url = (
                "https://dukehub.duke.edu/psc/CSPRD01/EMPLOYEE/SA/s/"
                "WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_ClassDetails"
                f"?institution=DUKEU&class_nbr={class_number}&term={term}"
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
                    "source_url": 'https://dukehub.duke.edu/psp/CSPRD01/EMPLOYEE/SA/s/WEBLIB_HCX_CM.H_CLASS_SEARCH.FieldFormula.IScript_Main?institution=DUKEU'
                }
            )

      
    def parse_course_desc(self, response):
        """
        Parses the detailed JSON for a single course to extract the long 
        description and construct the eCampus textbook URL.
        """

        description = ""
        textbool_url = ''
        if response.status == 200:
            try:
                desc_data = response.json()
                # Extract deep nested description
                description = (
                    desc_data
                    .get("section_info", {})
                    .get("catalog_descr", {})
                    .get("crse_catalog_description", "")
                )

                # Extract bookstore parameters to build the affiliate link
                params = desc_data.get("bookstore", {}).get("params", [])
                course1 = params[0].get("value", "") if len(params) > 0 else ""
                course2 = params[1].get("value", "") if len(params) > 1 else ""
                course3 = params[2].get("value", "") if len(params) > 2 else ""
                course4 = params[3].get("value", "") if len(params) > 3 else ""
                semestername = params[4].get("value", "") if len(params) > 4 else ""
                sintschoolid = params[5].get("value", "") if len(params) > 5 else ""
                textbool_url = f'https://www.ecampus.com/autocourselist.asp?courses={course1}&courses2={course2}&courses3={course3}&courses4={course4}&semestername={semestername}&sintschoolid={sintschoolid}'
                
            except Exception:
                description = ""

        # Store the final flat data structure
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
            "Textbook/Course Materials": textbool_url,
        })

    def parse_directory(self, response):
        """Parses the OIT staff directory Solr JSON response."""

        datas = response.json()
        for data in datas['response']['docs']:
            name = data.get('ss_federated_title', '').replace('»','').strip()
            title = data.get('ss_federated_subtitle', '').replace('amp;','').strip()
            email = data.get('ss_federated_email', '').strip()
            
            self.directory_row.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": 'https://oit.duke.edu/staff/',
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": ''
            })

        df = pd.DataFrame(self.directory_row)
        save_df(df, self.institution_id, "campus")

    def parse_calendar(self, response):
        """Parses the Registrar's academic calendar HTML tables."""

        blocks = response.xpath('//div[@class="table-wrapper"]')
        for block in blocks:
            # Identifies the term (e.g., Fall 2025) usually found in the preceding header
            term_name = block.xpath('preceding-sibling::h5[1]//text()').get()
            term_name.strip() if term_name else ''
            rows = block.xpath('.//tbody/tr')

            for row in rows:
                date_text = row.xpath('./td[1]/text()').get()
                desc_list = row.xpath('./td[3]//text()').getall()
                desc_text = ' '.join(desc_list).replace('\xa0', ' ').strip() if desc_list else ''

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": term_name,
                    "Term Date": date_text,
                    "Term Date Description": desc_text
                })

        
        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")

    def closed(self, reason):
        """Saves course data to CSV/DB upon spider completion."""
        if self.course_rows:
            course_df = pd.DataFrame(self.course_rows)
            save_df(course_df, self.institution_id, "course")
