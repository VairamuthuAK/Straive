import json
import scrapy
import requests
import pandas as pd
from ..utils import save_df
from parsel import Selector
from datetime import datetime

class MvnuSpider(scrapy.Spider):
    """
    Scrapy spider for Mount Vernon Nazarene University (MVNU).

    This spider supports scraping:
        - Course catalog data (via MVNU Self-Service API)
        - Faculty directory data
        - Academic calendar events

    SCRAPE_MODE options:
        - course
        - directory
        - calendar
        - course_directory (or directory_course)
        - course_calendar (or calendar_course)
        - directory_calendar (or calendar_directory)
        - all (default)
    """

    name = "mvnu"
    institution_id = 258450014525745115

    # Storage containers
    course_rows = []
    calendar_rows = []

    course_url = "https://selfservice.mvnu.edu/Student/Courses/GetCatalogAdvancedSearch"
    course_headers = {
        '__isguestuser': 'true',
        '__requestverificationtoken': 'CfDJ8B0GSlrVWN9Dn-qVRI-h8-1zo0FfZdEDCRlFHWX1vhz18fcez8mNSovw-weoX9FlBnsbvm0rIBuKdJR8AOCfb95siqfM9yB1w47IdpdhpetN4pHbxgwnQGJL4mytl2S6ox-vg5BHkuHMydMLeEPDPos',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json, charset=utf-8',
        'referer': 'https://selfservice.mvnu.edu/Student/Courses',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'Cookie': 'cookieyes-consent=consentid:d0owaVVvazVQeldUemZ0aVF5OGs1RVdLM3pPQlBaY24,consent:no,action:,necessary:yes,functional:no,analytics:no,performance:no,advertisement:no,other:no; _fbp=fb.1.1770198687545.390053971848850268; .ColleagueSelfServiceAntiforgery=CfDJ8B0GSlrVWN9Dn-qVRI-h8-3nh21_6fqR70Jj9EgE3drxDz_XVUxrl_KmACAd-BAsWRfixXUgfAY8yX_4RRc1ho4MFKEAkwLR2E9TwBO1up0Lxewc7lRT3FLoM2ioY_JKySTDrO0xELvp9AIbGqL2JFk'
        }

    directory_url = "https://mvnu.edu/academics/on-campus/meet-the-faculty/"
    calendar_url = "https://mvnu.edu/admissions/events/calendar-of-events/"

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            self.parse_course()
        
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)


    def parse_course(self):
        """
        Fetches all subjects from MVNU Self-Service API.
        For each subject:
            - Searches courses
            - Retrieves section details
            - Extracts instructor, dates, location, etc.
        Saves results as course dataset.
        """

        response = requests.get(self.course_url, headers=self.course_headers)
        json_data = response.json()
        for data in json_data['Subjects']:
            subject_code = data.get('Code', '')

            # Search API endpoint
            product_url = "https://selfservice.mvnu.edu/Student/Courses/PostSearchCriteria"

            # Dynamic payload per subject
            product_payload = {
                    "keyword": None,
                    "terms": [],
                    "requirement": None,
                    "subrequirement": None,
                    "courseIds": None,
                    "sectionIds": None,
                    "requirementText": None,
                    "subrequirementText": "",
                    "group": None,
                    "startTime": None,
                    "endTime": None,
                    "openSections": None,
                    "subjects": [subject_code],   # dynamic here
                    "academicLevels": [],
                    "courseLevels": [],
                    "synonyms": [],
                    "courseTypes": [],
                    "topicCodes": [],
                    "days": [],
                    "locations": [],
                    "faculty": [],
                    "onlineCategories": None,
                    "keywordComponents": [],
                    "startDate": None,
                    "endDate": None,
                    "startsAtTime": None,
                    "endsByTime": None,
                    "pageNumber": 3,
                    "sortOn": "None",
                    "sortDirection": "Ascending",
                    "subjectsBadge": [],
                    "locationsBadge": [],
                    "termFiltersBadge": [],
                    "daysBadge": [],
                    "facultyBadge": [],
                    "academicLevelsBadge": [],
                    "courseLevelsBadge": [],
                    "courseTypesBadge": [],
                    "topicCodesBadge": [],
                    "onlineCategoriesBadge": [],
                    "openSectionsBadge": "",
                    "openAndWaitlistedSectionsBadge": "",
                    "subRequirementText": None,
                    "quantityPerPage": 30,
                    "openAndWaitlistedSections": None,
                    "searchResultsView": "CatalogListing"
                }
            product_headers = {
                '__isguestuser': 'true',
                '__requestverificationtoken': 'CfDJ8B0GSlrVWN9Dn-qVRI-h8-3BkjMhteSyX7qjX-V7onefl8rfOL5wMVy5G4B-qe0rnPHK0VeGCm8vn2VL4362Z_liTUCJftKbxV8XBEqfpwmhdLCr5HHYIfyGhs0zVC4Z12aTELdHcbjUXujqjN2fJis',
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'accept-language': 'en-US,en;q=0.9',
                'content-type': 'application/json, charset=UTF-8',
                'origin': 'https://selfservice.mvnu.edu',
                'referer': f'https://selfservice.mvnu.edu/Student/Courses/Search?subjects={subject_code}',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
            # Fetch courses for subject
            product_response = requests.post(product_url, headers=product_headers, json=product_payload)
            product_json = product_response.json()
            for product in product_json.get('CourseFullModels', []):
                course_name = product.get('FullTitleDisplay','')
                class_number = product.get('CourseTitleDisplay','')
                LocationsDisplay = product.get('LocationsDisplay', '')
                course_id = product.get('Id')
                Description = product.get('Description', '').strip()
                MatchingSectionIds = product.get('MatchingSectionIds', 0)

                # If course has sections
                if MatchingSectionIds != []:
                    for sectionIds in MatchingSectionIds:
                        section_url = "https://selfservice.mvnu.edu/Student/Courses/Sections"
                        payload_dict = {
                            "courseId": str(course_id),
                            "sectionIds": [str(sectionIds)]
                        }
                        dynamic_payload = json.dumps(payload_dict)
                        section_headers = {
                            '__isguestuser': 'true',
                            '__requestverificationtoken': 'CfDJ8B0GSlrVWN9Dn-qVRI-h8-05XklktUwauInp293Txyi56EigXbyDs9kUvsbw7B_hKlndNFASR5P9QA_6JNvhCpoBAPuROeERMYQ51LKVvdK03O00gY4RoVyAwRp6_YOwqgs713hDeGFu4sykagGIJ-s',
                            'accept': 'application/json, text/javascript, */*; q=0.01',
                            'accept-language': 'en-US,en;q=0.9',
                            'content-type': 'application/json, charset=UTF-8',
                            'origin': 'https://selfservice.mvnu.edu',
                            'referer': f'https://selfservice.mvnu.edu/Student/Courses/Search?subjects={subject_code}',
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                            }

                        section_response = requests.request("POST", section_url, headers=section_headers, data=dynamic_payload)
                        section_json = section_response.json()
                        # Parse section details
                        for term in section_json.get('SectionsRetrieved', {}).get('TermsAndSections', []):
                            for sec in term.get('Sections', []):
                                section_title = (sec.get('Section', {}).get('SectionNameDisplay', ''))
                                section = section_title.split('-')[-1]
                                StartDateDisplay = sec.get('Section', {}).get('StartDateDisplay', '')
                                EndDateDisplay = sec.get('Section', {}).get('EndDateDisplay', '')
                                course_date = f"{StartDateDisplay} - {EndDateDisplay}"
                                LocationDisplay = sec.get('Section', {}).get('LocationDisplay', '')
                                FacultyDisplay = sec.get('FacultyDisplay', '')
                            
                                self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": 'https://selfservice.mvnu.edu/Student/Courses',
                                    "Course Name": course_name,
                                    "Course Description": Description,
                                    "Class Number": class_number,
                                    "Section": section,
                                    "Instructor": FacultyDisplay,
                                    "Enrollment": "",
                                    "Course Dates": course_date,
                                    "Location": LocationDisplay,
                                    "Textbook/Course Materials": "",
                                })
                else: 
                    # Course without section details
                    self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": 'https://selfservice.mvnu.edu/Student/Courses',
                        "Course Name": course_name,
                        "Course Description": Description,
                        "Class Number": class_number,
                        "Section": "",  
                        "Instructor": "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": LocationsDisplay,
                        "Textbook/Course Materials": "",
                    })
                
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")
        
    def parse_directory(self, response):
        """
        Scrapes faculty directory:
            - Extracts name, title, email, phone
            - Visits individual profile pages when available
        """
     
        rows = []
        people = response.xpath('//div[@class="fl-rich-text"]//ul//li')
        for idx, person in enumerate(people):
            name = ''
            title = ''
            phone = ''
            email = ''
            product_url =  person.xpath('.//a/@href').get('')
            if product_url:
                pro_headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
                product_res = requests.get(product_url,headers=pro_headers)
                product_sel = Selector(text=product_res.text)
                name = product_sel.xpath("//h1/span/text()").get('')
                title1 = product_sel.xpath('//div[@class="fl-module fl-module-rich-text fl-node-8xl0517ztm9p faculty-highlight"]//p/text()').get('')
                title2 = product_sel.xpath('//div[@class="fl-module fl-module-rich-text fl-node-8o7be4psg0vm faculty-highlight"]//p/text()').get('')
                title = title1 + ', ' + title2 if title1 and title2 else title1 or title2 or ''
                phone = product_sel.xpath("//span[@class='fl-heading-text'][contains(., 'Ext') or contains(., '-')][1]/text()").get('')
                email = product_sel.xpath("//span[@class='fl-heading-text'][contains(., '@')]/text()").get('')

                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": product_url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone
                })
            
            else:
                name_parts = person.xpath('.//text()[1]').getall()
                full_text = name_parts[0] if name_parts else ''
                name = full_text.split(',')[0].strip()
                title = ','.join(full_text.split(',')[1:]).strip() if ',' in full_text else ''
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone
                    })

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "campus")

    
    def parse_calendar(self, response):
        """
        Extracts academic events from embedded JavaScript JSON.
        Formats:
            - Term Name (Aug-25)
            - Term Date (25-Aug)
            - Event Title
        """
        json_data = response.xpath('//script[@id="codemine-calendar-js-js-extra"]').get('')
        json_data = (
            json_data
            .replace('<script id="codemine-calendar-js-js-extra">\nvar CMCAL_vars_18 = ', '')
            .replace(';\n</script>', '')
        )

        data = json.loads(json_data)
        for dat in data.get('all_events', []):
            start_date = dat.get('start')
            if not start_date:
                continue

            dt = datetime.fromisoformat(start_date)

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": dt.strftime('%b-%y'),      # Aug-25, Jan-26
                "Term Date": dt.strftime('%d-%b'),      # 25-Aug
                "Term Date Description": (
                    dat.get('title', '')
                    .replace("&#8217;", "’")
                    .replace("&#8211;", "–")
                    .replace("&#8220;", "“")
                    .replace("&#8221;", "”")
                ),
                "_sort_date": dt                         # helper column
            })

        df = pd.DataFrame(self.calendar_rows)

        #SORT FROM 2025 → 2026
        df = df.sort_values("_sort_date").drop(columns=["_sort_date"])
        save_df(df, self.institution_id, "calendar")