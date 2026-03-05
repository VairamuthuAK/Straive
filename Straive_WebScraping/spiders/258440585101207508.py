import re
import io
import scrapy
import pandas as pd
import pdfplumber
from ..utils import save_df


class HpuSpider(scrapy.Spider):
    name = "hpu"
    institution_id = 258440585101207508

    course_url = "https://hpu.edu/military-and-veterans/military-campus/files/scheduletop.pdf"
    directory_url = "https://www.hpu.edu/faculty/index.html"
    calendar_url = "https://www.hpu.edu/registrar/academic-calendar.html"


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback= self.parse_calendar, dont_filter=True)


    def parse_course(self, response):
        """
        Parse course data from HPU Military Campus PDF.
    
        This PDF contains THREE logical sections:
    
        1) Regular Course Schedule (Session 8A / 8B)
        2) GENERAL EDUCATION SCHEDULE
        3) COURSE DESCRIPTION (catalog text at the end)
    
        IMPORTANT DESIGN:
        - We FIRST parse all course rows (no descriptions yet)
        - We THEN parse course descriptions into a dictionary
        - We FINALLY map descriptions to rows using SUBJECT + NUMBER
        """
        rows = []
        current_session = None          # Tracks 8A / 8B
        session_dates = {}              # 8A → date range, 8B → date range
        in_general_ed = False            # Are we in General Education section?
        in_description_section = False   # Are we in Course Description section?
    
        # Used to detect regular course rows
        COURSE_ROW_PATTERN = re.compile(r"^\d{4,5}\s+")
        # Example:
        # {
        #   "ACCT2000": "An introduction to accounting principles...",
        #   "ARTS1000": "An introductory visual arts course..."
        # }
        description_map = {}
    
        current_desc_code = None
        current_desc_lines = []
    
        # Regex to detect description headers
        # Example: ACCT2000 - Principles of Accounting I: Description...
        DESC_HEADER_PATTERN = re.compile(r"^([A-Z]{2,5})(\d{4})\s*-\s*[^:]+:\s*(.*)")
    
        with pdfplumber.open(io.BytesIO(response.body)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()

                if not text:
                    continue
    
                for raw_line in text.split("\n"):
                    line = re.sub(r"\s+", " ", raw_line).strip()

                    if not line:
                        continue
    
                    # ENTER COURSE DESCRIPTION SECTION        
                    if line == "COURSE DESCRIPTION":
                        in_description_section = True
                        in_general_ed = False
                        current_session = None
                        continue
    
                    # PARSE COURSE DESCRIPTION SECTION
                    if in_description_section:
                        match = DESC_HEADER_PATTERN.match(line)
    
                        if match:
                            # Save previous description
                            if current_desc_code:
                                description_map[current_desc_code] = " ".join(
                                    current_desc_lines
                                ).strip()
    
                            subject = match.group(1)
                            number = match.group(2)
                            first_text = match.group(3)
    
                            current_desc_code = f"{subject}{number}"
                            current_desc_lines = [first_text]
    
                        else:
                            # Continuation of description
                            if current_desc_code:
                                current_desc_lines.append(line)
    
                        continue  # Do not fall into row parsing
    
                    # GENERAL EDUCATION SECTION START / END
                    if line == "GENERAL EDUCATION SCHEDULE":
                        in_general_ed = True
                        current_session = None
                        continue
    
                    # SESSION HEADERS (REGULAR COURSES)
                    if not in_general_ed and line.startswith("Session: 8A"):
                        current_session = "8A"
                        session_dates["8A"] = line.split("(", 1)[1].rstrip(")")
                        continue
    
                    if not in_general_ed and line.startswith("Session: 8B"):
                        current_session = "8B"
                        session_dates["8B"] = line.split("(", 1)[1].rstrip(")")
                        continue
    
                    # GENERAL EDUCATION ROWS
                    if in_general_ed:
    
                        if (
                            line.startswith("GEN ED TYPE")
                            or line.startswith("General Ed:")
                            or line.startswith("M=Monday")
                        ):
                            continue
    
                        tokens = line.split(" ")
    
                        # Find CRN (4 digits AFTER 8A / 8B)
                        crn_index = None
                        for i, t in enumerate(tokens):
                            if (
                                t.isdigit()
                                and len(t) == 4
                                and i > 0
                                and tokens[i - 1] in {"8A", "8B"}
                            ):
                                crn_index = i
                                break
    
                        if crn_index is None:
                            continue
    
                        try:
                            subject = tokens[crn_index + 1]
                            course_num = tokens[crn_index + 2]
                            section = tokens[crn_index + 3]
                        except IndexError:
                            continue
    
                        # ASSUMPTION: location is last token
                        tokens_after_sec = tokens[crn_index + 4:]
    
                        if len(tokens_after_sec) > 1:
                            location = tokens_after_sec[-1]
                            title_tokens = tokens_after_sec[:-1]

                        else:
                            location = ""
                            title_tokens = tokens_after_sec
    
                        # Remove day / time pollution
                        clean_title = []
                        for t in title_tokens:
                            if t in {"M", "T", "W", "R", "F"} or t.isdigit():
                                break
                            clean_title.append(t)
    
                        course_name = " ".join(clean_title)
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Course Name": course_name,
                            "Course Description": "", 
                            "Class Number": f"{subject} {course_num}",
                            "Section": section,
                            "Instructor": "",
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": location,
                            "Textbook/Course Materials": "",
                        })
    
                        continue
    
                    # REGULAR COURSE ROWS
                    if current_session not in {"8A", "8B"}:
                        continue
    
                    if (
                        line.startswith("CRN SUBJ NUM")
                        or line.startswith("M=Monday")
                        or line.startswith("Created by")
                    ):
                        continue
    
                    if not COURSE_ROW_PATTERN.match(line):
                        continue
    
                    tokens = line.split(" ")
    
                    subject = tokens[1]
                    course_num = tokens[2]
                    section = tokens[3]
    
                    credits_index = None
                    for i, t in enumerate(tokens):
                        if t.isdigit() and i > 4:
                            credits_index = i
                            break
    
                    if credits_index is None:
                        continue
    
                    course_name = " ".join(tokens[4:credits_index])
    
                    if tokens[credits_index + 1] == "-":
                        instructor = " ".join(tokens[credits_index + 2:-1])
                        location = tokens[-1]
                        
                    else:
                        instructor = " ".join(tokens[credits_index + 5:-1])
                        location = tokens[-1]
    
                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Course Name": course_name,
                        "Course Description": "", 
                        "Class Number": f"{subject} {course_num}",
                        "Section": section,
                        "Instructor": instructor,
                        "Enrollment": "",
                        "Course Dates": session_dates.get(current_session, ""),
                        "Location": location,
                        "Textbook/Course Materials": "",
                    })

        # SAVE LAST DESCRIPTION BLOCK
        if current_desc_code:
            description_map[current_desc_code] = " ".join(current_desc_lines).strip()
    
        # MAP DESCRIPTIONS TO ROWS
        for row in rows:
            key = row["Class Number"].replace(" ", "")
            row["Course Description"] = description_map.get(key, "")
    
        course_df = pd.DataFrame(rows)
        save_df(course_df, self.institution_id, "course")
    
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
        rows = []   
        for tr in response.css("tr.deptRow"):
            name = tr.css("td a::text").get(default="").strip()
    
            # <td> containing name, designation, and department
            profile_td = tr.css("td")[1]

            # Get only text nodes that are NOT inside <a>
            text_parts = profile_td.xpath('./text()').getall()
            text_parts = [t.strip() for t in text_parts if t.strip()]

            # First line is designation, second is department
            designation = text_parts[0] if len(text_parts) > 0 else ""
            department = text_parts[1] if len(text_parts) > 1 else ""

            # Final title
            title = f"{designation} {department}".strip()
    
            # Email
            email = tr.css('a[href^="mailto:"]::attr(href)').get()
            email = email.replace("mailto:", "").strip() if email else ""
    
            # Phone Number
            phone = tr.xpath(
                './/i[contains(@class,"fa-phone")]/following-sibling::text()'
            ).get()
            if phone:
                phone = phone.strip()
                if phone.lower() == "n/a" or not any(ch.isdigit() for ch in phone):
                    phone = ""
            else:
                phone = ""
            
            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })
    
        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        This function extracts academic calendar data from the page.
    
        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        # This list will store every calendar row we extract
        # The page has 3 big accordion sections:
        # 1) Academic Calendars
        # 2) Future Calendars
        # 3) Downloadable Calendars (we skip this due to same data)
        rows = []
        for wrapper in response.css("div.accordion.wrapper"):
            # "Academic Calendars", "Future Calendars", "Downloadable Calendars"
            section_title = wrapper.css("h3::text").get(default="").strip()
            # Skip Downloadable Calendars section
            if section_title == "Downloadable Calendars":
                continue
    
            for acc in wrapper.css("div.acc-row"):
                # Example: "Spring 2026 16-Week Term"
                base_term_name = acc.css("h3 label::text").get(default="").strip()

                if not base_term_name:
                    continue
    
                # This variable keeps track of the CURRENT term name.
                current_term_name = base_term_name
                # Each term contains a table.
                for tr in acc.css("table tr"):
    
                    # Extract all <td> cells from the row
                    tds = tr.css("td")
    
                    # If the row has no <td>, it is useless.
                    if not tds:
                        continue
    
                    # Skip header and footnote rows entirely
                    if tds[0].attrib.get("colspan") == "4":
                        continue

                    # Normal event rows always have at least 2 columns
                    if len(tds) < 2:
                        continue
    
                    # First column = description
                    description = tds[0].xpath("string(.)").get("").strip()
    
                    # Second column = start date
                    # Example: "Monday, March 9, 2026"
                    start_date = tds[1].xpath("string(.)").get("").strip()
    
                    # Third and fourth columns may contain date ranges
                    dash = ""
                    end_date = ""
    
                    if len(tds) > 2:
                        dash = tds[2].xpath("string(.)").get("").strip()
    
                    if len(tds) > 3:
                        end_date = tds[3].xpath("string(.)").get("").strip()
    
                    # If description or start date is missing,
                    # the row is incomplete, so skip it.
                    if not description or not start_date:
                        continue
    
                    # If there is a dash (-), this means a date range.
                    if dash == "-" and end_date:
                        term_date = f"{start_date} - {end_date}"
                    else:
                        # Otherwise, it's a single date.
                        term_date = start_date
    
                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": current_term_name,
                        "Term Date": term_date,
                        "Term Date Description": description,
                    })
    
        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "calendar")
    
    
    