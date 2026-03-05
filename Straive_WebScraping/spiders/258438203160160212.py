import re
import io
import csv
import json
import scrapy
import pandas as pd
from dateutil import parser
from datetime import timedelta
from inline_requests import inline_requests
from ..utils import *

class SdccdSpider(scrapy.Spider):
    name="sdccd"
    institution_id = 258438203160160212

    base_url = "https://www.sdcity.edu"
    course_base_url = "https://www.sdccd.edu/students/class-search/search.html"
    course_url = "https://mws-api.sdccd.edu/?term=2263&career=ugrd&_=1768040291281"
    directory_url = "https://www.sdcity.edu/directory/index.aspx"
    calendar_url = "https://www.sdcity.edu/about/calendar.aspx"

    # Updated to collect data from 2026-2028
    calendar_api_url = "https://api.calendar.moderncampus.net/pubcalendar/4dc6bdff-d5c7-4395-b6dc-21a297d0fb43/events?start=2026-01-01&end=2028-12-31&category=717434fd-1324-4277-8287-ad59f566cb78"

    desc_url= "https://www.sdccd.edu/docs/StudentServices/schedule/crsedescr.csv?_=1768040291280"
    
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
    
    calendar_headers = {
        'accept-language': 'en-IN,en-US;q=0.9,en-CA;q=0.8,en;q=0.7,ta;q=0.6,en-GB-oxendict;q=0.5',
        'origin': 'https://www.sdcity.edu',
        'referer': 'https://www.sdcity.edu/',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }
    
    def parse_directory_extract_emails(self, texts):
        emails = []
        
        for t in texts:
            found = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", t)
            emails.extend(found)
        
        return list(dict.fromkeys(emails))

    def parse_directory_decode_cfemail(self, encoded):
        r = int(encoded[:2], 16)
        email = ''.join([chr(int(encoded[i:i+2], 16) ^ r) for i in range(2, len(encoded), 2)])
        return email
    
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url= self.calendar_api_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_api_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_api_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_api_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
    
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
        
        # This list will hold final course records
        course_data = []

        # Course descriptions are NOT inside the API response,
        # so we load them separately from a CSV file.
        desc_res = yield scrapy.Request(url=self.desc_url, headers=self.headers)
        reader = csv.DictReader(io.StringIO(desc_res.text))
        
        # Dictionary to map CRSE_ID → Course Description
        course_dict = {}

        for row in reader:
            crse_id = row.get("CRSE_ID", "").strip()
            desc = row.get("CRSEDESCR", "").strip()
            
            # Store only valid course IDs
            if crse_id:
                course_dict[crse_id] = desc
        
        # Parse API response
        data = json.loads(response.text)
        
        # Actual course list is nested deep inside JSON
        courses = data.get("data", {}).get("query", {}).get("rows", [])

        # loop each course record
        for course in courses:
            
            # Only collect City College records
            campus_code = course.get("CAMPUS", "")
            if campus_code != "CITY":
                continue
            
            # Skip if not marked for schedule print
            if course.get("SCHEDULE_PRINT") != "Y":
                continue

            #  PARSE MEETING INFO 
            meeting_info = course.get("MEETINGINFO", "")
            instructors = []
            locations = []
            days_list = []
            times_list = []
            rooms = []
            
            # MEETINGINFO is a string with <br/> separators
            if meeting_info:
                meetings = meeting_info.split("<br/>")
                for meeting in meetings:
                    if not meeting.strip():
                        continue
                    
                    # Each meeting is pipe-separated
                    parts = meeting.split("|")
                    
                    # Ensure required fields exist
                    if len(parts) >= 11:
                        room = parts[1].strip() if len(parts) > 1 else "TBA"
                        days = parts[2].strip() if len(parts) > 2 else "TBA"
                        start_time = parts[3].strip() if len(parts) > 3 else ""
                        end_time = parts[4].strip() if len(parts) > 4 else ""
                        instructor_name = parts[6].strip() if len(parts) > 6 else ""
                        show_instructor = parts[10].strip().upper() if len(parts) > 10 else ""
                        
                        rooms.append(room)
                        days_list.append(days)
                        
                        # Combine start & end time
                        if start_time and end_time:
                            times_list.append(f"{start_time} - {end_time}")
                        else:
                            times_list.append("TBA")
                        
                        # Only show instructor if allowed
                        if show_instructor == "Y" and instructor_name:
                            instructors.append(instructor_name)

            # Remove duplicates
            instructors = list(dict.fromkeys(instructors))
            rooms = list(dict.fromkeys(rooms))
            days_list = list(dict.fromkeys(days_list))
            times_list = list(dict.fromkeys(times_list))

            # COURSE NAME 
            subject = course.get("SUBJECT", "")
            catalog_nbr = course.get("CATALOG_NBR", "")
            course_title = course.get("CRSE_NAME", "")
            
            # Handle honors designation
            if course.get("RQMNT_DESIGNTN") == "HNRS":
                course_title = f"Honors {course_title}"
            
            course_name = f"{subject} {catalog_nbr} - {course_title}"

            #  LOCATION (Modality) 
            location_code = course.get("Location") or course.get("LOCATION", "")
            location_map = {
                "HYFLEX": "Hyflex-Optional In-Person",
                "OFF": "Off Campus",
                "ONCAMPUS": "On Campus",
                "ONLINE": "Fully Online",
                "ONLINESYNC": "Online Live",
                "PT-ONLINE": "Partially Online"
            }
            location = location_map.get(location_code, location_code)
            
            # City College campus name
            campus_map = {
                "CITY": "City College",
                "MESA": "Mesa College",
                "MIRA": "Miramar College"
            }
            campus_name = campus_map.get(campus_code, campus_code)
            
            # Combine location with room info
            if rooms and rooms[0] != "TBA":
                location_full = f"{location} - {campus_name} ({', '.join(rooms)})"
            else:
                location_full = f"{location} - {campus_name}"

            #  COURSE DATES 
            start_date = course.get("START_DT", "")
            end_date = course.get("END_DT", "")
            course_dates = ""
            
            if start_date and end_date:
                try:
                    start = parser.parse(start_date).strftime("%m/%d/%y")
                    end = parser.parse(end_date).strftime("%m/%d/%y")
                    course_dates = f"{start} - {end}"
                except:
                    course_dates = f"{start_date} - {end_date}"

            #  TEXTBOOK URL 
            books_value = str(course.get("BOOKS", "")).strip()
            strm = course.get("STRM", "")
            class_nbr = course.get("CLASS_NBR", "")
            textbook_url = ""
            
            # Format term for bookstore URL
            def format_book_term(term_code):
                if term_code and len(str(term_code)) == 4:
                    year = "20" + str(term_code)[1:3]
                    term_num = str(term_code)[3]
                    term_name = {"3": "Spring", "5": "Summer", "7": "Fall"}.get(term_num, "Term")
                    return f"{term_name}+{year}"
                return term_code
            
            if books_value in ["1", "2"]:
                book_term = format_book_term(strm)
                bookstore_campus = "CITY"
                
                section = class_nbr
                # Add -WEB suffix for online courses
                if location_code == "ONLINE" and section:
                    section = f"{section}-WEB"
                
                textbook_url = (
                    f"https://www.bookstore.sdccd.edu/{bookstore_campus}/textbook_express/"
                    f"get_txtexpress.asp?remote=1&student=&ref=2023&term={bookstore_campus}+{book_term}"
                    f"&dept={subject}&course={catalog_nbr}&section={section}&getbooks=display+books"
                )

            #  ENROLLMENT
            enrl_tot = course.get("ENRL_TOT", 0)
            enrl_cap = course.get("ENRL_CAP", 0)
            enrollment = f"{enrl_tot}/{enrl_cap}"

            #  BUILD ITEM 
            item = {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_base_url,
                "Course Name": course_name,
                "Course Description": course_dict.get(course.get("CRSE_ID", ""), ""),
                "Class Number": course.get("CLASS_NBR", ""),
                "Section": course.get("CLASS_SECTION", ""),
                "Instructor": "; ".join(instructors) if instructors else "",
                "Enrollment": enrollment,
                "Course Dates": course_dates,
                "Location": location_full,
                "Textbook/Course Materials": textbook_url
            }
            
            course_data.append(item)

        # Save to DataFrame
        course_df = pd.DataFrame(course_data)
        save_df(course_df, self.institution_id, "course")

    @inline_requests 
    def parse_directory(self, response):
        """
        Parse directory data.
        FIXES:
        - Extract program details and add to title
        - Improve email extraction
        - Better phone number parsing for faculty
        - Handle all record types correctly
        
        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """

        # This list will store final directory records
        final_data = []

        # Collect all links from the main directory table
        # Each link opens a department / staff page
        links = response.xpath('//table//tr/td//a/@href').getall()
        
        # Loop through each directory link
        for link in links:
            
            # Skip office-only links (not staff)
            if "office" in link.lower():
                continue
            
            # Convert relative URL to full URL
            full_url = response.urljoin(link)
            res = yield scrapy.Request(full_url)

            # Read section title (Faculty / Classified / Admin etc.)
            section_title = res.xpath('//h1/text() | //h2/text()').get(default="").strip().lower()
            
            # Each staff record is inside table rows
            rows = res.xpath('//table//tr')

            # BASE PHONE (used by Classified section)
            base_phone = ""
            header_texts = res.xpath('//th[contains(., "Phone")]//text()').getall()
            header_texts = [t.strip() for t in header_texts if t.strip()]

            # Extract base phone like "(619) 388"
            for t in header_texts:
                m = re.search(r"\(\d{3}\)\s*\d{3}", t)
                if m:
                    digits = re.findall(r"\d+", m.group())
                    if len(digits) >= 2:
                        base_phone = f"({digits[0]}) {digits[1]}"
                    break
            
            # LOOP THROUGH EACH PERSON ROW
            for row in rows:
                
                # Default empty values
                name = ""
                title = ""
                phone = ""
                email = ""
                
                # Extract table cells (<td>)
                cols = row.xpath('.//td')
                
                # Skip rows without enough columns
                if len(cols) < 2:
                    continue

                # EMAIL logic
                emails = []

                # 1️⃣ MAILTO EMAILS (normal emails)
                mailtos = row.xpath('.//a[contains(@href,"mailto")]/@href').getall()
                for e in mailtos:
                    e = e.replace("mailto:", "").strip()
                    if e:
                        emails.append(e)

                # 2️⃣ CLOUDFLARE EMAILS (MOST IMPORTANT)
                # Cloudflare hides real email in data-cfemail
                cfemails = row.xpath('.//*[contains(@class,"__cf_email__")]/@data-cfemail').getall()
                for cf in cfemails:
                    # Decode using XOR logic
                    decoded = self.parse_directory_decode_cfemail(cf)
                    if decoded:
                        emails.append(decoded)

                # plain text - search in ALL cell text including <a> tags
                all_text = row.xpath('.//td//text()').getall()
                plain = self.parse_directory_extract_emails(all_text)
                emails.extend(plain)

                # Remove duplicates and junk values
                emails = [e for e in emails if "protected" not in e.lower()]
                emails = list(dict.fromkeys(emails))
                email = ", ".join(emails)

                # Helper to clean text from <td>
                def clean(sel):
                    return " ".join(t.strip() for t in sel.xpath('.//text()').getall() if t.strip())

                #  Administration 
                if "administration" in section_title:
                    title = clean(cols[0])
                    name  = clean(cols[1])
                    phone = clean(cols[3]) if len(cols) > 3 else ""

                #  Faculty 
                elif "faculty" in section_title:
                    name  = clean(cols[0])
                    title = clean(cols[1])
                    phone = clean(cols[4]) if len(cols) > 4 else ""
                    
                    # Normalize phone formatting
                    if phone:
                        phone = re.sub(r"\s*-\s*", "-", phone)   # normalize dash
                        phone = re.sub(r"\s+", " ", phone).strip()

                #  Classified Professionals 
                elif "classified" in section_title:
                    name = f"{clean(cols[1])} {clean(cols[0])}"
                    t1 = clean(cols[2])
                    t2 = clean(cols[3])
                    
                    # Avoid duplicate titles
                    title = f"{t1}, {t2}" if t1 and t2 and t1.lower() != t2.lower() else (t1 or t2 or "")
                    
                    extra_phone = clean(cols[5]) if len(cols) > 5 else ""

                    if base_phone and extra_phone:
                        if "/" in extra_phone:
                            parts = [p.strip() for p in extra_phone.split("/") if p.strip()]
                            phone = ", ".join(f"{base_phone} {p}" for p in parts)
                        else:
                            phone = f"{base_phone} {extra_phone}"
                    else:
                        phone =  ""

                #  Fallback 
                else:
                    continue

                # Remove extra spaces
                name = name.strip()
                title = title.strip()
                email = email.strip()
                phone = phone.strip()
                
                # Skip rows with no meaningful data
                if not (name or title or email or phone):
                    continue
                
                # Save final record
                final_data.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": full_url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone
                })

        df = pd.DataFrame(final_data)
        save_df(df, self.institution_id, "campus")


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
        
        # List to store all calendar records
        calendar_data = []

        # Calendar API returns JSON, not HTML
        try:
            events = json.loads(response.text)
        except Exception as e:
            # If JSON fails to parse, log error and stop
            self.logger.error(f"JSON parse error: {e}")
            return

        # LOOP THROUGH EACH EVENT
        for event in events:
            
            # Event title (used as Term Name)
            title = event.get("title", "").strip()

            #  SAFE DESCRIPTION 
            # Description may contain newlines / extra spaces
            raw_desc = event.get("descriptionText", "") or ""
            desc = re.sub(r"\s+", " ", raw_desc).strip()

            # Calendar API may use different keys
            start_date = (
                event.get("start")
                or event.get("startDate")
                or event.get("startDatetime")
                or ""
            )

            end_date = (
                event.get("end")
                or event.get("endDate")
                or event.get("endDatetime")
                or ""
            )

            # This will be the final formatted date string
            term_date = ""

            try:
                if start_date and end_date:
                    
                    # Convert string → datetime object
                    s = parser.parse(start_date)
                    e = parser.parse(end_date)

                    # If time is 00:00, subtract 1 day
                    if e.hour == 0 and e.minute == 0:
                        e = e - timedelta(days=1)

                    # Case 1: Single-day event
                    if s.date() == e.date():
                        term_date = s.strftime("%B %d, %Y")
                        
                    # Case 2: Same month & year
                    elif s.month == e.month and s.year == e.year:
                        term_date = f"{s.strftime('%B %d')} – {e.strftime('%d, %Y')}"
                    
                    # Case 3: Different month/year
                    else:
                        term_date = f"{s.strftime('%B %d, %Y')} – {e.strftime('%B %d, %Y')}"
            
            except Exception as e:
                self.logger.warning(f"Date parse error: {start_date}, {end_date} | {e}")

            calendar_data.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": title,
                "Term Date": term_date,
                "Term Date Description": desc
            })

        calendar_df = pd.DataFrame(calendar_data)
        save_df(calendar_df, self.institution_id, "calendar")
        