import io
import scrapy
import pdfplumber
import pandas as pd
import unicodedata
from ..utils import *

def safe_text(sel, xpath):
    return sel.xpath(xpath).get(default="").strip()

class FhtcSpider(scrapy.Spider):

    name = "fhtc"
    institution_id = 258440800344500178

    course_rows = []
    course_url = "https://fhtc.edu/admissions/course-schedule/"
    course_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'priority': 'u=0, i',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
    }

    directory_url = "https://fhtc.edu/wp-admin/admin-ajax.php"
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

    calendar_url = "https://www2.fhtc.edu/assets/pub/academic-calendar.pdf"

    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url,method="POST",headers=self.directory_headers,body=self.directory_payload, callback=self.parse_directory, dont_filter=True)

        # If only calendar data is required
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url,method="POST",headers=self.directory_headers,body=self.directory_payload, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url,method="POST",headers=self.directory_headers,body=self.directory_payload, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # All three (default)
        else:
            yield scrapy.Request(self.course_url,headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url,method="POST",headers=self.directory_headers,body=self.directory_payload, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)



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
        
        blocks = response.xpath('//div[@id="course-results"]/div')
        rows=[]

        for block in blocks:
            title = block.xpath('./div[@class="description"]/h4/text()').get("").strip()
            class_number = title.split(" ")[0].strip()
            desc = block.xpath('./div[@class="description"]/p/text()').get("").strip()
            decode_desc = unicodedata.normalize("NFKD", desc).encode("ascii", "ignore").decode()
            course_desc = re.sub(r"\r\n","",decode_desc)
            blogs = block.xpath('./div[contains(@class,"course-item")]')

            for blog in blogs:
                section = blog.xpath('./div[@class="section-table"]/span[@class="course-section"]/text()').get('').split(":")[-1].strip()
                enrollment = blog.xpath('./div[@class="section-table"]/span[@class="course-seats"]/text()').get('').strip().split(":")[-1].strip()
                tables = blog.xpath('./div[@class="table-format-grid"]/div[@class="grid-body"]')

                for table in tables:
                    instructor = safe_text(table, './span[6]/text()')
                    course_dates = safe_text(table, './span[3]/text()')
                    location = safe_text(table, './span[2]/text()')

                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Course Name": title or "",
                        "Course Description": course_desc or "",
                        "Class Number": class_number or "",
                        "Section": section or "",
                        "Instructor": instructor,
                        "Enrollment": enrollment or "",
                        "Course Dates": course_dates,
                        "Location": location,
                        "Textbook/Course Materials": '',
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
         
        blocks = response.xpath('//div[@class="employee-wrapper"]')
        rows=[]
        for block in blocks:

            title = block.xpath('./div[@class="instructor-info1"]/div/p/text()').getall()
            title = ", ".join([t.strip() for t in title if t.strip()])
            rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": "https://fhtc.edu/about/employee-directory/",
                    "Name": block.xpath('./div[@class="instructor-info1"]/div/h5/text()').get("").strip(),
                    "Title":title,
                    "Email":block.xpath('./div[@class="instructor-info1"]/div[contains(@class,"info-dropdown")]/p/a/@data-email').get("").strip(),
                    "Phone Number": block.xpath('./div[@class="instructor-info1"]/div[contains(@class,"info-dropdown")]/p/a[@class="txt_w"]/text()').get("").strip(),
                })
            
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")


    def parse_calendar(self, response):
        """
        Parses an academic calendar PDF and extracts term dates and descriptions.

        This method processes a PDF response using pdfplumber, extracts text
        from specific column regions, and identifies academic term months,
        dates, and associated event descriptions.

        Workflow:
        ---------
        1. Opens the PDF from the Scrapy response body.
        2. Iterates through each page of the PDF.
        3. Crops the page into four vertical columns.
        4. Processes only the 2nd and 4th columns (based on layout structure).
        5. Extracts text line-by-line.
        6. Detects:
            - Month names (JANUARY–DECEMBER) to determine Term Name.
            - Date patterns (e.g., "1", "5-9").
            - Event descriptions associated with dates.
        7. Maps months to the appropriate academic year using `month_to_year`.
        8. Cleans and formats:
            - Date ranges (e.g., "5-9" → "5 - 9")
            - Removes invalid comma-separated dates (e.g., "5,16")
            - Removes numeric prefixes accidentally captured in descriptions.
        9. Appends structured records into a list of dictionaries.
        10. Saves the extracted data as a DataFrame using `save_df()`.

        Extracted Fields:
        -----------------
        - Cengage Master Institution ID
        - Source URL
        - Term Name (e.g., "JANUARY-2026")
        - Term Date (formatted as "MONTH-date")
        - Term Date Description (event description)

        Args:
        -----
        response (scrapy.http.Response):
            The PDF response object containing the academic calendar.

        Returns:
        --------
        None
            The method saves the extracted calendar data but does not return a value.

        Notes:
        ------
        - Assumes a fixed 4-column PDF layout.
        - Month-to-year mapping is hardcoded for the 2025–2026 academic year.
        - Lines containing "ACADEMIC" or partial year headers are ignored.
        - Only valid date/description pairs are stored.
        """

        with pdfplumber.open(io.BytesIO(response.body)) as pdf:
            rows = []  # Store rows for later use
            current_term = ""
            current_date = ""
            current_desc = ""

            # Define a dictionary to map months to the term year
            month_to_year = {
                "JANUARY": "2026", "FEBRUARY": "2026", "MARCH": "2026", "APRIL": "2026", 
                "MAY": "2026", "JUNE": "2026", "JULY": "2026", "AUGUST": "2026",
                "SEPTEMBER": "2025", "OCTOBER": "2025", "NOVEMBER": "2025", "DECEMBER": "2025"
            }

            for page in pdf.pages:
                width, height = page.width, page.height
                columns = [
                    page.crop((0, 0, width * 0.20, height)),  # First column (0-20%)
                    page.crop((width * 0.22, 0, width * 0.48, height)),  # Second column (20-50%)
                    page.crop((width * 0.48, 0, width * 0.70, height)),  # Third column (50-70%)
                    page.crop((width * 0.70, 0, width, height))  # Fourth column (70-100%)
                ]

                for col in columns[1::2]:  # Process the second and fourth columns (as per your requirement)
                    text = col.extract_text()
                    if not text:
                        continue
                    
                    lines = text.split("\n")
                    for line in lines:
                        line = line.strip()
                        
                        # Skip irrelevant lines
                        if "ACADEMIC" in line or "2025-202" in line:
                            continue
                        
                        # Match for months (term_name) - case insensitive
                        term_month_match = re.compile(r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)', re.I)
                        term_month = term_month_match.search(line)
                        
                        if term_month:
                            # Get the month in uppercase and map to the correct year
                            month = term_month.group(0).upper()
                            current_term = f"{month}-{month_to_year.get(month, '2025')}"  # Add the year based on the month
                        
                        # Match the date (term_date) and description (term_desc)
                        date_desc_match = re.compile(r'(\d{1,2}[-\d{1,2}]{0,1})\s*(.*)', re.I)
                        date_desc = date_desc_match.search(line)
                        
                        if date_desc:
                            current_date = date_desc.group(1).strip()  # Capture the date part (e.g., "1", "5-9")
                            current_desc = date_desc.group(2).strip()  # Capture the description part (e.g., "No Classes", "First Day of Classes")

                            # Skip rows where date contains a comma (i.e., "5,16", "2,23", "9,30")
                            if ',' in current_date:
                                continue
                            
                            # If no description found, set a default description
                            if not current_desc:
                                continue
                                
                            # Correctly format date ranges (e.g., "NOVEMBER-24 to,25" -> "NOVEMBER-24 to 25")
                            current_date = current_date.replace(",", "").replace("to,", "to")

                            # Handle the "1-" or "5-" date formats correctly (if needed)
                            if '-' in current_date:
                                # Ensure the range is properly captured (e.g., "5-9" should be handled as a range)
                                current_date = current_date.replace('-', ' - ')
                            
                            # **Extract number from description and move it to term_date if necessary**
                            number_in_desc = re.match(r'(\d+)', current_desc)  # Check if the description starts with a number
                            if number_in_desc:
                                # If the description contains a number at the start, we move that to `Term Date`
                                current_date = number_in_desc.group(1)  # Extract the number into the term_date
                                current_desc = current_desc.replace(number_in_desc.group(1), "").strip()  # Remove number from description

                            # Only append the row if we have a valid date and description
                            if current_date and current_desc:
                                rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Term Name": current_term,
                                    "Term Date": f"{month}-{current_date}",
                                    "Term Date Description": current_desc
                                })

            # After processing, you can save the data (e.g., into a DataFrame or CSV)
            if rows:
                calendar_df = pd.DataFrame(rows)
                save_df(calendar_df, self.institution_id, "calendar")


    