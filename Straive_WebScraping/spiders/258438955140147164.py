import re
import scrapy
import pandas as pd
from ..utils import save_df


class WillistonstateSpider(scrapy.Spider):
    name = "williston"

    institution_id = 258438955140147164

    course_url = "https://willistonstate.edu/class-search.aspx"
    directory_url = "https://willistonstate.edu/about/Faculty-and-Staff-Directory/"
    calendar_url = "https://willistonstate.edu/events/Academic-Calendar/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # Entry Point (Mode Based Scraping)
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # Utility Cleaner
    def clean(self, text):
        return re.sub(r"\s+", " ", text).strip() if text else ""

    # Course Scraper
    def parse_course(self, response):
        for course in response.xpath("//div[contains(@class,'class-item-inner')]"):
            subject = course.xpath(".//span[contains(@id,'lblSubject')]/text()").get("")
            title = course.xpath(".//span[contains(@id,'lblClassTitle')]/text()").get("")
            course_name = f"{self.clean(subject)} - {self.clean(title)}"

            desc = course.xpath("preceding::input[contains(@id,'hfLongDesc')][1]/@value").get("")
            class_num = course.xpath("preceding::input[contains(@id,'hfClassNum')][1]/@value").get("")

            index = course.xpath(".//span[contains(@id,'lblSubject')]/@id").re_first(r'_(\d+)$')
            enrolled = response.xpath(f"//input[@id='classSearch_rptrClasses_hfEnrolled_{index}']/@value").get("0")
            cap = response.xpath(f"//input[@id='classSearch_rptrClasses_hfEnrolledCap_{index}']/@value").get("0")
            enrollment = f"{enrolled}/{cap}"

            dates = self.clean(course.xpath(".//div[contains(text(),'-')]/text()").get(""))
            location = self.clean(course.xpath(".//span[contains(@id,'litLoc')]/text()").get(""))
            instructor = self.clean(course.xpath(".//span[contains(@id,'lblInstructor')]/text()").get(""))

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": course_name,
                "Course Description": self.clean(desc),
                "Class Number": class_num,
                "Section": "",
                "Instructor": instructor,
                "Enrollment": enrollment,
                "Course Dates": dates,
                "Location": location,
                "Textbook/Course Materials": ""
            })

        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    # Faculty / Staff Directory
    def parse_directory(self, response):
        for card in response.xpath("//div[contains(@class,'staffcardOuter')]"):
            name = self.clean(card.xpath(".//h4/text()").get(""))
            title = self.clean(card.xpath(".//div[contains(@class,'staffcardTitle')]/text()").get(""))
            department = self.clean(card.xpath(".//div[contains(@class,'staffcardDepartment')]//div/text()").get(""))

            phone = card.xpath("normalize-space(.//div[contains(@class,'staffcardPhoneContainer')])").get("")
            email = card.xpath(".//div[contains(@class,'staffcardEmailContainer')]//a/@href").get("")
            email = email.replace("mailto:", "").strip()

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": f"{title}, {department}".strip(", "),
                "Email": email,
                "Phone Number": phone,
            })

        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    # Academic Calendar
    def parse_calendar(self, response):
        url = response.xpath('//div[@id="MainContentArea_Academic Calendar100_pnlCurrentList"]//li/a/@href').get("")
        if url:
            yield scrapy.Request(response.urljoin(url), callback=self.parse_calendar_details)

    def parse_calendar_details(self, response):
        current_term = None

        blocks = response.xpath("//div[@class='small-12'][strong] | //div[contains(@class,'acaCalendarItem')]")

        for block in blocks:
            term = block.xpath(".//strong/text()").get()
            if term:
                current_term = term.strip()
                continue

            date = self.clean(block.xpath(".//div[contains(@class,'medium-2')]/text()").get(""))
            desc_list = block.xpath(".//div[contains(@class,'medium-10')]//text()").getall()
            desc = " ".join(self.clean(d) for d in desc_list if d.strip())

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": current_term,
                "Term Date": date,
                "Term Date Description": desc
            })

        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")
