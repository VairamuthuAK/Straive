import re
import html
import scrapy
import requests
import urllib.parse
import pandas as pd
from ..utils import *
from parsel import Selector
from inline_requests import inline_requests


class CentralazSpider(scrapy.Spider):
    name = "centralaz"
    institution_id = 258444127929133017
    
    # Indian region website is not opening, so I used a US region proxy.
    proxy ={'proxy':'Enter your proxy here'}
    proxies = {
        'http': 'Enter your proxy here',
        'https': 'Enter your proxy here',
        }
    
    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://sisportal-100361.campusnexus.cloud/CMCPortal/Common/CourseSchedule.aspx"
    directory_source_url = "https://centralaz.edu/directorytest/"
    calendar_url = "https://centralaz.edu/academic-cal/"
    
    # Headers used for course schedule page HTTP requests
    course_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://sisportal-100361.campusnexus.cloud',
        'referer': 'https://sisportal-100361.campusnexus.cloud/CMCPortal/Common/CourseSchedule.aspx',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    }
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, meta=self.proxy, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, meta=self.proxy, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, meta=self.proxy, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, meta=self.proxy, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, meta=self.proxy, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, meta=self.proxy, dont_filter=True)
        
        # # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, meta=self.proxy, dont_filter=True)


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
        # Initialize list to store parsed course records
        course_rows = []
        
        # Extract available academic terms
        terms = response.xpath('//select[@id="_ctl0_PlaceHolderMain__ctl0_cbTerm"]/option/@value').getall()
        
        # Extract subject names and corresponding input IDs
        subject_names = response.xpath('//span[@id="chbSubjects"]//label/text()').getall()
        subject_ids = response.xpath('//span[@id="chbSubjects"]//input/@name').getall()
        
        # Extract ASP.NET hidden form state values from initial page
        VIEWSTATE_text_main  = urllib.parse.quote(response.xpath('//input[@id="__VIEWSTATE"]/@value').get('')).strip()
        VIEWSTATEGENERATOR_text_main  = urllib.parse.quote(response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip())
        EVENTVALIDATION_text_main  = urllib.parse.quote(response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get('')).strip()
        
        # Iterate through each academic term
        for term in terms:
            print(term)
            
            # Iterate through each subject
            for sub_name, sub_id in list(zip(subject_names, subject_ids)):
                print(sub_name)
                
                # URL-encode subject input ID
                sub_id = urllib.parse.quote(sub_id)
                
                # Build POST payload to submit course search
                payload = f'__EVENTTARGET=_ctl0%24PlaceHolderMain%24_ctl0%24btnSearch&__EVENTARGUMENT=&__VIEWSTATE={VIEWSTATE_text_main}&_ctl0%3Apagetitle%3AhfShowLinkText=Show%20Quick%20Links...&_ctl0%3Apagetitle%3AhfHideLinkText=Hide%20Quick%20Link...&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbCampus=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_0=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_1=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_2=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_3=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_4=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_5=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_6=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_7=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_8=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_9=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbTerm={term}&{sub_id}=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtKeyword=&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbLowTime=0&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbHighTime=23&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkMorning=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkAfternoon=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkEvening=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbInstructor=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtInstuSearch=&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbDeliveryMethod=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtDelMethSearch=&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkMo=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkTu=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkWe=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkTh=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkFr=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkSa=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkSu=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AhdnShowMore=false&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtIsShowMore=0&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtCourseListResult=&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl2%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl2%3AtxtHidClassSchedID=176056&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl2%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl3%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl3%3AtxtHidClassSchedID=176057&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl3%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl4%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl4%3AtxtHidClassSchedID=176058&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl4%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl5%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl5%3AtxtHidClassSchedID=176150&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl5%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl6%3AtxtHidCourseId=ACC121&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl6%3AtxtHidClassSchedID=176059&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl6%3AtxtHidSection=Income%20Tax%20Fundamentals&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl7%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl7%3AtxtHidClassSchedID=176060&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl7%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl8%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl8%3AtxtHidClassSchedID=176061&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl8%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl9%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl9%3AtxtHidClassSchedID=176062&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl9%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl10%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl10%3AtxtHidClassSchedID=177354&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl10%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl11%3AtxtHidCourseId=ACC202&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl11%3AtxtHidClassSchedID=176063&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl11%3AtxtHidSection=Managerial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl12%3AtxtHidCourseId=ACC202&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl12%3AtxtHidClassSchedID=176064&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl12%3AtxtHidSection=Managerial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl2%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl2%3AtxtHidClassSchedID=176056&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl2%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl3%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl3%3AtxtHidClassSchedID=176057&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl3%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl4%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl4%3AtxtHidClassSchedID=176058&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl4%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl5%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl5%3AtxtHidClassSchedID=176150&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl5%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl1%3AdgSubGroup%3A_ctl2%3AtxtHidCourseId=ACC121&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl1%3AdgSubGroup%3A_ctl2%3AtxtHidClassSchedID=176059&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl1%3AdgSubGroup%3A_ctl2%3AtxtHidSection=Income%20Tax%20Fundamentals&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl2%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl2%3AtxtHidClassSchedID=176060&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl2%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl3%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl3%3AtxtHidClassSchedID=176061&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl3%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl4%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl4%3AtxtHidClassSchedID=176062&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl4%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl5%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl5%3AtxtHidClassSchedID=177354&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl5%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl2%3AtxtHidCourseId=ACC202&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl2%3AtxtHidClassSchedID=176063&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl2%3AtxtHidSection=Managerial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl3%3AtxtHidCourseId=ACC202&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl3%3AtxtHidClassSchedID=176064&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl3%3AtxtHidSection=Managerial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3Ahid_Courses=&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text_main}&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION={EVENTVALIDATION_text_main}'
                
                # Submit search request
                post_response = yield scrapy.Request(url=self.course_url, method="POST", headers=self.course_headers, body=payload, meta=self.proxy)
                
                # Update ASP.NET state values for subsequent requests
                VIEWSTATE_text  = urllib.parse.quote(post_response.xpath('//input[@id="__VIEWSTATE"]/@value').get('')).strip()
                VIEWSTATEGENERATOR_text  = urllib.parse.quote(post_response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip())
                EVENTVALIDATION_text  = urllib.parse.quote(post_response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get('')).strip()
                
                # Extract course result rows
                blocks = post_response.xpath('//div[@id="_ctl0_PlaceHolderMain__ctl0_pnlResults"]//div[@class="row"]//table/tbody/tr')
                counts = len(blocks)
                
                # Iterate through each course row
                for i, block in enumerate(blocks, start=1):
                    print(f"{i}/{counts}")
                    course_id = re.sub(r'\s+', ' ',block.xpath('./td[1]/input[1]/@value').get('')).strip()
                    course_dept = re.sub(r'\s+', ' ',block.xpath('./td[1]/input[3]/@value').get('')).strip()
                    course_name = f"{course_id} {course_dept}"
                    sec_url = block.xpath('./td[2]/a/@href').get('').strip()
                    seats = block.xpath('./td[9]/span/text()').get('').strip()
                    
                    # Build POST payload to load section details
                    section_url = urllib.parse.quote(re.findall(r'\'([\w\W]*?)\'', sec_url)[0])
                    sec_payload = f'__EVENTTARGET={section_url}&__EVENTARGUMENT=&__VIEWSTATE={VIEWSTATE_text}&_ctl0%3Apagetitle%3AhfShowLinkText=Show%20Quick%20Links...&_ctl0%3Apagetitle%3AhfHideLinkText=Hide%20Quick%20Link...&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbCampus=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_0=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_1=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_2=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_3=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_4=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_5=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_6=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_7=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_8=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbCampusList%3AchbCampusList_9=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbTerm={term}&{sub_id}=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtKeyword=&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbLowTime=0&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbHighTime=23&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkMorning=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkAfternoon=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkEvening=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbInstructor=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtInstuSearch=&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbDeliveryMethod=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtDelMethSearch=&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkMo=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkTu=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkWe=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkTh=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkFr=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkSa=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkSu=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AhdnShowMore=false&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtIsShowMore=0&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtCourseListResult=&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl2%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl2%3AtxtHidClassSchedID=176056&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl2%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl3%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl3%3AtxtHidClassSchedID=176057&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl3%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl4%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl4%3AtxtHidClassSchedID=176058&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl4%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl5%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl5%3AtxtHidClassSchedID=176150&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl5%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl6%3AtxtHidCourseId=ACC121&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl6%3AtxtHidClassSchedID=176059&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl6%3AtxtHidSection=Income%20Tax%20Fundamentals&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl7%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl7%3AtxtHidClassSchedID=176060&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl7%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl8%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl8%3AtxtHidClassSchedID=176061&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl8%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl9%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl9%3AtxtHidClassSchedID=176062&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl9%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl10%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl10%3AtxtHidClassSchedID=177354&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl10%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl11%3AtxtHidCourseId=ACC202&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl11%3AtxtHidClassSchedID=176063&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl11%3AtxtHidSection=Managerial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl12%3AtxtHidCourseId=ACC202&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl12%3AtxtHidClassSchedID=176064&_ctl0%3APlaceHolderMain%3A_ctl0%3ACourseList%3A_ctl12%3AtxtHidSection=Managerial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl2%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl2%3AtxtHidClassSchedID=176056&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl2%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl3%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl3%3AtxtHidClassSchedID=176057&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl3%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl4%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl4%3AtxtHidClassSchedID=176058&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl4%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl5%3AtxtHidCourseId=ACC100&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl5%3AtxtHidClassSchedID=176150&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl0%3AdgSubGroup%3A_ctl5%3AtxtHidSection=Fundamentals%20of%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl1%3AdgSubGroup%3A_ctl2%3AtxtHidCourseId=ACC121&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl1%3AdgSubGroup%3A_ctl2%3AtxtHidClassSchedID=176059&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl1%3AdgSubGroup%3A_ctl2%3AtxtHidSection=Income%20Tax%20Fundamentals&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl2%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl2%3AtxtHidClassSchedID=176060&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl2%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl3%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl3%3AtxtHidClassSchedID=176061&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl3%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl4%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl4%3AtxtHidClassSchedID=176062&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl4%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl5%3AtxtHidCourseId=ACC201&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl5%3AtxtHidClassSchedID=177354&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl2%3AdgSubGroup%3A_ctl5%3AtxtHidSection=Financial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl2%3AtxtHidCourseId=ACC202&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl2%3AtxtHidClassSchedID=176063&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl2%3AtxtHidSection=Managerial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl3%3AtxtHidCourseId=ACC202&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl3%3AtxtHidClassSchedID=176064&_ctl0%3APlaceHolderMain%3A_ctl0%3ArptrResultGroups%3A_ctl3%3AdgSubGroup%3A_ctl3%3AtxtHidSection=Managerial%20Accounting&_ctl0%3APlaceHolderMain%3A_ctl0%3Ahid_Courses=&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text}&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION={EVENTVALIDATION_text}'
                    
                    # Submit section details request
                    sec_response = yield scrapy.Request(url=self.course_url,method="POST",headers=self.course_headers,body=sec_payload, meta=self.proxy)
                    
                    # Extract course start and end dates
                    start_date = sec_response.xpath('//input[@id="_ctl0_PlaceHolderMain__ctl0_txtCDClassStart"]/@value').get('').strip()
                    end_date = sec_response.xpath('//input[@id="_ctl0_PlaceHolderMain__ctl0_txtCDClassEnd"]/@value').get('').strip()
                    
                    # Append parsed course record
                    course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": course_name,
                        "Course Description": re.sub(r'\s+', ' ',sec_response.xpath('//*[@id="_ctl0_PlaceHolderMain__ctl0_pCDCourseComments"]/text()').get('')).strip(),
                        "Class Number": sec_response.xpath('//input[@id="_ctl0_PlaceHolderMain__ctl0_txtCDClassCode"]/@value').get('').strip(),
                        "Section": sec_response.xpath('//input[@id="_ctl0_PlaceHolderMain__ctl0_txtCDSection"]/@value').get('').strip(),
                        "Instructor": sec_response.xpath('//input[@id="_ctl0_PlaceHolderMain__ctl0_txtCDInstructor"]/@value').get('').strip(),
                        "Enrollment": seats,
                        "Course Dates": f"{start_date} - {end_date}",
                        "Location": sec_response.xpath('//input[@id="_ctl0_PlaceHolderMain__ctl0_txtCDLocation"]/@value').get('').strip(),   
                        "Textbook/Course Materials": '',
                    })
                    
        # Convert scraped course rows into a DataFrame and save it
        course_df = pd.DataFrame(course_rows)
        save_df(course_df,    self.institution_id, "course")
        
        
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
        # Initialize list to store extracted staff records
        directory_rows = []
        
        # Get the list of all departments
        dept_lists = response.xpath('//select[@id="deptlst"]/option/@value').getall()
        
        # Iterate through each department
        for dept in dept_lists[1:]:
            print(dept)
            # URL-encode department name
            dept_frame = dept.replace(' ','%20')
            url = f"https://centralaz.edu/nexusphp/displaydir19.php?fname=&lname=&v_dept={dept_frame}"
            
            # Set request headers
            headers = {
            'accept': '*/*',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
            'referer': 'https://centralaz.edu/directorytest/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
            }
            
            # Make GET request to fetch department directory page
            inner_response = requests.request("GET", url, headers=headers, proxies=self.proxies)
            
            inner_response_xpath = Selector(text=inner_response.text)
            
            # Extract staff blocks
            blocks = inner_response_xpath.xpath('//div[@class="datablk"]')
            
            # Iterate through each staff block and extract details
            for block in blocks:
                
                # Append extracted staff data to the results list
                directory_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.directory_source_url,
                            "Name": html.unescape(re.sub(r'\s+', ' ',block.xpath('.//div[@class="datanm"]/text()').get(''))).strip(),
                            "Title": html.unescape(re.sub(r'\s+', ' ',block.xpath('.//label[contains(text(),"Title:")]/parent::div[1]/text()').get(''))).strip(),
                            "Email": html.unescape(re.sub(r'\s+', ' ',block.xpath('.//label[contains(text(),"Email:")]/parent::div[1]/text()').get(''))).strip(),
                            "Phone Number": html.unescape(re.sub(r'\s+', ' ',block.xpath('.//label[contains(text(),"Phone:")]/parent::div[1]/text()').get(''))).strip(),
                        }
                    )
                
        # Convert scraped directory rows into a DataFrame and save it
        directory_df = pd.DataFrame(directory_rows)
        save_df(directory_df, self.institution_id, "campus")
        
        
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
        # Initialize list to store calendar rows
        calendar_rows = []
        
        term1 = ''
        
        # Extract term codes from page text
        if re.search(r'webName\:\s*\"(.*?)\"', response.text):
            terms = re.findall(r'webName\:\s*\"(.*?)\"', response.text)
        else:
            terms = ''
            
        # Iterate through each term
        for term in terms:
            url = f"https://25livepub.collegenet.com/s.aspx?calendar={term}&widget=main&spudformat=xhr"
            
            # Fetch term calendar page
            cal_response = requests.request("GET", url, proxies=self.proxies)
            cal_response_xpath = Selector(text=cal_response.text)
            
            # Extract session headers (last 3 table header columns)
            session_headers = cal_response_xpath.xpath(
                '//table[@class="twSimpleTableTable"]/tbody/tr[@class="twSimpleTableHeadRow"][1]/th//text()'
            ).getall()[-3:]
            
            # Iterate through all table rows
            blocks = cal_response_xpath.xpath('//table[@class="twSimpleTableTable"]/tbody/tr')
            for block in blocks:
                
                term2 = block.xpath('.//div[@class="twSimpleTableGroupHead"]/text()').get('').strip()
                
                header_name = block.xpath('.//th[2]/text()').get('').strip()
                header_name1 = block.xpath('.//td[2]/text()').get('').strip()
                
                if term2:
                    # Update current term name
                    term1 = term2
                    print(term2)
                    
                elif "Event" == header_name or "Event" == header_name1:
                    # Skip table header rows
                    print("header rows")
                    
                else:
                    # Extract event details
                    event_name = block.xpath('.//span[@class="twDescription"]//text()').get('').strip()
                    date = block.xpath('./td[2]//text()').get('').strip()
                    description = block.xpath('./td[3]//text()').get('').strip()
                    
                    # Extract session-specific dates (TD4–TD6), ignoring "N/A"
                    td4 = block.xpath('./td[4]//text()').get('').replace('N/A','').strip() 
                    td5 = block.xpath('./td[5]//text()').get('').replace('N/A','').strip() 
                    td6 = block.xpath('./td[6]//text()').get('').replace('N/A','').strip() 
                    
                    # If event has a description, combine it with event name
                    if description:
                        # Build base row dictionary
                        base_row = {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_url,
                            "Term Name": term1,
                        }
                        
                        calendar_rows.append({
                            **base_row,
                            "Term Date": date,
                            "Term Date Description": f"{event_name} - {description}",
                        })
                        
                        # Add session-specific dates
                        for td, header in zip((td4, td5, td6), session_headers):
                            if td:
                                calendar_rows.append({
                                    **base_row,
                                    "Term Date": td,
                                    "Term Date Description": f"{description} - {header}",
                                })
                                    
                    elif not description:
                        # If no description, use event name as description   
                        base_row = {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_url,
                            "Term Name": term1,
                        }
                            
                        calendar_rows.append({
                            **base_row,
                            "Term Date": date,
                            "Term Date Description": event_name,
                        })
                        
                        for td, header in zip((td4, td5, td6), session_headers):
                            if td:
                                calendar_rows.append({
                                    **base_row,
                                    "Term Date": td,
                                    "Term Date Description": f"{event_name} - {header}",
                                })
                                    
        # Convert scraped directory rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")