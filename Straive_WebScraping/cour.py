import time
import random
import string
import requests
import pandas as pd
import re
from scrapy.selector import Selector
from playwright.sync_api import sync_playwright

# ---------------- CONFIG ---------------- #
START_URL = "https://ssb.cochise.edu/StudentRegistrationSsb/ssb/term/termSelection?mode=search"
HEADLESS = False   # True for server
PAGE_SIZE = 50

# ---------------- SAVE FUNCTION ---------------- #
def save_df(df, institution_id, name):
    filename = f"{institution_id}_{name}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ Saved: {filename}")
    
    # ---------------- MAIN SCRAPER ---------------- #
def scrape_hawaii_courses():
    rows = []
    institution_id = 258423580092557277

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        try:
            print("Opening term selector page...")
            page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)

            page.locator('//span[@class="select2-arrow"]').click()
            time.sleep(2)

            terms = page.locator('//ul[@class="select2-results"]//li/div[@class="select2-result-label"]/div')
            print(f"Found {terms.count()} terms")

            # LOOP TERMS
            for i in range(terms.count()):
                try:
                    print(f"\n====== Selecting term index {i} ======")

                    terms.nth(i).scroll_into_view_if_needed()
                    terms.nth(i).click()
                    time.sleep(1)

                    term_code = page.locator('//input[@id="txt_term"]').get_attribute("value")
                    print("Term Code:", term_code)

                    page.locator('//button[@id="term-go"]').click()
                    time.sleep(2)

                    page.locator('//a[@id="advanced-search-link"]').click()
                    time.sleep(2)

                    page.locator('//label[contains(text(),"Campus")]/parent::li/parent::ul').click()
                    time.sleep(2)

                    # page.locator('//div[@id="select2-result-label-8"]//div | //div[@id="select2-result-label-9"]//div').click()
                    # time.sleep(2)

                    page.locator('//div[@id="select2-result-label-9"]//div').click()
                    
                    time.sleep(2)

                    page.locator('//button[@id="search-go"]').click()
                    time.sleep(3)

                    cookies = context.cookies()
                    cookies_dict = {c["name"]: c["value"] for c in cookies}

                    unique_session_id = ''.join(random.choices(string.ascii_lowercase, k=5)) + str(int(time.time() * 1000))

                    page_offset = 0
                    total_count = None
                    course_counter = 0

                    while True:
                        try:
                            api_url = (
                                "https://ssb.cochise.edu/StudentRegistrationSsb/ssb/searchResults/searchResults?"
                                f"txt_campus=2&txt_term={term_code}"
                                f"&uniqueSessionId={unique_session_id}"
                                f"&pageOffset={page_offset}&pageMaxSize=187"
                                f"&sortColumn=subjectDescription&sortDirection=asc"
                            )

                            # api_url= ("https://ssb.cochise.edu/StudentRegistrationSsb/ssb/searchResults/searchResults?"
                            # f"txt_campus=1%2C2&txt_term={term_code}"
                            # f"&startDatepicker=&endDatepicker=&uniqueSessionId={unique_session_id}"
                            # f"&pageOffset={page_offset}&pageMaxSize=213"
                            # f"&sortColumn=subjectDescription&sortDirection=asc")
                            headers = {
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                    'Referer': 'https://ssb.cochise.edu/StudentRegistrationSsb/ssb/classSearch/classSearch',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest',
                    'X-Synchronizer-Token': '45f457cf-7bf3-4973-b5b1-611d267a6299',
                    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    # 'Cookie': 'JSESSIONID=07015678FA9D5F036F0C8D6AC3992B78; _gcl_au=1.1.1564233636.1771564599; calltrk_referrer=direct; calltrk_landing=https%3A%2F%2Fwww.cochise.edu%2Fabout%2Fdirectory%2Ffaculty-directory.html; _gid=GA1.2.2110633051.1771564600; X-Oracle-BMC-LBS-Route=749342f5cb9ec68ec0974d20dd5fe7841fde08fa; _ga_4YLCNM43XG=GS2.1.s1771582126$o3$g0$t1771582126$j60$l0$h0; _ga=GA1.1.1711505462.1771564600'
                    }

                            response = requests.get(api_url, headers=headers, cookies=cookies_dict, timeout=30)
                            api_json = response.json()

                            if total_count is None:
                                total_count = api_json.get("totalCount", 0)
                                print(f"Total courses: {total_count}")

                            datas = api_json.get("data", [])

                            for data in datas:
                                try:
                                    course_counter += 1

                                    if course_counter % 150 == 0:
                                        print("🔄 Refreshing Banner session...")
                                        page.reload(wait_until="networkidle")
                                        time.sleep(3)

                                        cookies = context.cookies()
                                        cookies_dict = {c["name"]: c["value"] for c in cookies}

                                    term_code = data.get("term", "")
                                    crn = data.get("courseReferenceNumber", "")

                                    # -------- Description -------- #
                                    desc = ""
                                    try:
                                        # breakpoint()
                                        desc_url = "https://ssb.cochise.edu/StudentRegistrationSsb/ssb/searchResults/getCourseDescription"
                                        # payload = f"term={term_code}&courseReferenceNumber={crn}"
                                        payload = f"term={term_code}&courseReferenceNumber={crn}&first=first"
                                        headers = {
                                            'Accept': 'text/html, */*; q=0.01',
                                            'Accept-Language': 'en-US,en;q=0.9',
                                            'Connection': 'keep-alive',
                                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                                            'Origin': 'https://ssb.cochise.edu',
                                            'Referer': 'https://ssb.cochise.edu/StudentRegistrationSsb/ssb/classSearch/classSearch',
                                            'Sec-Fetch-Dest': 'empty',
                                            'Sec-Fetch-Mode': 'cors',
                                            'Sec-Fetch-Site': 'same-origin',
                                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
                                            'X-Requested-With': 'XMLHttpRequest',
                                            'X-Synchronizer-Token': '2424f5c2-cd16-4e49-a257-9faa97560f7e',
                                            'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
                                            'sec-ch-ua-mobile': '?0',
                                            'sec-ch-ua-platform': '"Windows"',
                                            #   'Cookie': 'JSESSIONID=0ACBF5E8BBDA4A624AE0CEE0190FE14D; _gcl_au=1.1.1564233636.1771564599; calltrk_referrer=direct; calltrk_landing=https%3A%2F%2Fwww.cochise.edu%2Fabout%2Fdirectory%2Ffaculty-directory.html; _gid=GA1.2.2110633051.1771564600; _ga_4YLCNM43XG=GS2.1.s1771582126$o3$g0$t1771582126$j60$l0$h0; _ga=GA1.1.1711505462.1771564600; X-Oracle-BMC-LBS-Route=5c71b970f8ce6faadf846a98466380544aa729d2; JSESSIONID=4C489F0230B6EAEFBCC4C8F5A601B6C6'
                                            }
                                        desc_resp = requests.post(desc_url, headers=headers, data=payload, cookies=cookies_dict, timeout=60)
                                        desc_sel = Selector(text=desc_resp.text)
                                        # breakpoint()
                                        desc = " ".join(desc_sel.xpath('//section[@aria-labelledby="courseDescription"]/text()').getall()).strip()
                                    except Exception as e:
                                        print(f"❌ Desc error CRN {crn}: {e}")

                                    # -------- Meeting Times -------- #
                                    instructor = ""
                                    course_dates = ""
                                    try:
                                        meet_url = f"https://ssb.cochise.edu/StudentRegistrationSsb/ssb/searchResults/getFacultyMeetingTimes?term={term_code}&courseReferenceNumber={crn}"
                                        meet_resp = requests.get(meet_url, headers=headers, cookies=cookies_dict, timeout=30)
                                        meet_json = meet_resp.json()

                                        for m in meet_json.get("fmt", []):
                                            fac = m.get("faculty", [])
                                            instructor = ", ".join(f.get("displayName", "") for f in fac if f.get("displayName"))
                                            mt = m.get("meetingTime", {})
                                            course_dates = f"{mt.get('startDate','')} - {mt.get('endDate','')}"
                                    except Exception as e:
                                        print(f"❌ Meeting time error CRN {crn}: {e}")

                                    faculty_list = data.get("faculty", [])

                                    rows.append({
                                        "Cengage Master Institution ID": institution_id,
                                        "Source URL": START_URL,
                                        "Course Name": f"{data.get('subject','')} {data.get('courseDisplay','')} {data.get('courseTitle','')}".strip(),
                                        "Course Description": re.sub(r'\s+',' ', desc),
                                        "Class Number": faculty_list[0].get("courseReferenceNumber", "") if faculty_list else "",
                                        "Section": data.get("sequenceNumber",""),
                                        "Instructor": instructor,
                                        "Enrollment": f"{data.get('enrollment','')} / {data.get('maximumEnrollment','')}",
                                        "Course Dates": course_dates,
                                        "Location": data.get('campusDescription',''),
                                        "Textbook/Course Materials": "https://www.cochise.edu/bookstore"
                                    })

                                except Exception as course_err:
                                    print(f"❌ Course parse failed: {course_err}")
                                    continue

                            page_offset += PAGE_SIZE
                            print("Page Offset:", page_offset)

                            if page_offset >= total_count:
                                break

                        except Exception as page_err:
                            print(f"❌ API page error offset {page_offset}: {page_err}")
                            break

                    # Back to term selector
                    page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(2)
                    page.locator('//span[@class="select2-arrow"]').click()
                    time.sleep(2)

                except Exception as term_err:
                    print(f"❌ Term error index {i}: {term_err}")
                    continue

        except Exception as main_err:
            print("❌ MAIN SCRAPER ERROR:", main_err)

        finally:
            browser.close()

        # Save CSV
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, institution_id, "course")


# ---------------- RUN ---------------- #
if __name__ == "__main__":
    scrape_hawaii_courses()
