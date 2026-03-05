import scrapy
import requests
import pandas as pd
from ..utils import *
from ..utils import save_df
from parsel import Selector

class MsnSpider(scrapy.Spider):
    """
    Spider for Montana State University-Northern (MSUN).
    Scrapes Course Schedules via POST requests, Staff Directory via alphabetical pagination,
    and Academic Calendars from HTML tables.
    """
    name = "msn"
    institution_id = 258446291695069136

    # In-memory storage for extracted items
    calendar_rows = []
    directory_rows = []
    course_rows = []

    # Target Endpoints
    course_url = "https://prodmyinfo.montana.edu/pls/bzagent/bzskcrse.PW_ListSchClassSimple"
    course_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://prodmyinfo.montana.edu',
        'Referer': 'https://prodmyinfo.montana.edu/pls/bzagent/bzskcrse.PW_SelSchClass',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
        }

    directory_urls = "https://www.msun.edu/admin/directories/a.aspx"
    calendar_url = "https://www.msun.edu/academics/acad-cal.aspx"

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            # Term codes for Banner: 202670=Fall 2026, 202630=Spring 2026, etc.
            term_lists = ['202670','202650','202630','202570','202550','202530']
            for term_list in term_lists:
                # payload selects all subjects (sel_subj=AC, sel_subj=ACT, etc.)
                course_payload = f'sel_subj=dummy&bl_online=FALSE&sel_day=dummy&sel_online=dummy&term={term_list}&sel_subj=AC&sel_subj=ACT&sel_subj=ACTG&sel_subj=ACTV&sel_subj=AGBE&sel_subj=AGED&sel_subj=AGSC&sel_subj=AGTE&sel_subj=AHHS&sel_subj=AHMA&sel_subj=AHMS&sel_subj=AHMT&sel_subj=AMGT&sel_subj=AMST&sel_subj=ANSC&sel_subj=ANTY&sel_subj=ARAB&sel_subj=ARCH&sel_subj=ARNR&sel_subj=ART&sel_subj=ARTH&sel_subj=ARTZ&sel_subj=AS&sel_subj=ASTR&sel_subj=AVFT&sel_subj=AVMT&sel_subj=BCH&sel_subj=BFIN&sel_subj=BGEN&sel_subj=BIOB&sel_subj=BIOE&sel_subj=BIOH&sel_subj=BIOL&sel_subj=BIOM&sel_subj=BIOO&sel_subj=BMGT&sel_subj=BMIS&sel_subj=BMKT&sel_subj=CAA&sel_subj=CAPP&sel_subj=CHIN&sel_subj=CHMY&sel_subj=CHTH&sel_subj=CLS&sel_subj=COA&sel_subj=COLS&sel_subj=COMX&sel_subj=CRWR&sel_subj=CSCI&sel_subj=CSTN&sel_subj=CULA&sel_subj=DDSN&sel_subj=DENT&sel_subj=DGED&sel_subj=EBIO&sel_subj=EBME&sel_subj=ECHM&sel_subj=ECIV&sel_subj=ECNS&sel_subj=ECP&sel_subj=EDCI&sel_subj=EDEC&sel_subj=EDLD&sel_subj=EDM&sel_subj=EDP&sel_subj=EDSD&sel_subj=EDSP&sel_subj=EDU&sel_subj=EELE&sel_subj=EENV&sel_subj=EFIN&sel_subj=EGEN&sel_subj=EIND&sel_subj=ELCT&sel_subj=EM&sel_subj=EMAN&sel_subj=EMAT&sel_subj=EMEC&sel_subj=ENGL&sel_subj=ENGR&sel_subj=ENSC&sel_subj=ENT&sel_subj=ENTO&sel_subj=EQUH&sel_subj=EQUS&sel_subj=ERTH&sel_subj=ESCI&sel_subj=ESL&sel_subj=ESOF&sel_subj=ETCC&sel_subj=ETEC&sel_subj=ETME&sel_subj=FILM&sel_subj=FRCH&sel_subj=GDSN&sel_subj=GEO&sel_subj=GH&sel_subj=GPHY&sel_subj=GRMN&sel_subj=GRST&sel_subj=HADM&sel_subj=HDCO&sel_subj=HDFP&sel_subj=HDFS&sel_subj=HEE&sel_subj=HHD&sel_subj=HIST&sel_subj=HLD&sel_subj=HMED&sel_subj=HONR&sel_subj=HORT&sel_subj=HSTA&sel_subj=HSTR&sel_subj=HTH&sel_subj=HTR&sel_subj=HVC&sel_subj=IDSN&sel_subj=IMID&sel_subj=ITS&sel_subj=JPNS&sel_subj=KIN&sel_subj=LARC&sel_subj=LEG&sel_subj=LIBR&sel_subj=LIFE&sel_subj=LING&sel_subj=LIT&sel_subj=LRES&sel_subj=LS&sel_subj=LSCI&sel_subj=M&sel_subj=MART&sel_subj=MAS&sel_subj=MB&sel_subj=MBEH&sel_subj=MBSP&sel_subj=MCH&sel_subj=MEDS&sel_subj=MFTG&sel_subj=MOR&sel_subj=MSEM&sel_subj=MSL&sel_subj=MSSE&sel_subj=MTSI&sel_subj=MUSE&sel_subj=MUSI&sel_subj=MUST&sel_subj=NASX&sel_subj=NEUR&sel_subj=NRSG&sel_subj=NRSM&sel_subj=NUTR&sel_subj=OPTI&sel_subj=OSH&sel_subj=PHL&sel_subj=PHOT&sel_subj=PHSX&sel_subj=PLTT&sel_subj=PLUM&sel_subj=PSCI&sel_subj=PSPP&sel_subj=PSYX&sel_subj=RLST&sel_subj=RS&sel_subj=SFBS&sel_subj=SIGN&sel_subj=SOCI&sel_subj=SPNS&sel_subj=SRVY&sel_subj=STAT&sel_subj=TE&sel_subj=THTR&sel_subj=UC&sel_subj=US&sel_subj=USP&sel_subj=VM&sel_subj=WGSS&sel_subj=WILD&sel_subj=WLDG&sel_subj=WRIT&sel_inst=0&sel_online=&sel_crse=&begin_hh=0&begin_mi=0&end_hh=0&end_mi=0'
                yield scrapy.Request(url=self.course_url,headers=self.course_headers,body=course_payload,method="POST",callback=self.parse_course, dont_filter=True)
        
        elif mode == 'directory':
            letters=  ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j','k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't','u', 'w', 'y', 'z']
            for letter in letters:
                directory_url = f"https://www.msun.edu/admin/directories/{letter}.aspx"
                yield scrapy.Request(url=directory_url, callback=self.parse_directory, dont_filter=True)
        
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            # Term codes for Banner: 202670=Fall 2026, 202630=Spring 2026, etc.
            term_lists = ['202670','202650','202630','202570','202550','202530']
            for term_list in term_lists:
                # payload selects all subjects (sel_subj=AC, sel_subj=ACT, etc.)
                course_payload = f'sel_subj=dummy&bl_online=FALSE&sel_day=dummy&sel_online=dummy&term={term_list}&sel_subj=AC&sel_subj=ACT&sel_subj=ACTG&sel_subj=ACTV&sel_subj=AGBE&sel_subj=AGED&sel_subj=AGSC&sel_subj=AGTE&sel_subj=AHHS&sel_subj=AHMA&sel_subj=AHMS&sel_subj=AHMT&sel_subj=AMGT&sel_subj=AMST&sel_subj=ANSC&sel_subj=ANTY&sel_subj=ARAB&sel_subj=ARCH&sel_subj=ARNR&sel_subj=ART&sel_subj=ARTH&sel_subj=ARTZ&sel_subj=AS&sel_subj=ASTR&sel_subj=AVFT&sel_subj=AVMT&sel_subj=BCH&sel_subj=BFIN&sel_subj=BGEN&sel_subj=BIOB&sel_subj=BIOE&sel_subj=BIOH&sel_subj=BIOL&sel_subj=BIOM&sel_subj=BIOO&sel_subj=BMGT&sel_subj=BMIS&sel_subj=BMKT&sel_subj=CAA&sel_subj=CAPP&sel_subj=CHIN&sel_subj=CHMY&sel_subj=CHTH&sel_subj=CLS&sel_subj=COA&sel_subj=COLS&sel_subj=COMX&sel_subj=CRWR&sel_subj=CSCI&sel_subj=CSTN&sel_subj=CULA&sel_subj=DDSN&sel_subj=DENT&sel_subj=DGED&sel_subj=EBIO&sel_subj=EBME&sel_subj=ECHM&sel_subj=ECIV&sel_subj=ECNS&sel_subj=ECP&sel_subj=EDCI&sel_subj=EDEC&sel_subj=EDLD&sel_subj=EDM&sel_subj=EDP&sel_subj=EDSD&sel_subj=EDSP&sel_subj=EDU&sel_subj=EELE&sel_subj=EENV&sel_subj=EFIN&sel_subj=EGEN&sel_subj=EIND&sel_subj=ELCT&sel_subj=EM&sel_subj=EMAN&sel_subj=EMAT&sel_subj=EMEC&sel_subj=ENGL&sel_subj=ENGR&sel_subj=ENSC&sel_subj=ENT&sel_subj=ENTO&sel_subj=EQUH&sel_subj=EQUS&sel_subj=ERTH&sel_subj=ESCI&sel_subj=ESL&sel_subj=ESOF&sel_subj=ETCC&sel_subj=ETEC&sel_subj=ETME&sel_subj=FILM&sel_subj=FRCH&sel_subj=GDSN&sel_subj=GEO&sel_subj=GH&sel_subj=GPHY&sel_subj=GRMN&sel_subj=GRST&sel_subj=HADM&sel_subj=HDCO&sel_subj=HDFP&sel_subj=HDFS&sel_subj=HEE&sel_subj=HHD&sel_subj=HIST&sel_subj=HLD&sel_subj=HMED&sel_subj=HONR&sel_subj=HORT&sel_subj=HSTA&sel_subj=HSTR&sel_subj=HTH&sel_subj=HTR&sel_subj=HVC&sel_subj=IDSN&sel_subj=IMID&sel_subj=ITS&sel_subj=JPNS&sel_subj=KIN&sel_subj=LARC&sel_subj=LEG&sel_subj=LIBR&sel_subj=LIFE&sel_subj=LING&sel_subj=LIT&sel_subj=LRES&sel_subj=LS&sel_subj=LSCI&sel_subj=M&sel_subj=MART&sel_subj=MAS&sel_subj=MB&sel_subj=MBEH&sel_subj=MBSP&sel_subj=MCH&sel_subj=MEDS&sel_subj=MFTG&sel_subj=MOR&sel_subj=MSEM&sel_subj=MSL&sel_subj=MSSE&sel_subj=MTSI&sel_subj=MUSE&sel_subj=MUSI&sel_subj=MUST&sel_subj=NASX&sel_subj=NEUR&sel_subj=NRSG&sel_subj=NRSM&sel_subj=NUTR&sel_subj=OPTI&sel_subj=OSH&sel_subj=PHL&sel_subj=PHOT&sel_subj=PHSX&sel_subj=PLTT&sel_subj=PLUM&sel_subj=PSCI&sel_subj=PSPP&sel_subj=PSYX&sel_subj=RLST&sel_subj=RS&sel_subj=SFBS&sel_subj=SIGN&sel_subj=SOCI&sel_subj=SPNS&sel_subj=SRVY&sel_subj=STAT&sel_subj=TE&sel_subj=THTR&sel_subj=UC&sel_subj=US&sel_subj=USP&sel_subj=VM&sel_subj=WGSS&sel_subj=WILD&sel_subj=WLDG&sel_subj=WRIT&sel_inst=0&sel_online=&sel_crse=&begin_hh=0&begin_mi=0&end_hh=0&end_mi=0'
                yield scrapy.Request(url=self.course_url,headers=self.course_headers,body=course_payload,method="POST",callback=self.parse_course, dont_filter=True)
            
            letters=  ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j','k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't','u', 'w', 'y', 'z']
            for letter in letters:
                directory_url = f"https://www.msun.edu/admin/directories/{letter}.aspx"
                yield scrapy.Request(url=directory_url, callback=self.parse_directory, dont_filter=True)
            
        elif mode in ['course_calendar', 'calendar_course']:
            # Term codes for Banner: 202670=Fall 2026, 202630=Spring 2026, etc.
            term_lists = ['202670','202650','202630','202570','202550','202530']
            for term_list in term_lists:
                # payload selects all subjects (sel_subj=AC, sel_subj=ACT, etc.)
                course_payload = f'sel_subj=dummy&bl_online=FALSE&sel_day=dummy&sel_online=dummy&term={term_list}&sel_subj=AC&sel_subj=ACT&sel_subj=ACTG&sel_subj=ACTV&sel_subj=AGBE&sel_subj=AGED&sel_subj=AGSC&sel_subj=AGTE&sel_subj=AHHS&sel_subj=AHMA&sel_subj=AHMS&sel_subj=AHMT&sel_subj=AMGT&sel_subj=AMST&sel_subj=ANSC&sel_subj=ANTY&sel_subj=ARAB&sel_subj=ARCH&sel_subj=ARNR&sel_subj=ART&sel_subj=ARTH&sel_subj=ARTZ&sel_subj=AS&sel_subj=ASTR&sel_subj=AVFT&sel_subj=AVMT&sel_subj=BCH&sel_subj=BFIN&sel_subj=BGEN&sel_subj=BIOB&sel_subj=BIOE&sel_subj=BIOH&sel_subj=BIOL&sel_subj=BIOM&sel_subj=BIOO&sel_subj=BMGT&sel_subj=BMIS&sel_subj=BMKT&sel_subj=CAA&sel_subj=CAPP&sel_subj=CHIN&sel_subj=CHMY&sel_subj=CHTH&sel_subj=CLS&sel_subj=COA&sel_subj=COLS&sel_subj=COMX&sel_subj=CRWR&sel_subj=CSCI&sel_subj=CSTN&sel_subj=CULA&sel_subj=DDSN&sel_subj=DENT&sel_subj=DGED&sel_subj=EBIO&sel_subj=EBME&sel_subj=ECHM&sel_subj=ECIV&sel_subj=ECNS&sel_subj=ECP&sel_subj=EDCI&sel_subj=EDEC&sel_subj=EDLD&sel_subj=EDM&sel_subj=EDP&sel_subj=EDSD&sel_subj=EDSP&sel_subj=EDU&sel_subj=EELE&sel_subj=EENV&sel_subj=EFIN&sel_subj=EGEN&sel_subj=EIND&sel_subj=ELCT&sel_subj=EM&sel_subj=EMAN&sel_subj=EMAT&sel_subj=EMEC&sel_subj=ENGL&sel_subj=ENGR&sel_subj=ENSC&sel_subj=ENT&sel_subj=ENTO&sel_subj=EQUH&sel_subj=EQUS&sel_subj=ERTH&sel_subj=ESCI&sel_subj=ESL&sel_subj=ESOF&sel_subj=ETCC&sel_subj=ETEC&sel_subj=ETME&sel_subj=FILM&sel_subj=FRCH&sel_subj=GDSN&sel_subj=GEO&sel_subj=GH&sel_subj=GPHY&sel_subj=GRMN&sel_subj=GRST&sel_subj=HADM&sel_subj=HDCO&sel_subj=HDFP&sel_subj=HDFS&sel_subj=HEE&sel_subj=HHD&sel_subj=HIST&sel_subj=HLD&sel_subj=HMED&sel_subj=HONR&sel_subj=HORT&sel_subj=HSTA&sel_subj=HSTR&sel_subj=HTH&sel_subj=HTR&sel_subj=HVC&sel_subj=IDSN&sel_subj=IMID&sel_subj=ITS&sel_subj=JPNS&sel_subj=KIN&sel_subj=LARC&sel_subj=LEG&sel_subj=LIBR&sel_subj=LIFE&sel_subj=LING&sel_subj=LIT&sel_subj=LRES&sel_subj=LS&sel_subj=LSCI&sel_subj=M&sel_subj=MART&sel_subj=MAS&sel_subj=MB&sel_subj=MBEH&sel_subj=MBSP&sel_subj=MCH&sel_subj=MEDS&sel_subj=MFTG&sel_subj=MOR&sel_subj=MSEM&sel_subj=MSL&sel_subj=MSSE&sel_subj=MTSI&sel_subj=MUSE&sel_subj=MUSI&sel_subj=MUST&sel_subj=NASX&sel_subj=NEUR&sel_subj=NRSG&sel_subj=NRSM&sel_subj=NUTR&sel_subj=OPTI&sel_subj=OSH&sel_subj=PHL&sel_subj=PHOT&sel_subj=PHSX&sel_subj=PLTT&sel_subj=PLUM&sel_subj=PSCI&sel_subj=PSPP&sel_subj=PSYX&sel_subj=RLST&sel_subj=RS&sel_subj=SFBS&sel_subj=SIGN&sel_subj=SOCI&sel_subj=SPNS&sel_subj=SRVY&sel_subj=STAT&sel_subj=TE&sel_subj=THTR&sel_subj=UC&sel_subj=US&sel_subj=USP&sel_subj=VM&sel_subj=WGSS&sel_subj=WILD&sel_subj=WLDG&sel_subj=WRIT&sel_inst=0&sel_online=&sel_crse=&begin_hh=0&begin_mi=0&end_hh=0&end_mi=0'
                yield scrapy.Request(url=self.course_url,headers=self.course_headers,body=course_payload,method="POST",callback=self.parse_course, dont_filter=True)
            
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            
            letters=  ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j','k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't','u', 'w', 'y', 'z']
            for letter in letters:
                directory_url = f"https://www.msun.edu/admin/directories/{letter}.aspx"
                yield scrapy.Request(url=directory_url, callback=self.parse_directory, dont_filter=True)
        # All three (default)
        else:
            # Term codes for Banner: 202670=Fall 2026, 202630=Spring 2026, etc.
            term_lists = ['202670','202650','202630','202570','202550','202530']
            for term_list in term_lists:
                # payload selects all subjects (sel_subj=AC, sel_subj=ACT, etc.)
                course_payload = f'sel_subj=dummy&bl_online=FALSE&sel_day=dummy&sel_online=dummy&term={term_list}&sel_subj=AC&sel_subj=ACT&sel_subj=ACTG&sel_subj=ACTV&sel_subj=AGBE&sel_subj=AGED&sel_subj=AGSC&sel_subj=AGTE&sel_subj=AHHS&sel_subj=AHMA&sel_subj=AHMS&sel_subj=AHMT&sel_subj=AMGT&sel_subj=AMST&sel_subj=ANSC&sel_subj=ANTY&sel_subj=ARAB&sel_subj=ARCH&sel_subj=ARNR&sel_subj=ART&sel_subj=ARTH&sel_subj=ARTZ&sel_subj=AS&sel_subj=ASTR&sel_subj=AVFT&sel_subj=AVMT&sel_subj=BCH&sel_subj=BFIN&sel_subj=BGEN&sel_subj=BIOB&sel_subj=BIOE&sel_subj=BIOH&sel_subj=BIOL&sel_subj=BIOM&sel_subj=BIOO&sel_subj=BMGT&sel_subj=BMIS&sel_subj=BMKT&sel_subj=CAA&sel_subj=CAPP&sel_subj=CHIN&sel_subj=CHMY&sel_subj=CHTH&sel_subj=CLS&sel_subj=COA&sel_subj=COLS&sel_subj=COMX&sel_subj=CRWR&sel_subj=CSCI&sel_subj=CSTN&sel_subj=CULA&sel_subj=DDSN&sel_subj=DENT&sel_subj=DGED&sel_subj=EBIO&sel_subj=EBME&sel_subj=ECHM&sel_subj=ECIV&sel_subj=ECNS&sel_subj=ECP&sel_subj=EDCI&sel_subj=EDEC&sel_subj=EDLD&sel_subj=EDM&sel_subj=EDP&sel_subj=EDSD&sel_subj=EDSP&sel_subj=EDU&sel_subj=EELE&sel_subj=EENV&sel_subj=EFIN&sel_subj=EGEN&sel_subj=EIND&sel_subj=ELCT&sel_subj=EM&sel_subj=EMAN&sel_subj=EMAT&sel_subj=EMEC&sel_subj=ENGL&sel_subj=ENGR&sel_subj=ENSC&sel_subj=ENT&sel_subj=ENTO&sel_subj=EQUH&sel_subj=EQUS&sel_subj=ERTH&sel_subj=ESCI&sel_subj=ESL&sel_subj=ESOF&sel_subj=ETCC&sel_subj=ETEC&sel_subj=ETME&sel_subj=FILM&sel_subj=FRCH&sel_subj=GDSN&sel_subj=GEO&sel_subj=GH&sel_subj=GPHY&sel_subj=GRMN&sel_subj=GRST&sel_subj=HADM&sel_subj=HDCO&sel_subj=HDFP&sel_subj=HDFS&sel_subj=HEE&sel_subj=HHD&sel_subj=HIST&sel_subj=HLD&sel_subj=HMED&sel_subj=HONR&sel_subj=HORT&sel_subj=HSTA&sel_subj=HSTR&sel_subj=HTH&sel_subj=HTR&sel_subj=HVC&sel_subj=IDSN&sel_subj=IMID&sel_subj=ITS&sel_subj=JPNS&sel_subj=KIN&sel_subj=LARC&sel_subj=LEG&sel_subj=LIBR&sel_subj=LIFE&sel_subj=LING&sel_subj=LIT&sel_subj=LRES&sel_subj=LS&sel_subj=LSCI&sel_subj=M&sel_subj=MART&sel_subj=MAS&sel_subj=MB&sel_subj=MBEH&sel_subj=MBSP&sel_subj=MCH&sel_subj=MEDS&sel_subj=MFTG&sel_subj=MOR&sel_subj=MSEM&sel_subj=MSL&sel_subj=MSSE&sel_subj=MTSI&sel_subj=MUSE&sel_subj=MUSI&sel_subj=MUST&sel_subj=NASX&sel_subj=NEUR&sel_subj=NRSG&sel_subj=NRSM&sel_subj=NUTR&sel_subj=OPTI&sel_subj=OSH&sel_subj=PHL&sel_subj=PHOT&sel_subj=PHSX&sel_subj=PLTT&sel_subj=PLUM&sel_subj=PSCI&sel_subj=PSPP&sel_subj=PSYX&sel_subj=RLST&sel_subj=RS&sel_subj=SFBS&sel_subj=SIGN&sel_subj=SOCI&sel_subj=SPNS&sel_subj=SRVY&sel_subj=STAT&sel_subj=TE&sel_subj=THTR&sel_subj=UC&sel_subj=US&sel_subj=USP&sel_subj=VM&sel_subj=WGSS&sel_subj=WILD&sel_subj=WLDG&sel_subj=WRIT&sel_inst=0&sel_online=&sel_crse=&begin_hh=0&begin_mi=0&end_hh=0&end_mi=0'
                yield scrapy.Request(url=self.course_url,headers=self.course_headers,body=course_payload,method="POST",callback=self.parse_course, dont_filter=True)
            
            letters=  ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j','k', 'l', 'm', 'n', 'o', 'p', 'r', 's', 't','u', 'w', 'y', 'z']
            for letter in letters:
                directory_url = f"https://www.msun.edu/admin/directories/{letter}.aspx"
                yield scrapy.Request(url=directory_url, callback=self.parse_directory, dont_filter=True)
            
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

    # Parse methods UNCHANGED from your original
    def parse_course(self, response):
    
        blocks = response.xpath('//td[@nowrap and .//a]')
        for i, block in enumerate(blocks):
            class_number = block.xpath('.//a/text()').get()
            if class_number:
                section = class_number.split('-')[-1]
                desc_search = class_number.split('-')[0]
                title = block.xpath('following-sibling::td[1]//a//text()').get()
                crn = block.xpath('following-sibling::td[2]//font/text()').get()
                capacity = block.xpath('following-sibling::td[3]//font/text()').get()
                availability = block.xpath('following-sibling::td[5]//font/text()').get()
                instructor = block.xpath('following-sibling::td[6]//font/text()').get()
                course_dates = block.xpath('following-sibling::td[7]//font/text()').get()
                location = block.xpath('following-sibling::tr[1]/td[4]//font/text()').get()

                des_url = block.xpath('following-sibling::td[1]//a//@href').get('')
                des_description = ""
                des_res = requests.get(des_url,timeout=30)
                des_response = Selector(text=des_res.text)
                des_blocks = des_response.xpath('//div[@class="courseblock"]')
                for des_block in des_blocks:
                    des_title = des_block.xpath('.//p[1]//text()').get('').replace('\xa0',' ')
                    if desc_search in des_title:
                        des_description = ' '.join(des_block.xpath('.//p[2]/text()').getall()).replace('\n','').strip()
                        break 
                course_name = f"{class_number or ''} {title or ''}".strip()
                enroll = f"{availability or ''}/{capacity or ''}"
                if course_name != '':
                    self.course_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": 'https://prodmyinfo.montana.edu/pls/bzagent/bzskcrse.PW_ListSchClassSimple',
                            "Course Name": course_name,
                            "Course Description": des_description,
                            "Class Number": crn,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": enroll,
                            "Course Dates": course_dates,
                            "Location": location,
                            "Textbook/Course Materials": "",
                        }
                    )

        # SAVE
        course_df = pd.DataFrame(self.course_rows)
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
        
        rows = response.xpath('//table[@class="table_dark table table-striped table-condensed"]//tr')
        for row in rows:
            name = row.xpath('./td[1]//text()').get(default='').strip()
            title = row.xpath('./td[2]//text()').get(default='').strip()
            phone = row.xpath('./td[4]//a/@href').get(default='').replace('tel:', '').strip()
            email = row.xpath('./td[1]//a/@href').get(default='').replace('mailto:', '').strip()
            if name:
                self.directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                })

        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        Parse academic calendar PDF and extract:
        - Term Name
        - Term Date
        - Term Date Description
        """

        current_term_name = "" 
        blocks = response.xpath('//tbody//tr')
        for block in blocks:
            term_name = block.xpath('.//th//text()').get('')
            term_date = block.xpath('.//td[1]//text()').getall()
            term_date = ' '.join(term_date)
            term_desc = block.xpath('.//td[2]//text()').getall()
            term_desc = ' '.join(term_desc)
            if term_name:
                current_term_name = term_name.strip()
            else:
                term_name = current_term_name   # reuse previous
            
            if term_date != '':
                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": term_name,
                    "Term Date": term_date,
                    "Term Date Description": term_desc
                })


        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
