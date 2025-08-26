import os 
import logging
import requests
import time
import re
import urllib.robotparser as robotparser
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import OrderedDict
from typing import Iterable,Any
import pandas as pd


Dataset_id="76t5-zqzr"   # APIendpointID
V_url=f"https://data.seattle.gov/resource/{Dataset_id}.json"  #json form of API endpoint
MAX_PAGES = int(os.getenv("MAX_PAGES", "2000"))    #Maximum pages for loop   
Page_limit=int(os.getenv("PAGE_LIMIT", "1000"))# num of rows to request per page from API
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.5")) #pause btw delays
TIMEOUT = int(os.getenv("TIMEOUT", "30"))   
SHOW_N = int(os.getenv("SHOW_N", "50"))  #how many links to print
APP_TOKEN = os.environ.get("SODA_APP_TOKEN", "").strip()  # this looks up the token from env SODA_AOO_TOKEN , if it's not set it'll fallback empty 
UA = "EducationalCrawler" #useragent
link_host="https://cosaccela.seattle.gov/portal/customize/LinkToRecord.aspx"   #base url for seattle accela portal

prefixes=("https://services.seattle.gov/portal/customize/linktorecord.aspx","https://cosaccela.seattle.gov/portal/customize/linktorecord.aspx")


session = requests.Session()
session.headers.update({"User-Agent": f"{UA}/1.0 (+contact: veera@gmail.com)"})
if APP_TOKEN:
    session.headers.update({"X-App-Token": APP_TOKEN}) #it'll make highervolume queries without hitting anonymous rate limits
    


def _uniq(seq:Iterable[str]) -> list[str]:  #takes list of strings and preserves original order and keeps only first occurence
    return list(OrderedDict.fromkeys(seq))

def looks_like_target (u:str, prefixes: tuple[str,...])->bool: #checks whether given str looks like one of our target permit links
    if not u:
        return False
    low = u.lower().strip()
    return "altid=" in low and any(low.startswith(p.lower()) for p in prefixes)
"""return true only if url contains altid and and strtas with one known prefixes"""
def normalize_prefixes(V_url:str)->tuple[str,...]:
    """take base url and produce multiple safe variants for matching 
    useful bcoz sometimes links differ by case or hht/https scheme"""
    p=V_url.rstrip("/")  #remove any slash(/) so comparsions are consistent
    variants = {
        base,
        base.lower(),
        base.replace("http://", "https://"),
        base.replace("https://", "http://"),
        base.replace("HTTPS://", "https://"),
    }
    return tuple(v.lower().rstrip("/") for v in variants)
"""return them as a tuple , all lowercasedand withouttrailing slashes"""


def collect_all_links_from_permitnums(dataset_api_url: str) -> list[str]:
    """
    Page through the Seattle permits dataset and build a LinkToRecord URL
    for every row using its 'permitnum' field.
    Returns a list of full portal links.
    """
    links = []  #stores final list of urls
    offset = 0  #start reading rows at beginning
    while True:
        params = {
            "$select": "permitnum",   # only fetch permitnum field
            "$limit": str(Page_limit), #num of rows per page 
            "$offset": str(offset)     # where to start in dataset
        }
        r = session.get(dataset_api_url, params=params, timeout=TIMEOUT)   #mak the request to dataset API
        r.raise_for_status()  #raise for error if request failed
        rows = r.json()   #parse JSON response into python objects
        if not rows:  # stop if no rows returned
            break

        for row in rows:   #for each row returned  get permitnum field
            alt = row.get("permitnum")
            if alt:
                links.append(f"{link_host}?altId={alt}")  #build the full acela portallink with altid

        if len(rows) < Page_limit:
            break
        offset += Page_limit

    return links




def crawl_and_print_target_urls(dataset_api_url: str, show_n: int = 50, excel_file: str = "links.xlsx") -> None:
    """crawls the dataset api to build permit portal links and then print preview 
    """
    try:
        links = collect_all_links_from_permitnums(dataset_api_url)
    except Exception as e:
        print(f"[error] loading failed:{e}")
        links = []

    print(f"Source used: {dataset_api_url}")
    print(f"Built {len(links)} LinkToRecord URLs\n")

    to_show = links[:show_n]  # how many links to print for preview
    if to_show:
        print(f"First {len(to_show)} links:")
        for u in to_show:
            print(u)
    if links:
        df = pd.DataFrame(links, columns=["LinkToRecord"])
        df.to_excel(excel_file, index=False)
        print(f"\nSaved {len(links)} links to {excel_file}")
    else:
        print("\nNo links to save.")

start_url = "https://data.seattle.gov/Built-Environment/Building-Permits/76t5-zqzr/data_preview"  
V_URL="https://services.seattle.gov/portal/customize/LinkToRecord.aspx"

crawl_and_print_target_urls(V_url, show_n=50)
