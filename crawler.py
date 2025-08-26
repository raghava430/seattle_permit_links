import os                   # for file paths, environment variables, directory creation
import requests             # to send HTTP requests to Seattle's open data API
import pandas as pd         # for handling Excel files
import boto3                # for aws services
from moto import mock_aws as moto_mock  # Moto library fakes AWS services in memory

# Configuration

DATASET_ID = "76t5-zqzr"  # Seattle Building Permits dataset ID on data.seattle.gov

# API endpoint for fetching data (JSON format)
DATASET_API_URL = f"https://data.seattle.gov/resource/{DATASET_ID}.json"

# Base URL for Seattle’s “LinkToRecord” portal (permits are opened here)
LINK_HOST = "https://services.seattle.gov/portal/customize/LinkToRecord.aspx"

MAX_PAGES  = int(os.getenv("MAX_PAGES", "2000"))          # stop after this many API pages
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "1000"))         # rows per API call
TIMEOUT    = int(os.getenv("TIMEOUT", "30"))              # HTTP timeout (seconds)
SHOW_N     = int(os.getenv("SHOW_N", "50"))               # how many preview links to print
UA         = "EducationalCrawler"                         # User-Agent string


DEFAULT_EXCEL = r"C:\Users\ragha\Desktop\Project1\crawler\links.xlsx"  #Path for Excel file output

# Mock S3 upload settings 
MOCK_BUCKET = "my-offline-bucket"                 # bucket name (all lowercase)
MOCK_KEY    = "project1/outputs/links.xlsx"       # “folder/key” inside the bucket



# Requests Session setup
# Make one session object and reuse it 
session = requests.Session()

# Add User-Agent header
session.headers.update({"User-Agent": f"{UA}/1.0 (+contact: veera@gmail.com)"})

# Function: collect_all_links_from_permitnums

def collect_all_links_from_permitnums() -> list[str]:
    """
    Pages through the Seattle permits dataset.
    For every record, extracts the permit number and builds a LinkToRecord URL.
    Returns a list of portal links.
    """
    links: list[str] = []    # store all permit URLs here
    offset = 0               # API offset --like a cursor, moves forward
    pages_seen = 0           

    while True:  # loop until no rows left or max pages reached
        if pages_seen >= MAX_PAGES:
            break
        pages_seen += 1

        # API query parameters: only ask for "permitnum"
        params = {
            "$select": "permitnum",        # only need this column
            "$limit": str(PAGE_LIMIT),     # number of rows per call
            "$offset": str(offset),        # skip rows already seen
        }

        # Send request to Seattle Open Data API
        r = session.get(DATASET_API_URL, params=params, timeout=TIMEOUT)
        r.raise_for_status()   # error if response isn’t 200 OK
        rows = r.json()        # parse JSON response

        if not rows:  # stop if no data
            break

        # Build LinkToRecord URL for each permit number
        for row in rows:
            alt = row.get("permitnum")
            if alt:
                links.append(f"{LINK_HOST}?altId={alt}")

        # Stop if we reached the last page (fewer rows than limit)
        if len(rows) < PAGE_LIMIT:
            break

        # Otherwise move the offset forward and loop again
        offset += PAGE_LIMIT

    return links


def upload_excel_to_mock_s3(excel_file: str,
                            bucket: str = MOCK_BUCKET,
                            key: str = MOCK_KEY) -> None:
    """
    OFFLINE S3 upload using Moto.
    - Creates a fake S3 bucket
    - Uploads the Excel file
    and  Verifies upload by checking file size
    """
    if not os.path.exists(excel_file):  #check if excel file exists locally
        raise FileNotFoundError(f"File not found: {excel_file}")  # If not found, stop execution and raise error

    print("\n[mock-upload] Uploading Excel to mock S3 (Moto)...")

    # Moto replaces all AWS S3 calls with an in-memory fake
    with moto_mock():
        # Initialize fake S3 client
        s3 = boto3.client("s3", region_name="us-east-1")

        # Create fake bucket to store objects
        s3.create_bucket(Bucket=bucket)

        # Upload file into fake S3 bucket at given key
        s3.upload_file(
            Filename=excel_file, #local path to file
            Bucket=bucket,   #fake bucket name
            Key=key,         # "path/filename" inside bucket
            ExtraArgs={
                "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            },
        )

        #    Verification step 
        # Fetch metadata for uploaded object
        head = s3.head_object(Bucket=bucket, Key=key)  #Ask S3 for object metadata (head request)
        remote_size = head["ContentLength"]         # Extract the uploaded file size from fake S3.
        local_size = os.path.getsize(excel_file)    # Get the actual local file size from disk.

        print(f"[mock-upload] remote size = {remote_size} | local size = {local_size}")  #Print both sizes so user can confirm upload integrity.

        # Compare size ---- If equal → upload successful. If not → something went wrong.
        if remote_size == local_size:
            print("[mock-upload] ✔ size matches")
        else:
            print("[mock-upload] ✖ size mismatch")
# Function: crawl_and_print_target_urls

def crawl_and_print_target_urls(show_n: int = SHOW_N, excel_file: str = DEFAULT_EXCEL) -> None:
    """
    - Calls the API to collect permit links
    - Prints first few links
    - Saves all links to Excel
    - Uploads that Excel file to fake S3 (Moto)
    """
    try:  #Call the API function to collect all permit links
        links = collect_all_links_from_permitnums()
    except Exception as e:   #If something goes wrong like network error or any errors, catch and log it
        print(f"[error] loading failed: {e}")
        links = []   # fall back to empty list so program continues
        
#  Print summary info: which dataset was used + total number of links found
    print(f"Source used: {DATASET_API_URL}")  
    print(f"Built {len(links)} LinkToRecord URLs\n")

    # Print a preview of first N links
    to_show = links[:show_n]   ## slice list to only show first few
    if to_show:     ## only print if list isn’t empty
        print(f"First {len(to_show)} links:")
        for u in to_show:   ## print one link per line
            print(u)

            
# Save links to Excel file
    if links:  
        # Put all links in a DataFrame
        df = pd.DataFrame(links, columns=["LinkToRecord"])

        # Ensure the folder exists, then save Excel
        os.makedirs(os.path.dirname(excel_file), exist_ok=True)
        df.to_excel(excel_file, index=False)   # Write DataFrame to Excel file on disk
        print(f"\nSaved {len(links)} links to {excel_file}")

        # Upload Excel to mock S3 (offline)
        upload_excel_to_mock_s3(excel_file)
    else:
        print("\nNo links to save.")   # If no links were collected at all

# Script entrypoint

if __name__ == "__main__":
    # If this file is run directly (not imported), start the crawl
    crawl_and_print_target_urls()
