import os                   # for file paths, environment variables, directory creation
import requests             # to send HTTP requests to Seattle's open data API
import pandas as pd         # for handling Excel files
import boto3                # for aws services
from moto import mock_aws as moto_mock  # Moto library fakes AWS services in memory
from requests.adapters import HTTPAdapter  
from urllib3.util import Retry
import logging


# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()  #default to INFO if not set
LOG_FILE = os.getenv("LOG_FILE", "crawler.log")    # default log filename 

logging.basicConfig(                                     # set global logging settings
    level=getattr(logging, LOG_LEVEL, logging.INFO),     # use chosen level
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",  #  defining how each log should look 
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),   
        logging.StreamHandler()
    ],
)
logger = logging.getLogger("crawler")    #naming the logger so wecan use everywhere

# Configuration

MAX_PAGES  = int(os.getenv("MAX_PAGES", "2000"))          # stop after this many API pages
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "1000"))         # rows per API call
TIMEOUT    = int(os.getenv("TIMEOUT", "30"))              # HTTP timeout (seconds)
SHOW_N     = int(os.getenv("SHOW_N", "50"))               # how many preview links to print
UA         = "EducationalCrawler"                         # User-Agent string
SODA_TOKEN  = os.getenv("SODA_APP_TOKEN", "").strip()     #API token if available

DEFAULT_EXCEL = r"C:\Users\ragha\Desktop\Project1\crawler\links.xlsx"  #Path for Excel file output

# Mock S3 upload settings 
MOCK_BUCKET = "my-offline-bucket"                 # fake bucket name
MOCK_KEY    = "project1/outputs/links.xlsx"       # path/key of the file inside the bucket


DATASET_ID = input("Enter the dataset ID:").strip()  # ask user for dataset id
if not DATASET_ID:
    raise ValueError("Dataset ID cannot be Empty")   #safety check
# API endpoint for fetching data 
DATASET_API_URL = f"https://data.seattle.gov/resource/{DATASET_ID}.json"  

# Base URL for portal (permits are opened here)
LINK_HOST = "https://services.seattle.gov/portal/customize/LinkToRecord.aspx"



# Requests Session setup
# Make one session object and reuse it 
session = requests.Session()

# Add User-Agent header
session.headers.update({"User-Agent": f"{UA}/1.0 (+contact: veera@gmail.com)"})
if SODA_TOKEN:        # if token exists
    session.headers.update({"X-App-Token": SODA_TOKEN})   # add token header
retry=Retry(
    total=5,
    connect=5,read=5,backoff_factor=1.2,
    status_forcelist=(500,502,503,504),  # retry on server errors
    allowed_methods=frozenset(["GET"])
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://",adapter)  # attaching retry logic for https


# Function: collect_all_links_from_permitnums

def collect_all_links_from_permitnums() -> list[str]:
    """
    Pages through the Seattle permits dataset.
    For every record, extracts the permit number and builds a LinkToRecord URL.
    Returns a list of portal links.
    """
    links: list[str] = []    # store all permit URLs here
    offset = 0               # API offset tells where server to start
    pages_seen = 0           # how many pages we have looked at

    while True:  # loop until no rows left or max pages reached
        if pages_seen >= MAX_PAGES:   # stop if we hit last page
            break
        pages_seen += 1

        # API query parameters: only ask for "permitnum"
        params = {
            "$select": "permitnum",        # only need permitnum field
            "$limit": str(PAGE_LIMIT),     # number of rows per call
            "$offset": str(offset),        # skip rows of already seen
        }

        # Send request to Seattle Open Data API
        try:
            r = session.get(DATASET_API_URL, params=params, timeout=(10,TIMEOUT))
            r.raise_for_status()   # error if response isn’t 200 OK
        except Exception as e:
            logger.error("Request failed at offset {offset}:{e}",exc_info=True)
            break
        logger.debug("Fetched {len(rows)} at offset={offset}")

        try:
            rows=r.json()   # Parse JSON
        except ValueError: 
            logger.error("Invalid json at offset%s", offset, exc_info=True)
            break
        if not rows:    # no rows back mean we are done
            logger.info("No rows returned at offset", offset)
            break
        logger.debug("Fetched %s rows at offset=%s ", len(rows),offset)
        added=0
        for row in rows:
            alt=row.get("permitnum")    #extract permitnum
            if alt:
                links.append(f"{LINK_HOST}?altId={alt}")
                added +=1
        logger.debug("added %s links from this page.",added)
        

        # Stop if we reached the last page (fewer rows than limit)
        if len(rows) < PAGE_LIMIT:
            logger.info("last page detected (rows=%s < limit=%s)", len(rows), PAGE_LIMIT)
            break

        # Otherwise move the offset forward and loop again
        offset += PAGE_LIMIT
    logger.info("crawl finished with %s links collected.", len(links))
    return links

def upload_excel_to_mock_s3(excel_file: str,
                            bucket: str = MOCK_BUCKET,
                            key: str = MOCK_KEY) -> None:
    """
    OFFLINE S3 upload using Moto (mock_aws):
    - create bucket
    - upload object
    - verifies it by mtaching size from remote file and local file
    """
    if not os.path.exists(excel_file):
        logger.error("Excel file not found: %s", excel_file)
        raise FileNotFoundError(f"File not found: {excel_file}")

    logger.info("Uploading Excel to mock S3 (Moto)... file=%s bucket=%s key=%s",
                excel_file, bucket, key)

    import contextlib
# temporarily replace  AWS creds with testing
    @contextlib.contextmanager
    def _fake_creds():
        # ensure boto3 never talks to real AWS even if env creds exist
        old_ak = os.environ.get("AWS_ACCESS_KEY_ID")
        old_sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
        old_tok = os.environ.get("AWS_SESSION_TOKEN")
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ.pop("AWS_SESSION_TOKEN", None)
        try:
            yield
        finally:
            # restore old creds
            if old_ak is not None:
                os.environ["AWS_ACCESS_KEY_ID"] = old_ak
            else:
                os.environ.pop("AWS_ACCESS_KEY_ID", None)
            if old_sk is not None:
                os.environ["AWS_SECRET_ACCESS_KEY"] = old_sk
            else:
                os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
            if old_tok is not None:
                os.environ["AWS_SESSION_TOKEN"] = old_tok

    try:
        with moto_mock(), _fake_creds():   #activates moto+fake creds
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=bucket)

            # Uploaded files using put
            with open(excel_file, "rb") as f:
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=f.read(),
                    ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            # Verify if it is saved 
            listed = s3.list_objects_v2(Bucket=bucket, Prefix=key)
            contents = listed.get("Contents", [])
            if not any(obj.get("Key") == key for obj in contents):
                logger.error("Object not found after put_object. bucket=%s key=%s", bucket, key)
                raise RuntimeError("Object not found in mocked S3 after upload.")

            # Final verification via HEAD by confiriming size
            head = s3.head_object(Bucket=bucket, Key=key)
            remote_size = head["ContentLength"]
            local_size  = os.path.getsize(excel_file)

            logger.info("Mock upload complete. remote=%s bytes | local=%s bytes",
                        remote_size, local_size)
            if remote_size == local_size:
                logger.info("Upload verification: size matches ✔")
            else:
                logger.warning("Upload verification: size mismatch ✖")

    except Exception:
        logger.exception("Mock S3 upload failed.")
        raise


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
        logger.error(" loading failed: %s", e, exc_info=True)
        links = []   # fall back to empty list so program continues
        
#  Print summary info: which dataset was used + total number of links found
    logger.info("Source used:%s", DATASET_API_URL)  
    logger.info("Built %s LinkToRecord URLs.", len(links))

    # Print a preview of first N links
    to_show = links[:show_n]   ## slice list to only show first few
    if to_show:     ## only print if list isn’t empty
        print(f"First {len(to_show)} links:")
        for u in to_show:   ## print one link per line
            print(u)

            
# Save links to Excel file
    if links: 
        try:
            # Put all links in a DataFrame
            df = pd.DataFrame(links, columns=["LinkToRecord"])
            # Ensure the folder exists, then save Excel
            os.makedirs(os.path.dirname(excel_file), exist_ok=True)
            df.to_excel(excel_file, index=False)   # Write DataFrame to Excel file on disk
            logger.info("Saved %s links to %s", len(links),excel_file)
        except Exception:
            logger.exception("Failed to save excel file")
            return
        
        try:
            # Upload Excel to mock S3 (offline)
            upload_excel_to_mock_s3(excel_file)
        except Exception:
            logger.exception("Mock s3 upload got an error")
    else:
        logger.warning("No links to save.")   # If no links were collected at all

# Script entrypoint

if __name__ == "__main__":
    # If this file is run directly (not imported), start the crawl
    crawl_and_print_target_urls()
