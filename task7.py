if API giving only 100 records not full what will you do to get all records

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_single_page(page_num, base_url, page_size=100):
    """Fetch a single page of data"""
    params = {'page': page_num, 'limit': page_size}
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data.get('records', data.get('data', data.get('items', [])))
        
        # Add metadata
        for record in records:
            record['_page'] = page_num
            
        return records
    except Exception as e:
        print(f"Error fetching page {page_num}: {e}")
        return []

def fetch_all_pages_parallel(base_url, total_pages=None):
    """
    Fetch multiple pages in parallel using ThreadPoolExecutor
    """
    # First, get first page to determine total pages if unknown
    first_page = fetch_single_page(1, base_url)
    if not first_page:
        return spark.createDataFrame([], StructType([]))
    
    # Try to get total pages from response
    response = requests.get(base_url, params={'page': 1, 'limit': 100})
    total_pages = total_pages or response.json().get('total_pages', 10)
    
    # Fetch pages in parallel
    all_records = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_single_page, page, base_url): page 
            for page in range(2, total_pages + 1)
        }
        
        # Add first page records
        all_records.extend(first_page)
        
        for future in as_completed(futures):
            records = future.result()
            all_records.extend(records)
    
    # Convert to Spark DataFrame
    return spark.createDataFrame(all_records)

# Usage
df = fetch_all_pages_parallel('https://api.example.com/users')