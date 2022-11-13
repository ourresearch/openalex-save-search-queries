from datetime import datetime, timedelta
import os
import re

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models import SearchQuery


def get_latest_file_path_for_s3():
    """Get the latest log file from S3 bucket based on datetime."""
    # folder: /logs
    # day format: dt=2022-11-13/
    # file format: 2022-11-13-00.tsv.gz
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_date_minus_one_hour = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d-%H")
    bucket = "ourresearch-papertrail"
    folder = f"dt={current_date}"
    file_name = f"{current_date_minus_one_hour}.tsv.gz"
    file_path = f"s3://{bucket}/logs/{folder}/{file_name}"
    return file_path


def process_log_file(session):
    start_time = datetime.now()
    file_path = get_latest_file_path_for_s3()
    chunksize = 100000
    with pd.read_csv(file_path, chunksize=chunksize, sep="\t") as reader:
        for chunk in reader:
            process_chunk(chunk, session)
    end_time = datetime.now()
    print(f"total time: {end_time - start_time}")


def process_chunk(chunk, session):
    for index, row in chunk.iterrows():
        timestamp = row[1]
        service = row[4]  # openalex-api-proxy
        ip_address = row[5]
        service_type = row[8]  # heroku/router
        path = row[9]
        if service == "openalex-api-proxy" and service_type == "heroku/router" and "team@ourresearch.org" in path:
            process_record(ip_address, path, session, timestamp)
    print(f"chunk processed")
    session.commit()


def process_record(ip_address, path, session, timestamp):
    request_path = re.search(r"path=\"(.*?)\"", path)
    if request_path:
        request_path = request_path.group(1)
        query = None
        endpoint = None
        search_type = None
        if "search=" in request_path:
            search_type = "search"
            endpoint = get_endpoint(request_path)
            query = re.search(r"search=(.*?)&", request_path)
        elif "/suggest" in request_path:
            search_type = "suggest"
            query = re.search(r"suggest\?q=(.*)&?", request_path)
        if query:
            query = query.group(1)
            save_to_db(timestamp, ip_address, endpoint, search_type, query, session)
            print(f"saving {timestamp} {ip_address} {endpoint} {search_type} {query}")


def get_endpoint(path):
    if "/authors" in path:
        return "authors"
    elif "/concepts" in path:
        return "concepts"
    elif "/institutions" in path:
        return "institutions"
    elif "/venues" in path:
        return "venues"
    elif "/works" in path:
        return "works"


def save_to_db(timestamp, ip_address, endpoint, search_type, query, session):
    existing_record = session.query(SearchQuery).filter_by(query=query,endpoint=endpoint,type=search_type).first()
    if existing_record:
        existing_record.count = existing_record.count + 1
    else:
        search_query = SearchQuery(
            timestamp=timestamp,
            ip_address=ip_address,
            endpoint=endpoint,
            type=search_type,
            query=query,
            count=1,
        )
        session.add(search_query)


if __name__ == '__main__':
    engine = create_engine(os.getenv("DATABASE_URL"))
    session = Session(engine)
    process_log_file(session)
