from datetime import datetime
import re

import pandas as pd


def get_latest_file_path_for_s3():
    """Get the latest log file from S3 bucket based on datetime."""
    # folder: /logs
    # day format: dt=2022-11-13/
    # file format: 2022-11-13-00.tsv.gz
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_date_time = datetime.now().strftime("%Y-%m-%d-%H")
    bucket = "ourresearch-papertrail"
    folder = f"dt={current_date}"
    file_name = f"{current_date_time}.tsv.gz"
    file_path = f"s3://{bucket}/logs/{folder}/{file_name}"
    return file_path


def process_log_file():
    file_path = get_latest_file_path_for_s3()
    chunksize = 10000
    with pd.read_csv(file_path, chunksize=chunksize, sep="\t") as reader:
        for chunk in reader:
            process_chunk(chunk)


def process_chunk(chunk):
    for index, row in chunk.iterrows():
        timestamp = row[1]
        service = row[4]  # openalex-api-proxy
        ip_address = row[5]
        service_type = row[8]  # heroku/router
        path = row[9]
        if service == "openalex-api-proxy" and service_type == "heroku/router" and "team@ourresearch.org" in path:
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
                    print(f"{search_type}\t{endpoint}\t{query}")


def find_suggest_query(path):
    query = re.search(r"path=\"(.*?)\"", path)
    print(query.group(1))


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


if __name__ == '__main__':
    process_log_file()
