from datetime import datetime, timedelta
import os
import re

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models import APIUsage


def process_log_file(session, file_path):
    print(f"processing {file_path}")
    chunksize = 100000
    with pd.read_csv(file_path, chunksize=chunksize, sep="\t") as reader:
        for chunk in reader:
            process_chunk(chunk, session)


def process_chunk(chunk, session):
    for index, row in chunk.iterrows():
        service = row[4]  # openalex-api-proxy
        service_type = row[8]  # heroku/router
        path = row[9]
        if service == "openalex-api-proxy" and service_type == "heroku/router":
            process_record(path, session)
    session.commit()


def process_record(path, session):
    request_path = re.search(r"path=\"(.*?)\"", path)
    if request_path:
        request_path = request_path.group(1)
        if "mailto=" in request_path:
            email = re.search(r"mailto=([^&]*)", request_path)
        elif "email=" in request_path:
            email = re.search(r"email=([^&]*)", request_path)
        else:
            email = None

        if email:
            email = email.group(1)
            domain1 = email.split("%40")
            domain2 = email.split("@")
            if len(domain1) > 1:
                domain = domain1[1]
            elif len(domain2) > 1:
                domain = domain2[1]
        else:
            domain = None

        if not email:
            email = "no email"
        if not domain:
            domain = "no domain"

        if "%40" in email:
            email = email.replace("%40", "@")

        save_to_db(email, domain, session)


def get_file_path_for_s3(date, hour):
    """Get the latest log file from S3 bucket based on datetime."""
    # folder: /logs
    # day format: dt=2022-11-13/
    # file format: 2022-11-13-00.tsv.gz
    bucket = "ourresearch-papertrail"
    current_date = date.strftime("%Y-%m-%d")
    folder = f"dt={current_date}"
    file_name = f"{current_date}-{hour:02d}.tsv.gz"
    file_path = f"s3://{bucket}/logs/{folder}/{file_name}"
    return file_path


def save_to_db(email, domain, session):
    existing_record = session.query(APIUsage).filter_by(email=email, domain=domain).first()
    if existing_record:
        existing_record.count = existing_record.count + 1
    else:
        api_usage = APIUsage(
            email=email,
            domain=domain,
            count=1,
        )
        session.add(api_usage)


if __name__ == '__main__':
    engine = create_engine(os.getenv("DATABASE_URL"))
    session = Session(engine)
    today = datetime.utcnow() - timedelta(days=1)
    for i in range(30):
        day = today - timedelta(days=i)
        for hour in range(24):
            file_path = get_file_path_for_s3(day, hour)
            process_log_file(session, file_path)
