import requests
from bs4 import BeautifulSoup
import csv
import io
import boto3
import json
from concurrent.futures import ThreadPoolExecutor
import snowflake.connector
from botocore.exceptions import ClientError

# ----------- GET SECRETS -----------

def get_secret():
    secret_name = "nhl-pipeline-secret"
    region_name = "ap-south-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = json.loads(response['SecretString'])
    return secret


# ----------- SCRAPING -----------

BASE_URL = "https://www.scrapethissite.com/pages/forms/?page_num={}"
MAIN_URL = "https://www.scrapethissite.com/pages/forms/"

def get_total_pages():
    res = requests.get(MAIN_URL)
    soup = BeautifulSoup(res.text, "html.parser")

    pages = soup.select("ul.pagination li a")
    
    # ADDED .strip() to both the condition and the integer conversion
    nums = [int(p.text.strip()) for p in pages if p.text.strip().isdigit()]

    return max(nums) if nums else 1


def scrape_page(page):
    res = requests.get(BASE_URL.format(page))
    soup = BeautifulSoup(res.text, "html.parser")

    rows = soup.find_all("tr", class_="team")
    data = []

    for row in rows:
        cols = row.find_all("td")

        if len(cols) >= 9:
            # Grab the text, check if it exists, otherwise default to 0
            ot_text = cols[4].text.strip()
            ot_losses = int(ot_text) if ot_text else 0

            data.append({
                "Team": cols[0].text.strip(),
                "Year": int(cols[1].text.strip()),
                "Wins": int(cols[2].text.strip()),
                "Losses": int(cols[3].text.strip()),
                "OT_Losses": ot_losses, # Use the safe variable here
                "Win_PCT": float(cols[5].text.strip()),
                "GF": int(cols[6].text.strip()),
                "GA": int(cols[7].text.strip()),
                "GD": int(cols[8].text.strip())
            })

    print(f"✅ Page {page} scraped")
    return data


def scrape_all():
    total_pages = get_total_pages()
    all_data = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(scrape_page, range(1, total_pages + 1))

        for r in results:
            all_data.extend(r)

    return all_data


# ----------- S3 -----------

def upload_to_s3(data, bucket, key):
    s3 = boto3.client("s3")

    csv_buffer = io.StringIO()

    writer = csv.DictWriter(csv_buffer, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )

    print(f"✅ Uploaded to S3: s3://{bucket}/{key}")


# ----------- SNOWFLAKE -----------

def load_to_snowflake(data, sf_config):
    conn = snowflake.connector.connect(**sf_config)
    cursor = conn.cursor()

    # Clear table
    cursor.execute("DELETE FROM NHL_RAW")

    # 1. Transform your list of dictionaries into a list of tuples
    insert_data = [
        (
            row["Team"], row["Year"], row["Wins"], row["Losses"],
            row["OT_Losses"], row["Win_PCT"], row["GF"], row["GA"], row["GD"]
        )
        for row in data
    ]

    # 2. Use executemany with %s placeholders. 
    # Snowflake will automatically handle apostrophes safely.
    cursor.executemany("""
        INSERT INTO NHL_RAW VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, insert_data)

    # Create winners table
    cursor.execute("""
        CREATE OR REPLACE TABLE NHL_WINNERS AS
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Wins DESC) rn
            FROM NHL_RAW
        )
        WHERE rn = 1
    """)

    conn.close()
    print("✅ Data loaded into Snowflake")


def get_winners(sf_config):
    conn = snowflake.connector.connect(**sf_config)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT Year, Team, Wins, Win_PCT, GF, GA, GD
        FROM NHL_WINNERS
    """)

    cols = [c[0] for c in cursor.description]

    data = []
    for row in cursor.fetchall():
        data.append(dict(zip(cols, row)))

    conn.close()
    return data


# ----------- EMAIL -----------

def send_email(row, sender, recipient):
    ses = boto3.client("ses", region_name="ap-south-1")

    subject = f'Notification of Hockey Championship winner of {row["YEAR"]}'

    body = f"""
Hi Nirmalya,

The winner of The Hockey Championship of {row["YEAR"]} is {row["TEAM"]} with a stunning {row["WINS"]} wins.
Win % is {row["WIN_PCT"]}
GF is {row["GF"]}
GA is {row["GA"]}
GD is {row["GD"]}

Thanks
Data Analysis Team
"""

    ses.send_email(
        Source=sender,
        Destination={"ToAddresses": [recipient]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body}}
        }
    )

    print(f"📧 Email sent for {row['YEAR']}")


# ----------- MAIN HANDLER -----------

def lambda_handler(event, context):
    print("🚀 Pipeline started")

    # 🔐 Get secrets
    secrets = get_secret()

    # Config from secrets
    sf_config = {
        "user": secrets["snowflake_user"],
        "password": secrets["snowflake_password"],
        "account": secrets["snowflake_account"],
        "warehouse": secrets["snowflake_warehouse"],
        "database": secrets["snowflake_database"],
        "schema": secrets["snowflake_schema"]
    }

    bucket = secrets["s3_bucket"]
    key = "nhl/nhl_stats.csv"

    sender = secrets["sender_email"]
    recipient = secrets["recipient_email"]

    # 🚀 Run pipeline
    data = scrape_all()

    upload_to_s3(data, bucket, key)

    load_to_snowflake(data, sf_config)

    winners = get_winners(sf_config)

    for row in winners:
        send_email(row, sender, recipient)

    print("✅ Pipeline completed")

    return {"status": "SUCCESS"}