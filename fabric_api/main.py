import base64
import os

import requests
from dotenv import load_dotenv


def main():
    from .extract import get_company_data
    from .transform import process_and_store_data
    from .upload import upload_to_onelake
    print("Extracting company data from ConnectWise...")
    companies = get_company_data()
    print(f"Retrieved {len(companies)} companies")
    file_name, df = process_and_store_data(companies)
    print(f"Saved data to {file_name}")
    upload_to_onelake(
        file_name=file_name,
        workspace_name='YourWorkspaceName',
        lakehouse_name='YourLakehouseName'
    )

def get_reports():
    load_dotenv()
    url = "https://verk.thekking.is/v4_6_release/apis/3.0/system/reports"
    company = os.getenv("CW_COMPANY", "yourcompany")
    username = os.getenv("CW_USERNAME")
    password = os.getenv("CW_PASSWORD")
    headers = {
        "clientId": os.getenv("CW_CLIENTID"),
        "Accept": "application/vnd.connectwise.com+json; version=2025.1"
    }
    auth = (f"{company}+{username}", password)
    print(f"Requesting: {url}")
    print(f"Headers: {headers}")
    print(f"Auth username: {auth[0]}")
    response = requests.get(url, headers=headers, auth=auth)
    print(f"Status: {response.status_code}")
    print(response.text)
    return response

if __name__ == "__main__":
    # main()
    get_reports()
