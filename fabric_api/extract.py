import requests
from .config import base_url, headers

def get_company_data():
    companies = []
    page = 1
    page_size = 100

    while True:
        url = f"{base_url}/company/companies?page={page}&pageSize={page_size}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            companies.extend(data)
            if len(data) < page_size:
                break
            page += 1
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            break
    return companies
