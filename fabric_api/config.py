import base64

# Configuration and authentication for ConnectWise PSA API
company_id = 'your_company_id'
public_key = 'your_public_key'
private_key = 'your_private_key'
client_id = 'your_client_id'  # Optional depending on your setup

# Create the authorization header
auth_string = f"{company_id}+{public_key}:{private_key}"
encoded_auth = base64.b64encode(auth_string.encode()).decode()
headers = {
    'Authorization': f'Basic {encoded_auth}',
    'clientId': client_id,
    'Content-Type': 'application/json'
}

# Base URL for the API
base_url = 'https://your-instance.connectwisedev.com/v4_6_release/apis/3.0'
