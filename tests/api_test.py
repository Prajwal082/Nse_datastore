import json
import requests

key = 'HDFC'
host = "https://www.nseindia.com/api/historical/securityArchives?from=30-06-2023&to=30-12-2023&symbol=HDFC&dataType=priceVolumeDeliverable&series=ALL"
path = '/api/historical/securityArchives'
params = f'?from=30-01-2024&to=30-06-2024&symbol={key}&dataType=priceVolumeDeliverable&series=ALL'

payload = {"form" : "30-07-2024","to" : "02-08-2024","symbol" : "HDFC","dataType" : "priceVolumeDeliverable", "series" : "ALL"}
headers = {'User-Agent' :'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'}

session_object = requests.session()

response = session_object.get(url="https://www.nseindia.com",params=payload,headers=headers)

# response = session_object.get(url=host,headers=headers)

print(response.status_code)

d = response.json()
print(d)
# # Check the status and read the response body
# if response.status_code == 200:
#     # Parse the JSON response (if applicable)
#     json_data = json.loads(response)
#     print(json.dumps(json_data, indent=4))
# else:
#     print(f"Failed to retrieve data: {response.status}")

