# %%
import base64
import fitz
import json
import os
import gspread
import pandas as pd
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
import requests
from PyPDF2 import PdfMerger
from datetime import datetime, timedelta
import psycopg2
import pytz

def handler(event, context):
    "this function is triggered after everything else ran"
def gdrive_convert_to_download_url(preview_url):
    base_url = "https://drive.google.com/uc?export=download&confirm=no_antivirus&id="
    start = preview_url.find('/d/') + 3
    end = preview_url.find('/view')
    file_id = preview_url[start:end]
    download_url = base_url + file_id
    return download_url

def download_pdf_content(url):
    dl_url = gdrive_convert_to_download_url(url)
    response = requests.get(dl_url)
    if response.status_code == 200:
        return response.content
    else:
        # print(f"Error downloading file: {response.status_code}")
        return None

def merge_pdfs(paths, output_path):
    merger = PdfMerger()
    for path in paths:
        merger.append(path)
    merger.write(output_path)
    merger.close()


def fetch_data_from_sheets(wks):
    try:
        all_values = wks.get_all_values()
        return pd.DataFrame(all_values[1:], columns=all_values[0])
    except Exception as e:
        logging.error(f"Error fetching data from sheets: {e}")
        return pd.DataFrame()

def encode_image(image_path):
  with open(image_path, "rb") as image_file:
    return base64.b64encode(image_file.read()).decode('utf-8')

def parse_invoice(path):
  image_path = path
  # Getting the base64 string
  base64_image = encode_image(image_path)
  headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
  }
  payload = {
    "model": "gpt-4-vision-preview",
    "messages": [
      {
        "role": "user",
        "content": [
          {
            "type": "text",
            "text": """
  [{"Invoice #": ""  }]
  Given this JSON object, read this image and populate the items. DO NOT RETURN ANY INFORMATION OUTSIDE OF JSON FORMAT, WHATSOEVER, UNDER ANY CIRCUMSTANCES!
  The invoice # be numbers or decimals only. Return absolutely nothing except the Invoice #, WHATSOEVER, UNDER ANY CIRCUMSTANCES!
  """
          },
          {
            "type": "image_url",
            "image_url": {
              "url": f"data:image/jpeg;base64,{base64_image}",
              "detail": "high"
            }
          }
        ]
      }
    ],
    "max_tokens": 300
  }
  response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
  response = response.json()
  # parse the json into python object
  # print(response)
  return(response['choices'][0]['message']['content'])

def convert_string_to_json(input_string):
    # Strip the surrounding code block markers
    json_str = input_string.strip('```').strip('json\n[').strip(']\n```')
    # Convert the string to JSON
    json_obj = json.loads(json_str)
    return json_obj

def return_data(pdf_path, output_folder):
    """
    Convert a PDF into PNG files. Each page of the PDF will be converted to a separate PNG file using PyMuPDF (fitz).
    
    Parameters:
    - pdf_path: Path to the input PDF file.
    - output_folder: Folder where the output PNG files will be saved.
    
    Returns:
    A DataFrame containing data extracted from the PDF.
    """
    doc = fitz.open(pdf_path)
    data_list = []  # Initialize an empty list to store data
    
    for i in range(len(doc)):
        if (i+1)%5 == 0:
          print(f'{i+1}/{len(doc)}:')
        page = doc.load_page(i)  # number of page
        pix = page.get_pixmap()  # render page to an image
        output_path = f"{output_folder}/page_{i+1}.png"
        pix.save(output_path)
        
        # Assuming your parsing and conversion functions return a dictionary
        json_invoice = convert_string_to_json(parse_invoice(output_path))
        if isinstance(json_invoice, dict):
            data_list.append(json_invoice)
        else:
            print("Warning: Parsed data is not a dictionary. Data:", json_invoice)
    
    doc.close()  # Remember to close the document
    
    # Create a DataFrame from the list of dictionaries
    night_data = pd.DataFrame(data_list)
    
    return night_data

# %%
api_key = "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"
# Function to encode the image

json_file_path = './service_account.json'

# Define the scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

# Authenticate using the service account file
credentials = service_account.Credentials.from_service_account_file(
        json_file_path, scopes=SCOPES)

# Build the service object for the Drive API
service = build('drive', 'v3', credentials=credentials)

# Use the path to the JSON file with gspread
sa = gspread.service_account(filename=json_file_path)
sh = sa.open("PODs")

# Access the worksheet
wks = sh.worksheet("Sheet1")

# Assuming `fetch_data_from_sheets` returns a DataFrame with the links
data = fetch_data_from_sheets(wks)
links = data['Scans'].iloc[0:]  # Adjusted to Python's zero-based indexing

temp_files = []
print(enumerate(links))

downloads = []
for index, link in enumerate(links):
    pdf_content = download_pdf_content(link)
    if pdf_content:
        downloads.append(link)
        # print(f'Downloading {link}')
        temp_path = f'/tmp/temp_{index}.pdf'
        with open(temp_path, 'wb') as f:
            f.write(pdf_content)
            # print(f'saved {temp_path}')
        temp_files.append(temp_path)
    else:
        # print(f"Skipping link {link}")
        continue
print(f'Downloaded: {downloads}')

pdf_path = '/tmp/combined_pdfs.pdf'
merge_pdfs(temp_files, pdf_path)

# Optionally, clean up temporary files
for temp_file in temp_files:
    time.sleep(2)
    os.remove(temp_file)

print(f"Combined PDF saved as {pdf_path}")

process_scans = return_data(pdf_path, "/tmp")
tonight_printed = list(process_scans['Invoice #'])

# %% [markdown]
# #GETTING THE INVOICES FROM QB

# %%


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Database:
    def __init__(self, dbname, user, password, host, port):
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.cur = self.conn.cursor()
        except psycopg2.Error as e:
            print(f"Error connecting to the database: {e}")
            
    def execute_query(self, query, data=None):
        try:
            if data:
                self.cur.execute(query, data)
            else:
                self.cur.execute(query)
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")

    def fetch_data(self, query):
        try:
            self.cur.execute(query)
            rows = self.cur.fetchall()
            return rows
        except psycopg2.Error as e:
            print(f"Error fetching data: {e}")

    def insert_data(self, table, columns, values):
        try:
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])})"
            self.cur.execute(query, values)
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error inserting data: {e}")

    def delete_data(self, table, condition):
        try:
            query = f"DELETE FROM {table} WHERE {condition}"
            self.cur.execute(query)
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"Error deleting data: {e}")

    def update_data(self, table, set_values, condition, return_columns='*'):
        try:
            set_query = ", ".join([f"{column} = %s" for column in set_values.keys()])
            query = f"UPDATE {table} SET {set_query} WHERE {condition} RETURNING {return_columns}"
            self.cur.execute(query, list(set_values.values()))
            updated_row = self.cur.fetchone()
            self.conn.commit()
            return updated_row
        except psycopg2.Error as e:
            print(f"Error updating data: {e}")
        return None


    def close_connection(self):
        try:
            self.cur.close()
            self.conn.close()
        except psycopg2.Error as e:
            print(f"Error closing connection: {e}")

database = Database(
    dbname = "postgres",
    user = "master",
    password= "masterpassword123",
    host = "burnsed.cscnnhkxnqyb.us-east-2.rds.amazonaws.com",
    port = "5432"
)

def check_and_refresh_tokens():
        if not is_token_valid():
            refresh_tokens()

        credentials = get_quickbooks_credentials()

        if credentials["last_token_refresh_time"] is None or \
                (datetime.utcnow() - credentials["last_token_refresh_time"]).total_seconds() > 86400:
            refresh_tokens()
    

# refreshing outdated tokens
def refresh_tokens():
    try:
        refresh_token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

        credentials = get_quickbooks_credentials()

        refresh_data = {
            "grant_type": "refresh_token",
            "refresh_token": credentials["refresh_token"],
        }

        response = requests.post(refresh_token_url, data=refresh_data, auth=(
            credentials["client_id"], credentials["client_secret"]))
        
        # print(response.content)

        if response.status_code == 200:
            token_data = response.json()
            # Update the database with the new token information
            update_token_info(token_data)
        else:
            error_message = f"Error refreshing tokens: {response.status_code}, {response.text}"

    except Exception as e:
        error_message = f"Error refreshing tokens: {str(e)}"

# checking if the current tokens (access token and refresh token) are valid
def is_token_valid():
    credentials = get_quickbooks_credentials()

    if credentials["access_token"] == "" or credentials["refresh_token"] == "":
        return False
    current_time = datetime.utcnow()
    return credentials.get("access_token_expiry_time", current_time) > current_time

# creating headers with valid access token
def get_authenticated_headers():
    credentials = get_quickbooks_credentials()

    if is_token_valid():
        return {
            "Authorization": "Bearer " + credentials["access_token"],
            "Content-Type": "application/json",
        }
    else:
        refresh_tokens()
        credentials = get_quickbooks_credentials()
        return {
            "Authorization": "Bearer " + credentials["access_token"],
            "Content-Type": "application/json",
            "Accept":"application/json"
        }

# getting qb credentials from db
def get_quickbooks_credentials():
    query = "SELECT * FROM quickbooks_credentials_prod WHERE id = 1"
    result = database.fetch_data(query)
    if result:
        return {
            "client_id": result[0][1],
            "client_secret": result[0][2],
            "authorization_code": result[0][3],
            "realm_id": result[0][4],
            "access_token": result[0][5],
            "refresh_token": result[0][6],
            "last_token_refresh_time": result[0][7]
        }
    else:
        return None

# updating qb credentials in db
def update_token_info(token_data):
    query = """
            UPDATE quickbooks_credentials_prod
            SET access_token = %s, refresh_token = %s, access_token_expiry_time = %s, last_token_refresh_time = %s
            WHERE id = 1
        """
    access_token = token_data["access_token"]
    refresh_token = token_data["refresh_token"]
    expires_in = token_data["expires_in"]
    expiration_time = datetime.utcnow() + timedelta(seconds=expires_in)
    last_token_refresh_time = datetime.utcnow()

    database.execute_query(query, (access_token, refresh_token, expiration_time, last_token_refresh_time))


###################################################################################################################################################
###################################################################################################################################################

def invoices_currently_in_qb(days):
    invoice_numbers = []  # Use a separate list for storing just the invoice numbers
    check_and_refresh_tokens()  # Make sure this function is properly defined elsewhere
    headers = get_authenticated_headers()  # Make sure this function is properly defined elsewhere
    credentials = get_quickbooks_credentials()  # Make sure this function is properly defined elsewhere

    est = pytz.timezone('US/Eastern')
    # 3 PM EASTERN from two days ago
    todayinvoices = (datetime.now(est) - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    todayinvoices_str = todayinvoices.isoformat()

    sql = f"SELECT * FROM Invoice WHERE TxnDate >= '{todayinvoices_str}' ORDER BY TxnDate MAXRESULTS 1000"
    # print(sql)
    base_url = 'https://quickbooks.api.intuit.com'
    api_url = f"/v3/company/{credentials['realm_id']}/query?query={sql}&minorversion=69"
    url = base_url + api_url

    try:
        response = requests.get(url=url, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching invoices, status code: {response.status_code}")
            return

        invoices = json.loads(response.content)["QueryResponse"]["Invoice"]

        allowed_locations = [
            "FULTON FISH MKT, BRONX, NY",
            "ACA, NEW YORK, NY",
            "BFT, BROOKLYN, NY",
            "BOSTON, MA",
            "ARAHO, BOSTON, MA",
            "CFI, BOSTON, MA",
            "PRO FISH",
            "JESSUP, MD",
            "SEACAP, JESSUP, MD",
            "PHILA., PA",
            "LAWRENCE, PHILA, PA"
        ]

        for invoice in invoices:
            ship_addr_line2 = invoice.get('ShipAddr', {}).get('Line2', 'N/A')
            invoice_date = datetime.fromisoformat(invoice['TxnDate'])
            invoice_date_est = invoice_date.astimezone(pytz.timezone('US/Eastern'))
            ship_addr_line2 = invoice.get('ShipAddr', {}).get('Line2', 'N/A')
            # print(f"DocNumber: {invoice['DocNumber']}, Invoice Date: {invoice_date_est.strftime('%Y-%m-%d %H:%M:%S')}, ShipAddr Line2: {ship_addr_line2}")
            if ship_addr_line2 in allowed_locations:
                invoice_numbers.append(invoice['DocNumber'])  # Append the DocNumber to the invoice_numbers list
        
    except Exception as e:
        print(f"Failed to query invoices since {hour}:00 {days} days ago. Error: {e}")
    
    return invoice_numbers  # Return the list of invoice numbers


# %%
tonight_from_qb = invoices_currently_in_qb(days = 1)

# %% [markdown]
# #GETTING ALL THE INVOICES FROM THE GOOGLE SHEET

# %%
#json_file_path = r'C:\Users\jesse\Desktop\Potential Burnsed Projects\DO NOT TOUCH\service_account.json'

printer = "Kyocera ECOSYS M5526cdw KX [BSD-7TJK1V3](Mobility)"

# Define the scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

# Authenticate using the service account file
credentials = service_account.Credentials.from_service_account_file(
        json_file_path, scopes=SCOPES)

# Build the service object for the Drive API
service = build('drive', 'v3', credentials=credentials)

# Use the path to the JSON file with gspread
sa = gspread.service_account(filename=json_file_path)
sh = sa.open("BurnsedLive")

# Access the worksheet
orders_wks = sh.worksheet("Orders")

burnsedlive = list(fetch_data_from_sheets(orders_wks)['Invoice Number'])[1:]


# %% [markdown]
# #COMPARING ALL DATA

# %%
# What's in the first list (tonight_from_qb) but not in the second list (tonight_printed)
in_qb_not_printed = list(set(tonight_from_qb) - set(tonight_printed))

# What's in the second list (tonight_printed) but not in the first list (tonight_from_qb)
in_printed_not_qb = list(set(tonight_printed) - set(tonight_from_qb))

print("In QB but not printed:", in_qb_not_printed)
print("Printed but not in QB:", in_printed_not_qb)


# %%
# What's in the first list (tonight_from_qb) but not in the second list (tonight_printed)
in_qb_not_burnsedlive = list(set(tonight_from_qb) - set(burnsedlive))

# What's in the second list (tonight_printed) but not in the first list (tonight_from_qb)
in_burnsedlive_not_qb = list(set(burnsedlive) - set(tonight_from_qb))
if "" in in_burnsedlive_not_qb:
    in_burnsedlive_not_qb.remove("")

print("In QB but not in Google Sheet:", in_qb_not_burnsedlive)
print("In Google Sheet but not in QB:", in_burnsedlive_not_qb)

# %%


