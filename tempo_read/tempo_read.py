# %%
#!pip install netCDF4

# %%
import requests
import os
import zstandard as zstd
import time
import csv
from netCDF4 import Dataset
import pandas as pd
from joblib import Parallel, delayed
import subprocess

# %%
scr_dir = 'C:/aiarthon_py/nasa-airathon-merge/secure.txt'
secure = dict([e.split('=') for e in open(scr_dir, 'r').read().split('\n')])

# %%
tempo_row_dir = 'C:/aiarthon_py/nasa-airathon-merge/tempo_read/tempo_rows.txt'
urls = [line.strip() for line in open(tempo_row_dir, 'r') if line.strip()]

# %%
download_dir = '/tmp/'

def loadFileS3(urls):
    for i in range(1):
        try:
            values = {'email' : secure['username'], 'passwd' : secure['password'], 'action' : 'login'}
            login_url = 'https://urs.earthdata.nasa.gov'

            ret = requests.post(login_url, data=values)
            if ret.status_code == 200:
                print("Login successful.")
            else:
                print("Bad Authentication")
                return None
        except Exception as e:
            print(e)
            time.sleep(i)
        
    # zc = zstd.ZstdCompressor(level=15)
    os.makedirs(download_dir, exist_ok=True)

    for url in urls:
        try:
            outfile = os.path.basename(url)
            print("Downloading", outfile)
            with requests.get(url, cookies = ret.cookies, 
                  allow_redirects = True, stream=True) as r:
                  r.raise_for_status()
                  outfile_path = os.path.join(download_dir, outfile)
                  with open(outfile_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024*1024): 
                              f.write(chunk)
                  

            # Extract filename from URL and append '.zst' for the compressed version
            filename = url.split('/')[-1]
            save_path = os.path.join(download_dir, filename)
            print(f"Downloaded and compressed {filename} to {save_path}")

            print("Extracting variables and writing to CSV.")
            extract_var_and_wr_csv('C:/tmp/', 'C:/csv/output.csv')
        except requests.RequestException as e:
            print(f"Error downloading {url}: {e}")

        finally:
            print(f"Deleting: ")
            os_remove()

        
    return save_path


def extract_var_and_wr_csv(file_dir, output_csv_path):
    """
    Process NetCDF files in a given directory to extract metadata and write to a CSV file.

    Parameters:
    - file_dir: Directory containing the NetCDF files.
    - output_csv_path: Path to the output CSV file.
    """
        
    # List all NetCDF files in the specified directory
    files = [f for f in os.listdir(file_dir) if f.endswith('.nc')]

    # Define headers for the CSV file based on the metadata to extract
    headers = ['granule_id', 'time_start', 'time_end', 'product', 'location', 'split', 'granuleSize']

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

    # Open the CSV file for writing
    with open(output_csv_path, 'a', newline='') as csvfile:
        csvwriter = csv.DictWriter(csvfile, fieldnames=headers)
        if os.stat(output_csv_path).st_size == 0:
            csvwriter.writeheader()  # Write header only if file is empty
        
        # Process each NetCDF file
        for file_name in files:
            file_path = os.path.join(file_dir, file_name)
            with Dataset(file_path, 'r') as nc:
                # Extract metadata; adjust these as necessary to match your NetCDF structure
                timeStart = getattr(nc, 'time_coverage_start', 'NA')
                timeEnd = getattr(nc, 'time_coverage_end', 'NA')
                product = 'tempo'
                location = 'la'
                split = 'train'
                granuleSize = os.path.getsize(file_path)


                # Write the extracted metadata to the CSV
                csvwriter.writerow({
                    'granule_id': file_name,
                    'time_start': timeStart,
                    'time_end': timeEnd,
                    'product': product,
                    'location': location,
                    'split' : split,
                    'granuleSize': granuleSize,
                })
                
            # Delete the file after processing
            print(f"Successfuly Written: {file_name}")

def os_remove():
    tmp_dir = 'C:/tmp/'  # The directory from which you want to delete files
    files = [f for f in os.listdir(tmp_dir) if f.endswith('.nc')]  # List of all .nc files in the directory
    
    for filename in files:
        file_path = os.path.join(tmp_dir, filename)  # Full path to the file
        try:
            os.remove(file_path)  # Delete the file
            print(f"Successfuly Deleted: {filename} from {tmp_dir}")
        except FileNotFoundError:  # Catch the specific exception if the file does not exist
            print(f"{filename} does not exist")
            




# Call the function

loadFileS3(urls)
#os_remove()