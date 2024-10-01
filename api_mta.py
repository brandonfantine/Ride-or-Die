import requests
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_data(offset):
    url = f"https://data.ny.gov/resource/wujg-7c2s.json?$limit=1000&$offset={offset}"
    response = requests.get(url)
    return response.json()

def write_to_csv(data):
    with open("data_10mil.csv", "w", newline="") as file:
        writer = csv.writer(file)
        # Write headers if needed (assuming all records have the same keys)
        writer.writerow(data[0].keys())  # Optional: write header
        for record in data:
            writer.writerow(record.values())

def main():
    offsets = [i * 1000 for i in range(10000)]
    data = []

    # Use ThreadPoolExecutor to fetch data concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_data, offset): offset for offset in offsets}

        for future in as_completed(futures):
            try:
                result = future.result()
                data.extend(result)
            except Exception as e:
                print(f"Error fetching data for offset {futures[future]}: {e}")

    # Write all fetched data to CSV
    write_to_csv(data)

if __name__ == "__main__":
    main()
