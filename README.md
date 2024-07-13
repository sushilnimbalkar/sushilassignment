# sushilassignment
# 1. Data Extraction
# Write a Python Script to Download Data
import requests
import os

def download_file(url, local_filename):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

def main():
    base_url = 'http://example.com/dataset/2019/'
    files = ['yellow_tripdata_2019-01.csv', 'yellow_tripdata_2019-02.csv']  # List all files needed
    download_dir = './data/'
    
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    for file in files:
        url = base_url + file
        local_filename = os.path.join(download_dir, file)
        try:
            download_file(url, local_filename)
            print(f"Downloaded {file} successfully.")
        except Exception as e:
            print(f"Failed to download {file}: {e}")

if __name__ == "__main__":
    main()



# 2. . Data Processing

# Install Required Libraries:

pip install pandas

# Create a Python Script for Data Processing:

import pandas as pd
import os

def process_file(file_path):
    df = pd.read_csv(file_path)
    # Remove rows with missing or corrupt data
    df.dropna(subset=['pickup_time', 'dropoff_time', 'trip_distance', 'fare_amount', 'passenger_count'], inplace=True)
    # Convert pickup and dropoff times to datetime
    df['pickup_time'] = pd.to_datetime(df['pickup_time'])
    df['dropoff_time'] = pd.to_datetime(df['dropoff_time'])
    # Calculate new columns
    df['trip_duration'] = (df['dropoff_time'] - df['pickup_time']).dt.total_seconds() / 60
    df['average_speed'] = df['trip_distance'] / df['trip_duration']
    # Aggregate data
    daily_summary = df.groupby(df['pickup_time'].dt.date).agg({
        'trip_distance': 'sum',
        'fare_amount': 'mean',
        'passenger_count': 'sum'
    }).reset_index()
    return daily_summary

def main():
    input_dir = './data/'
    output_file = './data/processed_summary.csv'
    all_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.csv')]

    summary_list = []
    for file in all_files:
        daily_summary = process_file(file)
        summary_list.append(daily_summary)

    full_summary = pd.concat(summary_list)
    full_summary.to_csv(output_file, index=False)
    print(f"Processed data saved to {output_file}")

if __name__ == "__main__":
    main()


# 3. Data Loading

# Install SQLite Library:

import sqlite3
import pandas as pd

def load_data_to_db(db_path, csv_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_csv(csv_path)
    df.to_sql('taxi_data', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data loaded into SQLite database at {db_path}")

def main():
    db_path = 'taxi_data.db'
    csv_path = './data/processed_summary.csv'
    load_data_to_db(db_path, csv_path)

if __name__ == "__main__":
    main()


# 4. Data Analysis and Reporting


import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def analyze_data(db_path):
    conn = sqlite3.connect(db_path)
    query = """
    SELECT strftime('%H', pickup_time) AS hour, COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
    FROM taxi_data
    GROUP BY hour
    ORDER BY hour;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Plot peak hours
    df.plot(x='hour', y='trip_count', kind='bar')
    plt.title('Peak Hours for Taxi Usage')
    plt.xlabel('Hour of Day')
    plt.ylabel('Number of Trips')
    plt.savefig('peak_hours.png')

    print("Analysis complete. Check 'peak_hours.png' for the peak hours report.")

def main():
    db_path = 'taxi_data.db'
    analyze_data(db_path)

if __name__ == "__main__":
    main()
