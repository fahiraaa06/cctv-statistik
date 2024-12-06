import streamlit as st
import pandas as pd
import ast
from pymongo import MongoClient

st.set_page_config(
    page_title="Real-Time Data Science Dashboard",
    page_icon="✅",
    layout="wide",
)

# MongoDB connection
mongo_client = MongoClient("mongodb+srv://fahira:aqilah123@domyapi.wit3inx.mongodb.net/")  # Update your MongoDB URI
db = mongo_client.cctv_data  # Database name
collection = db.cctv_records  # Collection name

# Fetch all documents from the 'cctv_records' collection and create a DataFrame
df = pd.DataFrame(list(collection.find()))

# Drop the '_id' field if it exists
if '_id' in df.columns:
    df = df.drop(['_id'], axis=1)

# Ensure all missing fields have default values
default_columns = ['mobil', 'truck', 'motor', 'bus', 'total']
for col in default_columns:
    if col not in df.columns:
        df[col] = 0

# Rename 'timestamp' to 'time' for consistency
if 'timestamp' in df.columns:
    df = df.rename(columns={'timestamp': 'time'})

# Convert 'time' to datetime format
if 'time' in df.columns:
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d_%H-%M-%S', errors='coerce')

# Drop rows with invalid datetime in 'time'
df = df.dropna(subset=['time'])

# Add a 'CCTV' column based on the 'cctv_no' field
if 'cctv_no' in df.columns:
    df['CCTV'] = df['cctv_no']

# Dashboard title
st.title("Real-Time / Live Data Science Dashboard")
st.write(f"Jumlah data keseluruhan:", len(df))

# Extract hour for grouping
df['hour'] = df['time'].dt.strftime('%Y-%m-%d %H:00')

# Check if 'count' column exists before applying parsing
if 'count' in df.columns:
    # Function to parse vehicle counts and handle malformed data
    def parse_vehicle_count(count_str):
        try:
            # If it's already a dictionary, no need to parse
            if isinstance(count_str, dict):
                vehicle_count = count_str
            else:
                # Check if it's a string and is in dictionary-like format
                if isinstance(count_str, str) and count_str.startswith("{") and count_str.endswith("}") :
                    # Remove extra quotes or spaces
                    count_str = count_str.replace('\"', '"').strip()
                    # Now try to convert the string to a dictionary
                    vehicle_count = ast.literal_eval(count_str)
                else:
                    # If it’s neither, assume it’s malformed or empty
                    vehicle_count = {}

            # Return the vehicle counts with defaults if any key is missing
            return {
                'mobil': vehicle_count.get('car', 0),
                'truck': vehicle_count.get('truck', 0),
                'motor': vehicle_count.get('motorcycle', 0),
                'bus': vehicle_count.get('bus', 0),
            }

        except (ValueError, SyntaxError) as e:
            # Handle the case where the string is malformed or invalid
            print(f"Error parsing count: {count_str}, Error: {e}")
            return {'mobil': 0, 'truck': 0, 'motor': 0, 'bus': 0}

    # Apply the parse_vehicle_count function to the 'count' column
    df[['mobil', 'truck', 'motor', 'bus']] = df['count'].apply(parse_vehicle_count).apply(pd.Series)
else:
    st.error("The 'count' column is missing or not formatted correctly!")

# Group and sum vehicle counts by hour and vehicle type
grouped_summary = df.groupby('hour', as_index=False).agg({
    'mobil': 'sum',
    'truck': 'sum',
    'motor': 'sum',
    'bus': 'sum'
})

# Calculate the total count per hour
grouped_summary['total'] = grouped_summary[['mobil', 'truck', 'motor', 'bus']].sum(axis=1)

# Create hourly range from the earliest to the latest time
full_hour_range = pd.date_range(
    start=df['time'].min().floor('H'),  # Start from the earliest time
    end=df['time'].max().ceil('H'),    # End at the latest time
    freq='H'  # Hourly frequency
)

# Create reference DataFrame for hourly range
hour_df = pd.DataFrame({'hour': full_hour_range})

# Merge the original data with the hourly range, filling NaN values with 0
grouped_summary['hour'] = pd.to_datetime(grouped_summary['hour'])  # Ensure datetime format
merged_summary = hour_df.merge(grouped_summary, on='hour', how='left').fillna(0)

# Ensure consistent data types
merged_summary[['mobil', 'truck', 'motor', 'bus', 'total']] = merged_summary[[ 
    'mobil', 'truck', 'motor', 'bus', 'total'
]].astype(int)

# Sort by total vehicle count in descending order
sorted_merged_summary = merged_summary.sort_values(by='total', ascending=False)

# Display the area chart for the overall data using Streamlit's built-in area chart
st.subheader("Jumlah Kendaraan per Jam")
st.area_chart(merged_summary.set_index('hour')[['mobil', 'truck', 'motor', 'bus']])

# Display the overall DataFrame sorted by total vehicle count
st.write("Data Keseluruhan (Sorted by Total Vehicle Count):")
st.dataframe(merged_summary)

# Display the CCTV selection dropdown below the chart
selected_cctv = st.selectbox("Pilih CCTV", df['CCTV'].unique())

# Filter data for the selected CCTV
df_filtered = df[df['CCTV'] == selected_cctv]

# Show the filtered data
st.write(f"CCTV: {selected_cctv}, jumlah data: {len(df_filtered)}")

# Extract hour for the filtered data
df_filtered['hour'] = df_filtered['time'].dt.strftime('%Y-%m-%d %H:00')

# Group and sum the filtered data by hour
filtered_grouped = df_filtered.groupby('hour', as_index=False).agg({
    'mobil': 'sum',
    'truck': 'sum',
    'motor': 'sum',
    'bus': 'sum'
})

# Calculate the total count per hour for filtered data
filtered_grouped['total'] = filtered_grouped[['mobil', 'truck', 'motor', 'bus']].sum(axis=1)

# Create hourly range for the filtered data
filtered_full_hour_range = pd.date_range(
    start=df_filtered['time'].min().floor('H'),
    end=df_filtered['time'].max().ceil('H'),
    freq='H'
)

# Create reference DataFrame for the filtered hourly range
filtered_hour_df = pd.DataFrame({'hour': filtered_full_hour_range})

# Merge the filtered data with the hourly range, filling NaN values with 0
filtered_grouped['hour'] = pd.to_datetime(filtered_grouped['hour'])  # Ensure datetime format
filtered_merged_summary = filtered_hour_df.merge(filtered_grouped, on='hour', how='left').fillna(0)

# Ensure consistent data types for the filtered data
filtered_merged_summary[['mobil', 'truck', 'motor', 'bus', 'total']] = filtered_merged_summary[[ 
    'mobil', 'truck', 'motor', 'bus', 'total'
]].astype(int)

# Sort the filtered data by total vehicle count
sorted_filtered_grouped = filtered_merged_summary.sort_values(by='total', ascending=False)

# Display the area chart for the filtered CCTV data
st.subheader(f"Jumlah Kendaraan per Jam (CCTV: {selected_cctv})")
st.area_chart(filtered_merged_summary.set_index('hour')[['mobil', 'truck', 'motor', 'bus']])

# Display the filtered DataFrame sorted by total vehicle count
st.write(f"Data untuk CCTV {selected_cctv} (Sorted by Total Vehicle Count):")
st.dataframe(filtered_merged_summary)
