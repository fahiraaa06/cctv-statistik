import streamlit as st
import pandas as pd
import plotly.graph_objects as go
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
st.write(f"jumlah data keseluruhan:", len(df))

# Select CCTV camera using a dropdown
selected_cctv = st.selectbox("Pilih CCTV", df['CCTV'].unique())

# Filter data for the selected CCTV
df_filtered = df[df['CCTV'] == selected_cctv]

# Extract hour for grouping
df_filtered['hour'] = df_filtered['time'].dt.strftime('%Y-%m-%d %H:00')

# Check if 'count' column exists before applying parsing
if 'count' in df_filtered.columns:
    # Function to parse vehicle counts and handle malformed data
    def parse_vehicle_count(count_str):
        try:
            # If it's already a dictionary, no need to parse
            if isinstance(count_str, dict):
                vehicle_count = count_str
            else:
                # Check if it's a string and is in dictionary-like format
                if isinstance(count_str, str) and count_str.startswith("{") and count_str.endswith("}"):
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
    df_filtered[['mobil', 'truck', 'motor', 'bus']] = df_filtered['count'].apply(parse_vehicle_count).apply(pd.Series)
else:
    st.error("The 'count' column is missing or not formatted correctly!")

# Show the filtered data
st.write(f"CCTV: {selected_cctv}, jumlah data: {len(df_filtered)}")
# st.write(f"Kolom-kolom yang ada untuk CCTV {selected_cctv}:", df_filtered.columns)

# Group data by hour and vehicle type
grouped_vehicle_counts = df_filtered.melt(
    id_vars=["time", "hour"], 
    value_vars=["mobil", "truck", "motor", "bus"], 
    var_name="vehicle_type", 
    value_name="vehicle_count"  # Renamed from 'count' to 'vehicle_count'
)

# Show the melted data
# st.write("Grouped Vehicle Counts DataFrame:", grouped_vehicle_counts)

# Group and sum vehicle counts by hour and vehicle type
grouped_summary = df_filtered.groupby('hour', as_index=False).agg({
    'mobil': 'sum',
    'truck': 'sum',
    'motor': 'sum',
    'bus': 'sum'
})

# Calculate the total count per hour
grouped_summary['total'] = grouped_summary[['mobil', 'truck', 'motor', 'bus']].sum(axis=1)

# Custom CSS for table formatting
st.markdown("""
    <style>
        .streamlit-expanderHeader {
            font-size: 16px;
        }
        .dataframe th {
            white-space: nowrap;
            text-align: center;
        }
        .dataframe td {
            text-align: center;
        }
    </style>
""", unsafe_allow_html=True)

# Buat rentang waktu per jam dari waktu terawal hingga waktu terakhir
full_hour_range = pd.date_range(
    start=df_filtered['time'].min().floor('H'),  # Mulai dari waktu terawal
    end=df_filtered['time'].max().ceil('H'),    # Sampai waktu terakhir
    freq='H'  # Frekuensi per jam
)

# Buat DataFrame referensi waktu
hour_df = pd.DataFrame({'hour': full_hour_range})

# Gabungkan data asli dengan rentang waktu, isi nilai NaN dengan 0
grouped_summary['hour'] = pd.to_datetime(grouped_summary['hour'])  # Pastikan format datetime
merged_summary = hour_df.merge(grouped_summary, on='hour', how='left').fillna(0)

# Pastikan tipe data tetap konsisten
merged_summary[['mobil', 'truck', 'motor', 'bus', 'total']] = merged_summary[
    ['mobil', 'truck', 'motor', 'bus', 'total']
].astype(int)

# Pivot data untuk plotting
area_chart_data = merged_summary.melt(
    id_vars="hour",
    value_vars=["mobil", "truck", "motor", "bus"],
    var_name="vehicle_type",
    value_name="vehicle_count"
)

# Pastikan kolom 'hour' tetap dalam format datetime
area_chart_data['hour'] = pd.to_datetime(area_chart_data['hour'])

# Pivot ulang data untuk chart
pivoted_data = area_chart_data.pivot(index='hour', columns='vehicle_type', values='vehicle_count').fillna(0)

# # Display untuk memastikan pivot data benar
# st.write("Pivoted Data for Plotting (with Zero-filled Hours):", pivoted_data)

# # Check if pivoted data is empty or not
# if pivoted_data.empty:
#     st.error("Pivoted data is empty, there's no data to plot.")
# else:
#     # Display the pivoted data
#     st.write("Pivoted Data for Plotting:", pivoted_data)

# Plot the area chart using Plotly (stacked area chart)
fig = go.Figure()

# Add traces for each vehicle type
for vehicle_type in pivoted_data.columns:
    fig.add_trace(go.Scatter(
        x=pivoted_data.index,
        y=pivoted_data[vehicle_type],
        mode='lines',
        stackgroup='one',  # This ensures a stacked chart
        name=vehicle_type
    ))

# Update layout for better visualization
fig.update_layout(
    title='Jumlah Kendaraan per Jam (Stacked Area Chart)',
    xaxis_title='Waktu',
    yaxis_title='Jumlah Kendaraan',
    legend_title='Jenis Kendaraan',
    template='plotly'
)

# Display the plot
st.plotly_chart(fig)

hasil_df = df_filtered.drop(columns=['count', 'CCTV', 'hour'])[['cctv_no', 'time', 'mobil', 'truck',
                                                     'motor', 'bus', 'total']]

st.write(f"DataFrame for CCTV: {selected_cctv}")
# Tampilkan DataFrame dengan lebar kolom menyesuaikan kontennya
st.dataframe(
    hasil_df.style.set_table_styles(
        [{'selector': 'th', 'props': [('min-width', '75px'), ('text-align', 'center')]},
         {'selector': 'td', 'props': [('min-width', '75px'), ('text-align', 'center')]}]
    ), 
    use_container_width=False
)

# st.markdown("### Data keseluruhan CCTV per jam:")
# # Display DataFrame untuk pemeriksaan
# st.write("Grouped Summary with Full Hourly Range:", merged_summary)

