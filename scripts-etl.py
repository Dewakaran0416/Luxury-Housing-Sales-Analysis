import pandas as pd
import numpy as np
from sqlalchemy import create_engine



file_path = r"D:\Python\Project\Mini Project 2\Luxury_Housing_Bangalore.csv"
df = pd.read_csv(file_path)

print("Initial Shape:", df.shape)



df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

print("Cleaned Columns:", df.columns.tolist())



# Normalize text columns
df["micro_market"] = df["micro_market"].str.strip().str.title()
df["project_name"] = df["project_name"].str.strip().str.title()
df["developer_name"] = df["developer_name"].str.strip().str.title()
df["configuration"] = df["configuration"].str.upper()
df["transaction_type"] = df["transaction_type"].str.title()
df["buyer_type"] = df["buyer_type"].str.title()
df["possession_status"] = df["possession_status"].str.title()
df["sales_channel"] = df["sales_channel"].str.title()
df["nri_buyer"] = df["nri_buyer"].str.lower()



df["amenity_score"] = df["amenity_score"].fillna(df["amenity_score"].median())
df["buyer_comments"] = df["buyer_comments"].fillna("No Comments")



df["ticket_price_cr"] = pd.to_numeric(df["ticket_price_cr"], errors="coerce")

# Convert Crores → Actual INR
df["price_inr"] = df["ticket_price_cr"] * 10000000



# Price per Sqft
df["price_per_sqft"] = df["price_inr"] / df["unit_size_sqft"]

# Booking Flag
df["booking_flag"] = df["booking_status"].apply(
    lambda x: 1 if str(x).lower() == "booked" else 0
)

# Convert Purchase_Quarter to Date
df["purchase_quarter"] = pd.to_datetime(df["purchase_quarter"], errors="coerce")

df["year"] = df["purchase_quarter"].dt.year
df["quarter"] = df["purchase_quarter"].dt.quarter

# Traffic Category
df["traffic_category"] = pd.cut(
    df["avg_traffic_time_min"],
    bins=[0, 30, 60, 120, 1000],
    labels=["Low", "Moderate", "High", "Severe"]
)


df = df.drop_duplicates()

print("Final Shape:", df.shape)



output_path = r"D:\Python\Project\Mini Project 2\Cleaned_Luxury_Housing.csv"
df.to_csv(output_path, index=False)

print("Cleaned CSV Saved Successfully!")

engine = create_engine("mysql+pymysql://root:0923@localhost/real_estate_dw")

df.to_sql(
    name="fact_properties",
    con=engine,
    if_exists="replace",
    index=False
)

print("Data Loaded into MySQL Successfully!")

