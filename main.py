# TEAM DHARA | UIDAI DATA HACKATHON 2026
# Project: Digital Velocity Index (DVI)
# Purpose: Identify Identity Resilience & Identity Deserts
import pandas as pd
import glob
import matplotlib.pyplot as plt

# STEP 1: LOAD DATA FROM ALL THREE SILOS
# Enrolment data (New Aadhaar IDs)
enrol_files = glob.glob("data/enrolment/*.csv")
df_enrol = pd.concat((pd.read_csv(f) for f in enrol_files), ignore_index=True)
print("Enrolment loaded:", df_enrol.shape)

# Demographic update data (Address, mobile, name changes)
demo_files = glob.glob("data/demographic/*.csv")
df_demo = pd.concat((pd.read_csv(f) for f in demo_files), ignore_index=True)
print("Demographic loaded:", df_demo.shape)

# Biometric update data (MBU for children & adults)
bio_files = glob.glob("data/biometric/*.csv")
df_bio = pd.concat((pd.read_csv(f) for f in bio_files), ignore_index=True)
print("Biometric loaded:", df_bio.shape)

# STEP 2: BASIC DATA CLEANING
# Convert date column to datetime
for df in [df_enrol, df_demo, df_bio]:
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Standardize district names (important for correct aggregation)
def clean_district(df):
    df['district'] = df['district'].str.strip()
    df['district'] = df['district'].replace({
        'Bangalore Urban': 'Bengaluru',
        'Bengaluru Urban': 'Bengaluru'
    })
    return df

df_enrol = clean_district(df_enrol)
df_demo = clean_district(df_demo)
df_bio = clean_district(df_bio)

# STEP 3: AGGREGATE EACH SILO BEFORE MERGING (CRITICAL STEP)
# This prevents data duplication and incorrect inflation
enrol_agg = df_enrol.groupby(
    ['date', 'state', 'district', 'pincode'],
    as_index=False
).sum()

demo_agg = df_demo.groupby(
    ['date', 'state', 'district', 'pincode'],
    as_index=False
).sum()

bio_agg = df_bio.groupby(
    ['date', 'state', 'district', 'pincode'],
    as_index=False
).sum()

print("Aggregated enrolment:", enrol_agg.shape)
print("Aggregated demographic:", demo_agg.shape)
print("Aggregated biometric:", bio_agg.shape)

# STEP 4: MERGE ALL THREE SILOS INTO ONE MASTER TABLE
df = enrol_agg.merge(
    demo_agg,
    on=['date', 'state', 'district', 'pincode'],
    how='left'
)

df = df.merge(
    bio_agg,
    on=['date', 'state', 'district', 'pincode'],
    how='left'
)
# Replace missing updates with zero (means no update activity)
df.fillna(0, inplace=True)

print("Final merged dataset shape:", df.shape)

# STEP 5: DIGITAL VELOCITY INDEX (DVI) – CORE INNOVATION
# DVI measures Identity Resilience:
# How actively identities are updated relative to enrolment pressure
# Formula (Bounded between 0 and 1):
# DVI = Total Updates / (Enrolment Pressure + Total Updates + 1)
# Enrolment pressure = children + adolescents entering system
df['enrolment_pressure'] = (
    df['age_0_5'] +
    df['age_5_17']
)

# Total update activity = demographic + biometric updates
df['total_updates'] = (
    df['demo_age_5_17'] +
    df['demo_age_17_'] +
    df['bio_age_5_17'] +
    df['bio_age_17_']
)

# Digital Velocity Index (0 = stagnation, 1 = high resilience)
df['DVI'] = df['total_updates'] / (
    df['enrolment_pressure'] + df['total_updates'] + 1
)

print("\nSample DVI values:")
print(df[['state', 'district', 'pincode', 'DVI']].head())

# STEP 6: IDENTIFY IDENTITY DESERTS
# Identity Deserts = Areas where DVI < 0.1 
# Indicates high enrolment but poor lifecycle updates
identity_deserts = (
    df[df['DVI'] < 0.1]
    .groupby('district')['DVI']
    .mean()
    .sort_values()
    .head(10)
)

print("\n--- IDENTITY DESERTS (DVI < 0.1) ---")
print(identity_deserts)

identity_deserts.to_csv('identity_deserts_dvi_lt_0_1.csv')

# STEP 7: CASE STUDY 1 – CHILDHOOD PROTECTION GAP
# Focus District: Sitamarhi, Bihar
# Shows drop in Mandatory Biometric Updates (MBU)
sitamarhi_data = df[
    df['district'].str.contains('Sitamarhi', case=False, na=False)
]

sitamarhi_monthly = sitamarhi_data.groupby(
    sitamarhi_data['date'].dt.to_period('M')
)[['age_0_5', 'bio_age_5_17']].sum()

sitamarhi_monthly.to_csv('childhood_gap_sitamarhi.csv')

print("\nSitamarhi childhood protection data exported.")

# STEP 8: CASE STUDY 2 – MIGRATION PULSE
# Focus District: Bengaluru Urban
# Detects migration-linked demographic updates
bengaluru_data = df[
    df['district'].str.contains('Bengaluru', case=False, na=False)
]

bengaluru_monthly = bengaluru_data.groupby(
    bengaluru_data['date'].dt.to_period('M')
)[['demo_age_17_']].sum()

bengaluru_monthly.to_csv('migration_pulse_bengaluru.csv')

print("\nBengaluru migration pulse data exported.")

# STEP 9: PINCODE-LEVEL DATA FOR NATIONAL HEATMAP (PAGE 9)
heatmap_data = (
    df.groupby('pincode')['DVI']
    .mean()
    .reset_index()
)

heatmap_data.to_csv(
    'national_identity_desert_heatmap.csv',
    index=False
)

print("\nHeatmap data exported. Total pincodes mapped:", len(heatmap_data))
print("Process completed successfully.")
