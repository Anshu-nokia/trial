import pandas as pd
import os

# Input Data
cbs_data_location = "C:/Users/anshkuma/Desktop/2G Grow Tracker/Input"
cbs_data_sheet_name = "CBS"
ErlangB_data = "C:/Users/anshkuma/Desktop/2G Grow Tracker/ErlangB_Table.xlsx"


# Reading Input Files
for filename in os.listdir(cbs_data_location):
    if filename.endswith(".xlsx"):
        file_path = os.path.join(cbs_data_location, filename)
        cbs_data = pd.read_excel(file_path, sheet_name= cbs_data_sheet_name, header=2, index_col=None)

# Reading ErlangB Table
erlangb_table = pd.read_excel(ErlangB_data)

# Creating an Output file
df = pd.DataFrame()

def add_static_data(data):
    """ This function will add the static column of cbs_data.
    - data: CBS Data worksheet
    """
    global df

    columns_to_add = ['2G_Site_ID', 'Cell ID', 'Cellname', 'BSC Name', 'Vendor Name', 'Town']

    # Renaming columns
    rename_columns = {
    '2G_Site_ID': 'SITE_ID',
    'Cell ID': 'MO',
    'Cellname': 'Cell_Name',
    'BSC Name': 'BSC',
    'Vendor Name': 'OEM',
    'Town': 'HQ_TOWN',
    }

    for column in columns_to_add:
        df[column] = data[column]

    # Rename the columns using the mapping
    df.rename(columns=rename_columns, inplace=True)

    print("Static Data Concatenation Completed Successfully")

# Call a function
add_static_data(cbs_data)

## Defining a Site Sector column
df["last_character"] = df["Cell_Name"].str[-1]

# Function to determine the conditions for Site Sector
def determine_sector(row):
    if row["last_character"] == 'A':
        return '1'
    elif row["last_character"] == 'B':
        return '2'
    elif row["last_character"] == 'C':
        return '3'
    elif row["last_character"] in ['1','2', '3']:
        return row["last_character"]
    else:
        return None   

# Creating the Site Sector column based on the Conditions
df["Site Sector"] = df.apply(determine_sector, axis=1)

df["Site Sector"] = df["SITE_ID"] + "_" + df["Site Sector"]
df.drop(["last_character"], axis=1, inplace=True)

# Defining a function to extract and merge columns
def extract_and_merge(columns_to_merge):
    """
    This function recieves a list of columns from cbs_data and merge with our output file.
    * Use 'Cellname' in list whenever we want to merge column.
    -Columns_to_merge: A list of column names to merge.
    """
    
    global df
    # Extract columns from CBS Data
    t1 = cbs_data[columns_to_merge]

    # Merging the extracted column to our output dataframe.
    df = pd.merge(df, t1, left_on='Cell_Name', right_on='Cellname', how='inner')

    # Dropping the redundant 'Cellname' Column
    df.drop(['Cellname'], axis=1, inplace=True)
    print(f" {columns_to_merge} merged succesfully.")


# Function to rename any column.
def rename_dataframe_columns(df, columns_to_change, new_column_names):
    """
    This function will take a list of column names and change it to desired names.
    - df: Desired Dataframe
    - columns_to_change: A list of columns that need to be changed.
    - new_column_names: A list of new columns names.
    """

    if len(columns_to_change) != len(new_column_names):
        raise ValueError("The number of columns to change and new column names must be the same.")

    # Create a dictionary for column renaming
    rename_columns = {columns_to_change[i]: new_column_names[i] for i in range(len(columns_to_change))}

    # Rename the columns using the provided mapping
    df.rename(columns=rename_columns, inplace=True)

    print(f"{columns_to_change} has been changed to {new_column_names}")


# Adding 900 TRX and 1800 TRX in the output file.
extract_and_merge(['Cellname', '#900 Active TRX', '#1800 Active TRX'])

# Renaming the TRX column.
rename_dataframe_columns(df, ['#900 Active TRX', '#1800 Active TRX'], ['900 TRX', '1800 TRX'])

# Calculating TRX per sec.
df["TRX per sec"] = df["900 TRX"] + df["1800 TRX"]

# Calculating TRX per sec.
df["TRX per site"] = df.groupby("SITE_ID")["TRX per sec"].transform("sum")

# Addding BCCH, SDCCH, PDTCH, CCCH columns from cbs_data. Also renaming desired column.
extract_and_merge(['Cellname','BCCH', 'NO OF SDCCH (CAVAACC - NBH) ', 'PDTCH', 'CCCH'])
rename_dataframe_columns(df, ['NO OF SDCCH (CAVAACC - NBH) '], ["SDCCH"])

# Calculating Total TCH
df["Total TCH"] = df['TRX per sec']*8 - (df["BCCH"]+df["SDCCH"]+df["PDTCH"]+df["CCCH"])

## Calculating Equipped_ErlangB_capacity_forVoice Column.
# Extract the column from erlangB table to merge.
column_to_merge = erlangb_table[["Unnamed: 13", 0.02]]

# Merging column
df = pd.merge(df, column_to_merge, left_on="Total TCH", right_on="Unnamed: 13", how="inner")

# Dropping the reduntant column.
df.drop(["Unnamed: 13"], axis=1, inplace=True)

# Renaming Column
rename_dataframe_columns(df, [0.02], ["Equipped_Erlang_capacity_forVoice"])

# Adding Average BBH Trafic column
extract_and_merge(["Cellname", "Average BBH Traffic (Total)", "Cell Utilization", "% HR Traffic"])
rename_dataframe_columns(df, ["Average BBH Traffic (Total)", "Cell Utilization", "% HR Traffic"], ["Traffic(avg 7 days)", "Avg Cell Utilization", "% HR Traffic(avg 7 days)"])


# Calculating "TCH Blocking Nom(avg 7 days)" and "TCH Blocking (>1%) count in last 7 days" columns.
new_df = pd.DataFrame()

for filename in os.listdir(cbs_data_location):
    if filename.endswith(".csv"):
        file_path = os.path.join(cbs_data_location, filename)
        df1 = pd.read_csv(file_path)
        df1 = df1[["CELL_ID", "TCH_Blocking_Nom", "TCH_Blocking_User_Perceived"]]
        new_df = pd.concat([new_df, df1], ignore_index=True)

new_df.reset_index(drop=True, inplace=True)
new_df["TCH Blocking Nom(avg 7 days)"] = new_df.groupby("CELL_ID")["TCH_Blocking_Nom"].transform("mean")

# Count percentage nom value
new_df["per count"] = new_df["TCH_Blocking_User_Perceived"].apply(lambda x: 1 if x > 1 else 0)

# Calculate the sum of "per count" for each CELL_ID group
sum_per_count = new_df.groupby("CELL_ID")["per count"].sum().reset_index()

# Merge the sum_per_count DataFrame back into new_df using CELL_ID as the key
new_df = new_df.merge(sum_per_count, on="CELL_ID", suffixes=('', '_sum'))

new_df.drop(["TCH_Blocking_Nom", "TCH_Blocking_User_Perceived", "per count"], axis=1, inplace=True)
new_df = new_df.drop_duplicates(subset='CELL_ID', keep='first')
rename_dataframe_columns(new_df, ["CELL_ID"], ["MO"])

# changing the datatype of MO column to string.
df["MO"] = df["MO"].astype(str)
new_df["MO"] = new_df["MO"].astype(str)

df = pd.merge(df, new_df, on=["MO"], how="left")
rename_dataframe_columns(df, ["per count_sum"], ["TCH Blocking (>1%) count in last 7 days"])

# Adding Remarks to output column.
def get_remarks(row, hr_traffic_threshold, cell_utilization_threshold, blocking_count_threshold, trx_site_threshold, cell_utilization_lower, trx_sec_threshold, blocking_count_upper):
    if (row["% HR Traffic(avg 7 days)"] > hr_traffic_threshold) and (row["Avg Cell Utilization"] >= cell_utilization_threshold) and (row["TCH Blocking (>1%) count in last 7 days"] >= blocking_count_threshold) and (row["TRX per site"] < trx_site_threshold):
        return "Grow"
    if (row["Avg Cell Utilization"] < cell_utilization_lower) and (row["TRX per sec"] > trx_sec_threshold) and (row["TCH Blocking (>1%) count in last 7 days"] < blocking_count_upper):
        return "Degrow"
    else:
        return ""  # Handling other cases

    

# Define user-specific threshold values for Grow
grow_hr_traffic_threshold = 90
grow_cell_utilization_threshold = 140
grow_blocking_count_threshold = 3
grow_trx_site_threshold = 36

# Define user-specific threshold values for Degrow
degrow_cell_utilization_lower = 70
degrow_trx_sec_threshold = 2
degrow_blocking_count_upper = 2

# Apply the function for Grow and Degrow to create the 'Remarks' column
df['Remarks'] = df.apply(lambda row: get_remarks(row, grow_hr_traffic_threshold, grow_cell_utilization_threshold, grow_blocking_count_threshold, grow_trx_site_threshold, degrow_cell_utilization_lower, degrow_trx_sec_threshold, degrow_blocking_count_upper), axis=1)

## ----------
# Adding 'TRX count to be added/deleted' column
# df["TRX count to be added/deleted"] = pd.Series(dtype='int')
# df["TRX count to be added/deleted"] = 0

# # Calculating some more column on basis of TRX count added.
# df["Post TRX per sec"] = df["TRX per sec"] + df["TRX count to be added/deleted"]

# # Calculating post TCH
# df["Post TCH"] = df['Post TRX per sec']*8 - (df["BCCH"]+df["SDCCH"]+df["PDTCH"]+df["CCCH"])

# # Calculating "Post Equipped_Erlang_Capacity_forVoice" column.
# merge_column = erlangb_table[["Unnamed: 13", 0.02]]

# ## Merging column
# df = pd.merge(df, merge_column, left_on="Post TCH", right_on="Unnamed: 13", how="inner")

# ## Dropping the reduntant column.
# df.drop(["Unnamed: 13"], axis=1, inplace=True)

# ## Renaming Column
# rename_dataframe_columns(df, [0.02], ["Post Equipped_Erlang_Capacity_forVoice"])


# # Calculating Post Cell Utilization
# df["Post Cell Utilization"] = (df["Traffic(avg 7 days)"] / df["Post Equipped_Erlang_Capacity_forVoice"])* 100

# # Calculating Post site TRX.
# df["Post site TRX"] = df["TRX per site"] + df["TRX count to be added/deleted"]

# -------------------

for index, row in df.iterrows():

    # Initialize TRX change count
    trx_change = 0

    if row['Remarks'] == 'Grow':

        for i in range(0,10):
            trx_change = i

            # Recalculate columns with added TRX
            df.at[index,'Post TRX per sec'] = row['TRX per sec'] + trx_change
            post_tch = post_trx_per_sec*8 - (row["BCCH"]+row["SDCCH"]+row["PDTCH"]+row["CCCH"])

            df.at[index,'Post TCH'] = post_tch

            merge_column = erlangb_table[["Unnamed: 13", 0.02]]
            df = pd.merge(df, merge_column, left_on="Post TCH", right_on="Unnamed: 13", how="inner")
            df.drop(["Unnamed: 13"], axis=1, inplace=True)
            rename_dataframe_columns(df, [0.02], ["Post Equipped_Erlang_Capacity_forVoice"])

            df.at[index,'Post Cell Utilization'] = (df["Traffic(avg 7 days)"] / df["Post Equipped_Erlang_Capacity_forVoice"])* 100

            df.at[index,'Post site TRX'] = row['TRX per site'] + trx_change

            # Check if constraints are met
            if df.at[index, 'Post Cell Utilization'] < 130 and df.at[index,'Post site TRX'] < 36:
                break

    elif row['Remarks'] == 'Degrow':

        for i in range(10):
            trx_change = -i

            # Recalculate columns with reduced TRX 
            post_trx_per_sec = row['TRX per sec'] + trx_change 
            post_tch = post_trx_per_sec*8 - (row["BCCH"]+row["SDCCH"]+row["PDTCH"]+row["CCCH"])

            df.at[index,'Post TCH'] = post_tch
            merge_column = erlangb_table[["Unnamed: 13", 0.02]]
            df = pd.merge(df, merge_column, left_on="Post TCH", right_on="Unnamed: 13", how="inner")
            df.drop(["Unnamed: 13"], axis=1, inplace=True)
            rename_dataframe_columns(df, [0.02], ["Post Equipped_Erlang_Capacity_forVoice"])
            df.at[index,'Post Cell Utilization'] = (df["Traffic(avg 7 days)"] / df["Post Equipped_Erlang_Capacity_forVoice"])* 100

            df.at[index,'Post site TRX'] = row['TRX per site'] + trx_change

            if df.at[index, 'Post Cell Utilization'] < 100:
                break

    else:
        trx_change = 0

    # Set final TRX change        
    df.at[index, 'TRX count to be added/deleted'] = trx_change
# ---------------


# # Existing code to create the Excel file
# df.to_excel("Output_worksheet.xlsx", index=False)

# # Add the following code to overwrite the file
# output_file_path = "Output_worksheet.xlsx"

# if os.path.exists(output_file_path):
#     os.remove(output_file_path)

# df.to_excel(output_file_path, index=False)