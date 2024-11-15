import pandas as pd

# Load the first and second CSV files
first_csv = pd.read_csv("dataset/address_poisoning_addresses_list.csv")  # The file containing addresses to compare against
second_csv= pd.read_csv("address.csv")  #  The file from which we want to remove duplicates

# Find addresses in the second CSV that are not in the first CSV
filtered_csv = second_csv[~second_csv['Address'].isin(first_csv['Address'])]

# Save the filtered results back to a new CSV file
filtered_csv.to_csv("filtered_second.csv", index=False)
