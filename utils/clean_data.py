import pandas as pd
from datetime import datetime

def clean_data(raw_data):
    """
    Clean the raw data by performing several tasks:
    1. Drop rows with empty values in 'Price' and 'LivingArea' columns
    2. Remove duplicates in the 'ID' column and where all columns but 'ID' are equal
    3. Convert empty values to 0 for specified columns; assumption that if blank then 0
    4. Filter rows where SaleType == 'residential_sale' and BidStylePricing == 0
    5. Remove specified columns
    6. Adjust text format
    7. Remove leading and trailing spaces from string columns
    8. Replace the symbol '�' with 'e' in all string columns
    9. Fill missing values with None and convert specified columns to float64 type
    10. Convert specified columns to Int64 type
    11. Replace any ConstructionYear > current_year + 10 with None
    12. Trim text after and including '_' from the 'EPCScore' column
    13. Convert 'ListingCreateDate', 'ListingExpirationDate', and 'ListingCloseDate' to Date type with standard DD/MM/YYYY format
    14. Replace values less than or equal to 0 in 'EnergyConsumptionPerSqm' with 0
    15. Calculate 'TotalArea'
    16. Calculate 'PricePerLivingSquareMeter'
    17. Calculate 'PricePerTotalSquareMeter'
    18. Convert string values to numeric values using dictionaries for specified columns
    19. Write resulting dataframe to a CSV

    Parameters:
    raw_data (DataFrame): The raw DataFrame to be cleaned
    """

    raw_data = pd.read_csv("./source/raw_data.csv")

    # Task 1: Drop rows with empty values in 'Price' and 'LivingArea' columns
    raw_data.dropna(subset=['Price', 'LivingArea'], inplace=True)
    
    # Task 2: Remove duplicates in the 'ID' column and where all columns but 'ID' are equal
    raw_data.drop_duplicates(subset='ID', inplace=True)
    raw_data.drop_duplicates(subset=raw_data.columns.difference(['ID']), keep='first', inplace=True)

    # Task 3: Convert empty values to 0 for specified columns; assumption that if blank then 0
    columns_to_fill_with_zero = ['Furnished', 'Fireplace', 'Terrace', 'TerraceArea', 'Garden', 'GardenArea', 'SwimmingPool', 'BidStylePricing', 'ViewCount', 'bookmarkCount']
    raw_data[columns_to_fill_with_zero] = raw_data[columns_to_fill_with_zero].fillna(0)

    # Task 4: Filter rows where SaleType == 'residential_sale' and BidStylePricing == 0
    raw_data = raw_data[(raw_data['SaleType'] == 'residential_sale') & (raw_data['BidStylePricing'] == 0)].copy()

    # Task 5: Remove specified columns
    columns_to_drop = ['PropertyUrl', 'Street', 'HouseNumber', 'Box', 'Floor', 'SaleType', 'BidStylePricing', 'Property url']
    raw_data.drop(columns=columns_to_drop, inplace=True)

    # Task 6: Adjust text format
    columns_to_str = ['City', 'Region', 'District', 'Province', 'PropertyType', 'PropertySubType', 'KitchenType', 'Condition', 'EPCScore']

    def adjust_text_format(x):
        if isinstance(x, str):
            return x.title()
        else:
            return x

    raw_data.loc[:, columns_to_str] = raw_data.loc[:, columns_to_str].applymap(adjust_text_format)

    # Task 7: Remove leading and trailing spaces from string columns
    raw_data.loc[:, columns_to_str] = raw_data.loc[:, columns_to_str].apply(lambda x: x.str.strip() if isinstance(x, str) else x)

    # Task 8: Replace the symbol '�' with 'e' in all string columns
    raw_data = raw_data.applymap(lambda x: x.replace('�', 'e') if isinstance(x, str) else x)

    # Task 9: Fill missing values with None and convert specified columns to float64 type
    columns_to_fill_with_none = ['EnergyConsumptionPerSqm']
    raw_data[columns_to_fill_with_none] = raw_data[columns_to_fill_with_none].where(raw_data[columns_to_fill_with_none].notna(), None)

    columns_to_float64 = ['Price', 'LivingArea', 'TerraceArea', 'GardenArea', 'EnergyConsumptionPerSqm']
    raw_data[columns_to_float64] = raw_data[columns_to_float64].astype(float)

    # Task 10: Convert specified columns to Int64 type
    columns_to_int64 = ['ID', 'PostalCode', 'ConstructionYear', 'BedroomCount', 'Furnished', 'Fireplace', 'Terrace', 'Garden', 'Facades', 'SwimmingPool', 'bookmarkCount', 'ViewCount']
    raw_data[columns_to_int64] = raw_data[columns_to_int64].astype(float).round().astype('Int64')

    # Task 11: Replace any ConstructionYear > current_year + 10 with None
    current_year = datetime.now().year
    max_construction_year = current_year + 10
    raw_data['ConstructionYear'] = raw_data['ConstructionYear'].where(raw_data['ConstructionYear'] <= max_construction_year, None)

    # Task 12: Trim text after and including '_' from the 'EPCScore' column
    raw_data['EPCScore'] = raw_data['EPCScore'].str.split('_').str[0]

    # Task 13: Convert 'ListingCreateDate', 'ListingExpirationDate', and 'ListingCloseDate' to Date type with standard DD/MM/YYYY format
    date_columns = ['ListingCreateDate', 'ListingExpirationDate', 'ListingCloseDate']
    for col in date_columns:
        raw_data[col] = pd.to_datetime(raw_data[col], dayfirst=True).dt.date

    # Task 14: Replace values less than or equal to 0 in 'EnergyConsumptionPerSqm' with 0
    raw_data.loc[raw_data['EnergyConsumptionPerSqm'] < 0, 'EnergyConsumptionPerSqm'] = 0

    # Task 15: Calculate 'TotalArea'
    raw_data['TotalArea'] = raw_data['LivingArea'] + raw_data['GardenArea'] + raw_data['TerraceArea']

    # Task 16: Calculate 'PricePerLivingSquareMeter'
    raw_data['PricePerLivingSquareMeter'] = (raw_data['Price'] / raw_data['LivingArea']).round().astype(int)

    # Task 17: Calculate 'PricePerTotalSquareMeter'
    raw_data['PricePerTotalSquareMeter'] = (raw_data['Price'] / raw_data['TotalArea']).round().astype(int)

    # Task 18: Convert string values to numeric values using dictionaries for specified columns
    condition_mapping = {
        'nan': None,
        'To_Be_Done_Up': 2,
        'To_Renovate': 1,
        'Just_Renovated': 4,
        'As_New': 5,
        'Good': 3,
        'To_Restore': 0
    }

    kitchen_mapping = {
        'nan': None,
        'Installed': 1,
        'Not_Installed': 0,
        'Hyper_Equipped': 1,
        'Semi_Equipped': 1,
        'Usa_Installed': 1,
        'Usa_Hyper_Equipped': 1,
        'Usa_Semi_Equipped': 1,
        'Usa_Uninstalled': 0
    }

    raw_data['Condition#'] = raw_data['Condition'].map(condition_mapping)
    raw_data['KitchenType#'] = raw_data['KitchenType'].map(kitchen_mapping)

    # Task 19: Write resulting dataframe to a CSV
    raw_data.to_csv('./source/clean_data.csv', index=False)