import pandas as pd
import geopandas as gpd
import time
from datetime import datetime
from source.utils import calculate_runtime
from shapely.geometry import Point

def import_data(refresh=False):
    if refresh:
        print("Loading new data and executing the data prep methods...")
        raw_data = pd.read_csv("https://raw.githubusercontent.com/bear-revels/immo-eliza-scraping-Python_Pricers/main/data/all_property_details.csv", dtype={'PostalCode': str})
        raw_data.to_csv('./data/external_data/raw_data.csv', index=False, encoding='utf-8')
    else:
        print("Applying the data prep methods to the existing data...")
        raw_data = pd.read_csv('./data/external_data/raw_data.csv')

    return raw_data

def join_data(raw_data):
    geo_data = gpd.GeoDataFrame(raw_data)

    # Define a function to create Point objects from latitude and longitude
    def create_point(row):
        try:
            latitude = float(row['Latitude'])
            longitude = float(row['Longitude'])
            return Point(longitude, latitude)
        except ValueError:
            return None

    # Create Point geometries from latitude and longitude coordinates in real estate data
    geo_data['geometry'] = geo_data.apply(create_point, axis=1)

    # Set geometry column after creating 'geometry' column
    geo_data = geo_data.set_geometry('geometry')  # Set geometry column
    
    # Explicitly set CRS to WGS 84 (EPSG:4326)
    geo_data.crs = 'EPSG:4326'

    municipality_gdf = gpd.read_file('./data/external_data/REFNIS_CODES.geojson', driver='GeoJSON').to_crs(epsg=4326)

    # Perform spatial join with municipality data
    joined_data = gpd.sjoin(geo_data, municipality_gdf, how='left', predicate='within')

    # Load population density data
    pop_density_data = pd.read_excel('./data/external_data/PopDensity.xlsx')

    # Convert columns to int type
    pop_density_data['Refnis'] = pop_density_data['Refnis'].astype(int)
    
    # Replace NaN values in 'cd_munty_refnis' column
    joined_data['cd_munty_refnis'] = joined_data['cd_munty_refnis'].fillna(-1)  # Use -1 as a placeholder value
    
    # Convert 'cd_munty_refnis' column to int type
    joined_data['cd_munty_refnis'] = joined_data['cd_munty_refnis'].astype(int)

    # Perform second join with population density data
    joined_data = joined_data.merge(pop_density_data, left_on='cd_munty_refnis', right_on='Refnis', how='left')

    # Convert the result to a DataFrame
    joined_data = pd.DataFrame(joined_data)

    joined_data.to_csv('./data/join_data.csv', index=False)

    # Return the resulting DataFrame
    return joined_data

def clean_data(joined_data):
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

    Parameters:
    raw_data (DataFrame): The raw DataFrame to be cleaned
    """

    cleaned_data = joined_data.copy()

    # Task 1: Drop rows with empty values in 'Price' and 'LivingArea' columns
    cleaned_data.dropna(subset=['Price', 'LivingArea'], inplace=True)
    
    # Task 2: Remove duplicates in the 'ID' column and where all columns but 'ID' are equal
    cleaned_data.drop_duplicates(subset='ID', inplace=True)
    cleaned_data.drop_duplicates(subset=cleaned_data.columns.difference(['ID']), keep='first', inplace=True)

    # Task 3: Convert empty values to 0 for specified columns; assumption that if blank then 0
    columns_to_fill_with_zero = ['Furnished', 'Fireplace', 'Terrace', 'TerraceArea', 'Garden', 'GardenArea', 'SwimmingPool', 'BidStylePricing', 'ViewCount', 'bookmarkCount']
    cleaned_data[columns_to_fill_with_zero] = cleaned_data[columns_to_fill_with_zero].fillna(0)

    # Task 4: Filter rows where SaleType == 'residential_sale' and BidStylePricing == 0
    cleaned_data = cleaned_data[(cleaned_data['SaleType'] == 'residential_sale') & (cleaned_data['BidStylePricing'] == 0)].copy()

    # Task 5: Remove specified columns
    columns_to_drop = ['PropertyUrl', 'Street', 'HouseNumber', 'Box', 'Floor', 'SaleType', 'BidStylePricing', 'Property url']
    cleaned_data.drop(columns=columns_to_drop, inplace=True)

    # Task 6: Adjust text format
    columns_to_str = ['City', 'Region', 'District', 'Province', 'PropertyType', 'PropertySubType', 'KitchenType', 'Condition', 'EPCScore']

    def adjust_text_format(x):
        if isinstance(x, str):
            return x.title()
        else:
            return x

    cleaned_data.loc[:, columns_to_str] = cleaned_data.loc[:, columns_to_str].map(adjust_text_format)

    # Task 7: Remove leading and trailing spaces from string columns
    cleaned_data.loc[:, columns_to_str] = cleaned_data.loc[:, columns_to_str].apply(lambda x: x.str.strip() if isinstance(x, str) else x)

    # Task 8: Replace the symbol '�' with 'e' in all string columns
    cleaned_data = cleaned_data.map(lambda x: x.replace('�', 'e') if isinstance(x, str) else x)

    # Task 9: Fill missing values with None and convert specified columns to float64 type
    columns_to_fill_with_none = ['EnergyConsumptionPerSqm']
    cleaned_data[columns_to_fill_with_none] = cleaned_data[columns_to_fill_with_none].where(cleaned_data[columns_to_fill_with_none].notna(), None)

    columns_to_float64 = ['Price', 'LivingArea', 'TerraceArea', 'GardenArea', 'EnergyConsumptionPerSqm']
    cleaned_data[columns_to_float64] = cleaned_data[columns_to_float64].astype(float)

    # Task 10: Convert specified columns to Int64 type
    columns_to_int64 = ['ID', 'PostalCode', 'ConstructionYear', 'BedroomCount', 'Furnished', 'Fireplace', 'Terrace', 'Garden', 'Facades', 'SwimmingPool', 'bookmarkCount', 'ViewCount']
    cleaned_data[columns_to_int64] = cleaned_data[columns_to_int64].astype(float).round().astype('Int64')

    # Task 11: Replace any ConstructionYear > current_year + 10 with None
    current_year = datetime.now().year
    max_construction_year = current_year + 10
    cleaned_data['ConstructionYear'] = cleaned_data['ConstructionYear'].where(cleaned_data['ConstructionYear'] <= max_construction_year, None)

    # Task 12: Trim text after and including '_' from the 'EPCScore' column
    cleaned_data['EPCScore'] = cleaned_data['EPCScore'].str.split('_').str[0]

    # Task 13: Convert 'ListingCreateDate', 'ListingExpirationDate', and 'ListingCloseDate' to Date type with standard YYYY-MM-DD format
    date_columns = ['ListingCreateDate', 'ListingExpirationDate', 'ListingCloseDate']
    for col in date_columns:
        cleaned_data[col] = pd.to_datetime(cleaned_data[col], format='ISO8601').dt.date

    # Task 14: Replace values less than or equal to 0 in 'EnergyConsumptionPerSqm' with 0
    cleaned_data.loc[cleaned_data['EnergyConsumptionPerSqm'] < 0, 'EnergyConsumptionPerSqm'] = 0

    # Task 15: Calculate 'TotalArea'
    cleaned_data['TotalArea'] = cleaned_data['LivingArea'] + cleaned_data['GardenArea'] + cleaned_data['TerraceArea']

    # Task 16: Calculate 'PricePerLivingSquareMeter'
    cleaned_data['PricePerLivingSquareMeter'] = (cleaned_data['Price'] / cleaned_data['LivingArea']).round().astype(int)

    # Task 17: Calculate 'PricePerTotalSquareMeter'
    cleaned_data['PricePerTotalSquareMeter'] = (cleaned_data['Price'] / cleaned_data['TotalArea']).round().astype(int)

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

    cleaned_data['Condition#'] = cleaned_data['Condition'].map(condition_mapping)
    cleaned_data['KitchenType#'] = cleaned_data['KitchenType'].map(kitchen_mapping)

    # Return the cleaned DataFrame
    return cleaned_data

def prepare_model_data(cleaned_data):
    """
    Prepare model data by removing outliers from cleaned data.

    Args:
        cleaned_data (DataFrame): The cleaned DataFrame.

    Returns:
        model_data (DataFrame): DataFrame with outliers removed.
    """
    
    model_data = cleaned_data.copy()
    
    def remove_outliers(data, column, grouping_cols):
        """
        Remove outliers from a DataFrame based on a specified column and grouping columns using the IQR method.

        Args:
            data (DataFrame): The DataFrame from which outliers will be removed.
            column (str): The column name used for outlier detection.
            grouping_cols (list): List of columns to group by for more granular outlier removal.

        Returns:
            DataFrame: DataFrame with outliers removed.
        """
        grouped = data.groupby(grouping_cols)
        outliers_removed = grouped.apply(lambda x: x[(x[column] >= x[column].quantile(0.25) - 1.5 * (x[column].quantile(0.75) - x[column].quantile(0.25))) &
                                                    (x[column] <= x[column].quantile(0.75) + 1.5 * (x[column].quantile(0.75) - x[column].quantile(0.25)))])
        return outliers_removed.reset_index(drop=True)

    # Define the grouping columns
    grouping_cols = ['Refnis', 'PropertySubType']

    # Remove outliers
    model_data = remove_outliers(model_data, 'PricePerLivingSquareMeter', grouping_cols)

    # Return the cleaned DataFrame
    return model_data

def execute_data_prep(refresh=False):
    start_time = time.time()
    print("Program initiated:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))

    raw_data = import_data(refresh)
    joined_data = join_data(raw_data)
    cleaned_data = clean_data(joined_data)
    model_data = prepare_model_data(cleaned_data)

    calculate_runtime(start_time)
    print("Program completed:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    return raw_data, joined_data, cleaned_data, model_data