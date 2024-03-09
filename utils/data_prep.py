import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime

class DataPrep:
    raw_data = None

    @staticmethod
    def import_data():
        if DataPrep.raw_data is None:
            # Importing the data from the URL
            DataPrep.raw_data = pd.read_csv("https://raw.githubusercontent.com/bear-revels/immo-eliza-scraping-Python_Pricers/main/data/all_property_details.csv", dtype={'PostalCode': str})
            # Storing locally without the index column
            DataPrep.raw_data.to_csv('./source/raw_data.csv', index=False, encoding='utf-8')
        return DataPrep.raw_data

    @staticmethod
    def join_data():
        # Load real estate data into a GeoDataFrame
        geo_data = gpd.read_file('./source/raw_data.csv')
        # Load municipality data into a GeoDataFrame and specify CRS as EPSG:4326
        municipality_gdf = gpd.read_file('./source/REFNIS_CODES.geojson', driver='GeoJSON').to_crs(epsg=4326)
        # Explicitly set CRS if not set
        if not geo_data.crs:
            geo_data.crs = municipality_gdf.crs

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

        # Perform spatial join with municipality data
        joined_data = gpd.sjoin(geo_data, municipality_gdf, how='left', predicate='within')

        # Load population density data
        pop_density_data = pd.read_excel('./source/PopDensity.xlsx')

        # Convert columns to int type
        pop_density_data['Refnis'] = pop_density_data['Refnis'].astype(int)
        
        # Replace NaN values in 'cd_munty_refnis' column
        joined_data['cd_munty_refnis'] = joined_data['cd_munty_refnis'].fillna(-1)  # Use -1 as a placeholder value
        
        # Convert 'cd_munty_refnis' column to int type
        joined_data['cd_munty_refnis'] = joined_data['cd_munty_refnis'].astype(int)

        # Perform second join with population density data
        joined_data = joined_data.merge(pop_density_data, left_on='cd_munty_refnis', right_on='Refnis', how='left')

        # Save the resulting DataFrame to a CSV file
        joined_data.to_csv('./source/clean_data.csv', index=False)

        # Return the resulting DataFrame
        return joined_data
    
    @staticmethod
    def clean_data(join_data):

        # Task 1: Drop rows with empty values in 'Price' and 'LivingArea' columns
        join_data.dropna(subset=['Price', 'LivingArea'], inplace=True)
        
        # Task 2: Remove duplicates in the 'ID' column and where all columns but 'ID' are equal
        join_data.drop_duplicates(subset='ID', inplace=True)
        join_data.drop_duplicates(subset=join_data.columns.difference(['ID']), keep='first', inplace=True)

        # Task 3: Convert empty values to 0 for specified columns; assumption that if blank then 0
        columns_to_fill_with_zero = ['Furnished', 'Fireplace', 'Terrace', 'TerraceArea', 'Garden', 'GardenArea', 'SwimmingPool', 'BidStylePricing', 'ViewCount', 'bookmarkCount']
        join_data[columns_to_fill_with_zero] = join_data[columns_to_fill_with_zero].fillna(0)

        # Task 4: Filter rows where SaleType == 'residential_sale' and BidStylePricing == 0
        join_data = join_data[(join_data['SaleType'] == 'residential_sale') & (join_data['BidStylePricing'] == 0)].copy()

        # Task 5: Remove specified columns
        columns_to_drop = ['PropertyUrl', 'Street', 'HouseNumber', 'Box', 'Floor', 'SaleType', 'BidStylePricing', 'Property url']
        join_data.drop(columns=columns_to_drop, inplace=True)

        # Task 6: Adjust text format
        columns_to_str = ['City', 'Region', 'District', 'Province', 'PropertyType', 'PropertySubType', 'KitchenType', 'Condition', 'EPCScore']

        def adjust_text_format(x):
            if isinstance(x, str):
                return x.title()
            else:
                return x

        join_data[columns_to_str] = join_data[columns_to_str].applymap(adjust_text_format)

        # Task 7: Remove leading and trailing spaces from string columns
        join_data[columns_to_str] = join_data[columns_to_str].apply(lambda x: x.str.strip() if isinstance(x, str) else x)

        # Task 8: Replace the symbol '�' with 'e' in all string columns
        join_data = join_data.applymap(lambda x: x.replace('�', 'e') if isinstance(x, str) else x)

        # Task 9: Fill missing values with None and convert specified columns to float64 type
        columns_to_fill_with_none = ['EnergyConsumptionPerSqm']
        join_data[columns_to_fill_with_none] = join_data[columns_to_fill_with_none].where(join_data[columns_to_fill_with_none].notna(), None)

        columns_to_float64 = ['Price', 'LivingArea', 'TerraceArea', 'GardenArea', 'EnergyConsumptionPerSqm']
        join_data[columns_to_float64] = join_data[columns_to_float64].astype(float)

        # Task 10: Convert specified columns to Int64 type
        columns_to_int64 = ['ID', 'PostalCode', 'ConstructionYear', 'BedroomCount', 'Furnished', 'Fireplace', 'Terrace', 'Garden', 'Facades', 'SwimmingPool', 'bookmarkCount', 'ViewCount']
        join_data[columns_to_int64] = join_data[columns_to_int64].astype(float).round().astype('Int64')

        # Task 11: Replace any ConstructionYear > current_year + 10 with None
        current_year = datetime.now().year
        max_construction_year = current_year + 10
        join_data['ConstructionYear'] = join_data['ConstructionYear'].where(join_data['ConstructionYear'] <= max_construction_year, None)

        # Task 12: Trim text after and including '_' from the 'EPCScore' column
        join_data['EPCScore'] = join_data['EPCScore'].str.split('_').str[0]

       # Task 13: Convert 'ListingCreateDate', 'ListingExpirationDate', and 'ListingCloseDate' to Date type with correct format
        date_columns = ['ListingCreateDate', 'ListingExpirationDate', 'ListingCloseDate']
        for col in date_columns:
            join_data[col] = pd.to_datetime(join_data[col], format='%Y-%m-%dT%H:%M:%S.%f%z', errors='coerce').dt.date

        # Task 14: Replace values less than or equal to 0 in 'EnergyConsumptionPerSqm' with 0
        join_data.loc[join_data['EnergyConsumptionPerSqm'] < 0, 'EnergyConsumptionPerSqm'] = 0

        # Task 15: Calculate 'TotalArea'
        join_data['TotalArea'] = join_data['LivingArea'] + join_data['GardenArea'] + join_data['TerraceArea']

        # Task 16: Calculate 'PricePerLivingSquareMeter'
        join_data['PricePerLivingSquareMeter'] = (join_data['Price'] / join_data['LivingArea']).round().astype(int)

        # Task 17: Calculate 'PricePerTotalSquareMeter'
        join_data['PricePerTotalSquareMeter'] = (join_data['Price'] / join_data['TotalArea']).round().astype(int)

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

        join_data['Condition#'] = join_data['Condition'].map(condition_mapping)
        join_data['KitchenType#'] = join_data['KitchenType'].map(kitchen_mapping)

        # Save the resulting DataFrame to a CSV file
        join_data.to_csv('./source/clean_data.csv', index=False)
        
        # Return the cleaned DataFrame
        return join_data
    
    @staticmethod
    def model_data(cleaned_data):
        """
        Prepare model data by removing outliers from cleaned data.

        Args:
            cleaned_data (DataFrame): The cleaned DataFrame.

        Returns:
            model_data (DataFrame): DataFrame with outliers removed.
        """
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
        cleaned_data = remove_outliers(cleaned_data, 'PricePerLivingSquareMeter', grouping_cols)

        # Storing locally without the index column
        cleaned_data.to_csv('./source/clean_data.csv', index=False)

        # Return the cleaned DataFrame
        return cleaned_data