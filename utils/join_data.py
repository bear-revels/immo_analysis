import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

def join_data(geo_data):
    # Load real estate data into a GeoDataFrame
    geo_data = gpd.read_file('./source/clean_data.csv')

    # Load municipality data into a GeoDataFrame and specify CRS as EPSG:4326
    municipality_gdf = gpd.read_file('./source/REFNIS_CODES.geojson', driver='GeoJSON')
    municipality_gdf = municipality_gdf.to_crs(epsg=4326)

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
    joined_data = gpd.sjoin(geo_data, municipality_gdf, how='left', op='within')

    # Drop unnecessary columns from the spatial join result if they exist
    columns_to_drop = [
        'geometry', 
        'ogc_fid0', 
        'tx_sector_descr_nl',
        'tx_sector_descr_fr', 
        'tx_sector_descr_de', 
        'cd_sub_munty', 
        'tx_sub_munty_nl', 
        'tx_sub_munty_fr', 
        'tx_munty_dstr', 
        'tx_munty_descr_fr', 
        'tx_munty_descr_de', 
        'cd_dstr_refnis', 
        'tx_adm_dstr_descr_nl', 
        'tx_adm_dstr_descr_fr', 
        'tx_adm_dstr_descr_de', 
        'cd_prov_refnis', 
        'tx_prov_descr_nl', 
        'tx_prov_descr_fr', 
        'tx_prov_descr_de', 
        'cd_rgn_refnis', 
        'tx_rgn_descr_nl', 
        'tx_rgn_descr_fr', 
        'tx_rgn_descr_de', 
        'cd_country', 
        'cd_nuts_lvl1', 
        'cd_nuts_lvl2', 
        'cd_nuts_lvl3', 
        'ms_area_ha', 
        'ms_perimeter_m', 
        'dt_situation'
    ]
    columns_to_drop = [col for col in columns_to_drop if col in joined_data.columns]
    joined_data = joined_data.drop(columns=columns_to_drop)

    # Load population density data
    pop_density_data = pd.read_csv('./source/PopDensity.xlsx')

    # Perform second join with population density data
    joined_data = joined_data.merge(pop_density_data, left_on='cd_munty_refnis', right_on='Refnis', how='left')

    # Save the resulting DataFrame to a CSV file
    joined_data.to_csv('./source/join_data.csv', index=False)