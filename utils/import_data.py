import pandas as pd

def import_data():
    """
    Import raw data from a CSV file.
    
    Returns:
    raw_data (DataFrame): Raw DataFrame containing the imported data.
    """
    # Importing the data from the URL
    raw_data = pd.read_csv("https://raw.githubusercontent.com/bear-revels/immo-eliza-scraping-Python_Pricers/main/data/all_property_details.csv")

    # Storing locally without the index column
    raw_data.to_csv('./source/raw_data.csv', index=False)

    return raw_data