from utils.data_prep import DataPrep

if __name__ == "__main__":
    # Import raw data
    raw_data = DataPrep.import_data()
    
    # Clean data
    cleaned_data = DataPrep.clean_data(raw_data)
    
    # Join data
    joined_data = DataPrep.join_data(cleaned_data)
    
    # Model data
    model_data = DataPrep.model_data(cleaned_data)
    
    # Write to CSV files
    raw_data.to_csv('./source/raw_data.csv', index=False)
    model_data.to_csv('./source/model_data.csv', index=False)