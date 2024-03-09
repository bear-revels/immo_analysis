from utils.data_prep import DataPrep

# Import raw data
raw_data = DataPrep.import_data()

# Join the data
joined_data = DataPrep.join_data()

# Clean the joined data
cleaned_data = DataPrep.clean_data(joined_data)

# Model the cleaned data
model_data = DataPrep.model_data(cleaned_data)