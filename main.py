from utils.import_data import import_data
from utils.clean_data import clean_data
from utils.join_data import join_data
from utils.model_data import prepare_model_data

def main():
    # Import raw data
    raw_data = import_data()

    # Clean the data
    cleaned_data = clean_data(raw_data)

    # Join data
    joined_data = join_data(cleaned_data)

    # Prepare model data
    prepare_model_data(joined_data)

if __name__ == "__main__":
    main()