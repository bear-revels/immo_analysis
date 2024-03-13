from source.data_prep import execute_data_prep

def main():
    refresh_input = input("Do you want to refresh the dataset? (Yes/No): ").strip().lower()
    refresh_data = refresh_input == "yes"
    raw_data, joined_data, cleaned_data, model_data = execute_data_prep(refresh=refresh_data)

if __name__ == "__main__":
    main()