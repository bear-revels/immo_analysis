import pandas as pd

def remove_outliers(data, column):
    """
    Remove outliers from a DataFrame based on a specified column using the IQR method.

    Args:
        data (DataFrame): The DataFrame from which outliers will be removed.
        column (str): The column name used for outlier detection.

    Returns:
        DataFrame: DataFrame with outliers removed.
    """
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return data[(data[column] >= lower_bound) & (data[column] <= upper_bound)]

def prepare_model_data(cleaned_data):
    """
    Prepare model data by removing outliers from cleaned data and saving the result to a CSV file.

    Args:
        cleaned_data_file (str): Path to the cleaned data file.
        output_file (str): Path to save the model data CSV file.

    Returns:
        None
    """
    # Load cleaned data
    cleaned_data = pd.read_csv('./source/join_data.csv')

    # Remove outliers
    for col in ['PricePerLivingSquareMeter']:
        cleaned_data = remove_outliers(cleaned_data, col)
    
    # Save the resulting dataframe to a CSV file
    cleaned_data.to_csv('./source/model_data.csv', index=False)
