import pandas as pd
from sqlalchemy import create_engine

def extract_data(file_path):
    """Extract data from a CSV file."""
    try:
        data = pd.read_csv(file_path)
        print("Data extraction successful!")
        return data
    except Exception as e:
        print(f"Error occurred during data extraction: {e}")
        return None

def transform_data(data):
    """Transform the extracted data."""
    try:
        # Add a new column 'salary' with dummy values
        data['salary'] = [50000, 60000, 70000, 80000, 90000]

        # Filter out employees younger than 30
        data = data[data['age'] >= 30]

        print("Data transformation successful!")
        return data
    except Exception as e:
        print(f"Error occurred during data transformation: {e}")
        return None

def load_data(data, db_name='example.db', table_name='employees'):
    """Load transformed data into a SQL database."""
    try:
        engine = create_engine(f'sqlite:///{db_name}')
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Data load successful!")
    except Exception as e:
        print(f"Error occurred during data loading: {e}")

if __name__ == "__main__":
    # File path to the CSV
    file_path = 'data.csv'

    # Step 3: Extract
    extracted_data = extract_data(file_path)

    if extracted_data is not None:
        # Step 4: Transform
        transformed_data = transform_data(extracted_data)

        if transformed_data is not None:
            # Step 5: Load
            load_data(transformed_data)
