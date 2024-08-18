# local_etl_scripts/main.py

from kaggle_download import download_ufo_dataset
from db_setup import create_table
from load_data import load_data_to_db
from data_cleaning import clean_ufo_data

if __name__ == "__main__":
    download_ufo_dataset()

    raw_file_path = './data/complete.csv'
    cleaned_file_path = './data/local_cleaned_data.csv'
    clean_ufo_data(raw_file_path, cleaned_file_path)

    create_table()

    load_data_to_db(cleaned_file_path)
