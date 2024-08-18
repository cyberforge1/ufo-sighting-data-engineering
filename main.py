# main.py

from etl_scripts.kaggle_download import download_ufo_dataset
from etl_scripts.db_setup import create_table
from etl_scripts.load_data import load_data_to_db

if __name__ == "__main__":
    download_ufo_dataset()

    create_table()

    load_data_to_db('./data/scrubbed.csv')
