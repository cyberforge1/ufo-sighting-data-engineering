# pyspark_etl_scripts/kaggle_download.py

from kaggle.api.kaggle_api_extended import KaggleApi

def download_ufo_dataset():
    api = KaggleApi()
    api.authenticate()

    dataset_path = "nuforc/ufo-sightings"
    api.dataset_download_files(dataset_path, path="./data", unzip=True)
    print("Dataset downloaded and unzipped in ./data")


download_ufo_dataset()