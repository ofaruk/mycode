import os
import logging
import pandas as pd

from google.cloud import bigquery, storage


project_id = "jlr-test"
base_csv = "gs://jlr-test-files/Base_dataset.csv"
options_csv = "gs://jlr-test-files/Options_dataset.csv"


def calculate_profit(event, context):
    """ Loads data from csv, transforms the data end write into a bq table
    :steps:
        load csv files
        extract options and models from csv files
        match model&option in both datasets
        calculate average cost for each option
        enrich data with cost for each model&option
        write to bigquery table
        write test
    :return:
    """
    base_df = load_file(base_csv)
    options_df = load_file(options_csv)
    print(base_df.size())
    print(options_df.size())


def load_file(path):

    df = pd.read_csv(path)
    return df
