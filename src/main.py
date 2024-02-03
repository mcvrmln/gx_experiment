""" My first script with great expectations """

import datetime
import logging

import great_expectations as gx
import pandas as pd



SOURCE_FILE = './data/source/API_SP.POP.TOTL_DS2_en_csv_v2_6508519.csv'
logging.basicConfig(filename=f"./log/logfile_{datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}.log",
                    encoding='utf-8',
                    format='%(asctime)s %(filename)s %(funcName)s %(levelname)s %(message)s',
                    level=logging.INFO)
logging.info('Script starts')

def read_raw_data(file: str) -> pd.DataFrame:
    """ Reads the file and return it as a Pandas Dataframe """

    data = pd.read_csv(file, sep=',', header=2)
    n_columns = data.shape[1]
    data.drop(data.iloc[:, n_columns-1:n_columns], inplace=True, axis=1)
    logging.info(f'Size fo the raw data {data.shape}')
    long_data = pd.melt(data, id_vars=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'],
                        var_name='Year',
                        value_name='Population')
    logging.info(f'Size of the melted data {long_data.shape}')
    return long_data


def run_app():
    """ Run this script """
    # Load config

    # Extract data
    # dataset from: https://data.worldbank.org/indicator/SP.POP.TOTL
    logging.info('Read raw data')
    data = read_raw_data(SOURCE_FILE)

    # Verify data
    context = gx.get_context()
    datasource = context.sources.add_pandas(name="my_pandas_datasource")
    name = "World population"
    data_asset = datasource.add_dataframe_asset(name=name)
    batch_request = data_asset.build_batch_request(dataframe=data)
    context.add_or_update_expectation_suite("my_expectation_suite")
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="my_expectation_suite",
    )
    validator.expect_column_values_to_not_be_null(column="Country Code")
    print(validator.head())
    expectation_validation_result = validator.expect_column_values_to_not_be_null(
    column="Country Name"
    )
    print(expectation_validation_result)

    # Load data (save to parquet file)


""" To execute this script"""
if __name__ == "__main__":
    run_app()
