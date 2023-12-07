#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd 


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Downloading artififact from W&B")
    # use the input artifacts specified in args
    artifact_path = run.use_artifact(args.input_artifact).file()
    df_input = pd.read_csv(artifact_path)

    # Drop outliers
    logger.info('Drop outliers based on user specified min price and max price')
    min_price = args.min_price
    max_price = args.max_price
    idx = df_input['price'].between(min_price, max_price)
    df_outlier_dropped = df_input[idx].copy()

    # Convert last_review to datetime
    logger.info("Convert last_review to datetime")
    df_outlier_dropped['last_review'] = pd.to_datetime(df_outlier_dropped['last_review'])

    # drop outlier based on longitude and latitude
    logger.info('Drop outliers which fall out of the specified longitude and latitude')
    idx = df_outlier_dropped['longitude'].between(-74.25, -73.50) & df_outlier_dropped['latitude'].between(40.5, 41.2)
    df_output = df_outlier_dropped[idx].copy() 

    # save dataframe
    logger.info(f'Save dataframe')
    df_output.to_csv(args.output_artifact, index = False)

    logger.info('Logging artifact from W&B')
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )

    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type= str,
        help= 'Name for input artifact',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type= str,
        help= 'Name for output artifact',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='Type for the artifact',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='Description for the output artifact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='Minimum price',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='Maximum price',
        required=True
    )


    args = parser.parse_args()

    go(args)
