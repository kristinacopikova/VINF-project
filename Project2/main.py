import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lower, regexp_extract
import os


# merges parsed wikipedia files with original parsed dataset from crawler
def merge_wiki():
    dir_path = "Project2/wikipedia_full"
    dir = os.listdir(dir_path)
    csv_files = [filename for filename in dir if filename.endswith(".csv")]
    wiki_data = pd.DataFrame()

    # we have to merge all files created by spark parallel processing
    for file in csv_files:
        csv_data = pd.read_csv(dir_path + "/" + file, delimiter=";\t")
        if wiki_data.shape[0] == 0:
            wiki_data = csv_data
        else:
            wiki_data = pd.concat([wiki_data, csv_data], ignore_index=True)

    print(wiki_data.shape)
    print(wiki_data.head())

    csv_data = pd.read_csv("Project2/parsed.csv", delimiter=",")

    df = csv_data.merge(wiki_data, how="left", left_on="Animal Name", right_on="title")
    print(df[~df["title"].isnull()].shape)
    df.replace('""', None, inplace=True)  # to make proper datset we have to replace all empty string with null values

    df.to_csv('Project2/merged_wiki.csv')


# function uses spark to parallely parse wikipedia and save the csv files into designated folder
def parse_wiki():
    # spark-submit --packages com.databricks:spark-xml_2.12:0.15.0 Project2/main.py

    spark = SparkSession.builder.appName("WikipediaParser").getOrCreate()
    # struct is used to define what fields we want to load
    schema = StructType([
        StructField("title", StringType(), True),
        StructField(
            "revision",
            StructType([StructField(
                "text",
                StructType([StructField("_VALUE", StringType(), True), ]),
                True
            ), ]),
            True
        ),
    ])

    # load wikipedia data according to defined structure
    wiki_data = spark.read.format("xml").option("rowTag", "page").schema(schema).load(
        "Project2/dumps/enwiki-latest-pages-articles.xml.bz2")

    print(wiki_data.printSchema())

    # we have to create a formatted dataframe with page title and description of the page
    wiki_data_parsed = wiki_data.select("title", col("revision.text._VALUE").alias("description"))

    # from newly formed dataframe we used regexp to select values that will be mapped to parsed data
    wiki_data_parsed = wiki_data_parsed.withColumn(
        'genus', regexp_extract(lower("description"), r'\|\s*genus\s*=\s*([^|\n]+)', 1)
    ).withColumn(
        'species', regexp_extract(lower("description"), r'\|\s*species\s*=\s*([^|\n]+)', 1)
    ).withColumn(
        'name', regexp_extract(lower("description"), r'\|\s*name\s*=\s*([^|\n]+)', 1)
    )

    # to reduce the number of saved data we will remove anything that didn't have any of the regexp outputs
    wiki_data_parsed = wiki_data_parsed.filter(
        (col("genus") != '') | (col("species") != "") | (col("name") != "")
    ).select("title", 'genus', 'species', 'name')

    print(wiki_data_parsed.show())
    print(wiki_data_parsed.printSchema())

    # csv files are parallely populated and  saved into wikipedia_fulll folder
    wiki_data_parsed.write.mode('overwrite').options(header='True', delimiter=';\t').csv("Project2/wikipedia_full")

    # Stop Spark session
    spark.stop()


# code will enable the wikipedia parsing with spark and finish by merging the files with original parsed data
if __name__ == '__main__':
    parse_wiki()
    merge_wiki()
