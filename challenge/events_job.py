import sys

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import types as st
from pyspark.sql import functions as sf

EVENT_SCHEMA = st.StructType([
    st.StructField("event_id", st.StringType()),
    st.StructField("timestamp", st.TimestampType()),
    st.StructField("domain", st.StringType()),
    st.StructField("event_type", st.StringType()),
    st.StructField("data", st.StringType())
])

spark_session = SparkSession.builder.appName("Pismo Challenge").getOrCreate()


def read_events_data(input_path):
    """Reads a folder with json files matching the events schema

    :param str input_path: The folder path with the json files

    :return: DataFrame object containing events

    """
    events_data = spark_session.read.json(
        input_path,
        schema=EVENT_SCHEMA
    )
    return events_data


def dedup_events(events_df):
    """Deduplicates a DataFrame object based on the newest timestamp field
    grouping by the event_id column

    :param str events_df: The DataFrame object matching the events schema

    :return: DataFrame object deduplicated

    """
    last_timestamp = Window.partitionBy("event_id") \
        .orderBy(sf.col("timestamp").desc())

    events_df = events_df.distinct() \
        .withColumn('distinct', sf.row_number().over(last_timestamp)) \
        .filter('distinct == 1') \
        .drop('distinct')

    return events_df


def trigger_events_processing(events_df, output_path):
    """Triggers the events processing partitioning data by domain and
    event_type. Also, all events will be written paritioned by year, month
    and day

    :param str events_df: The DataFrame object matching the events schema
    :param str output_path: The path where the events should be written to

    :return: The DataFrameWriter object

    """
    events_df = events_df.withColumn(
        'year', sf.year(sf.col('timestamp'))
    ).withColumn(
        'month', sf.month(sf.col('timestamp'))
    ).withColumn(
        'day', sf.dayofmonth(sf.col('timestamp'))
    )

    return events_df.write.partitionBy(
        'domain', 'event_type', 'year', 'month', 'day'
    ).parquet(path=output_path)


def process_events(input_path, output_path):
    """Reads events in the provided path and triggers the events processing
    partitioning data by domain and event_type. Also, all events will be
    written paritioned by year, month and day in the given output path.

    :param str output_path: The path where the events should be read from
    :param str output_path: The path where the events should be written to

    """
    events_data = read_events_data(input_path)
    events_data = dedup_events(events_data)
    trigger_events_processing(events_data, output_path).awaitTermination()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        input_arg_path = sys.argv[1]
        output_arg_path = sys.argv[2]
        process_events(
            input_path=input_arg_path,
            output_path=output_arg_path
        )
