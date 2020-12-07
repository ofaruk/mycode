from google.cloud import bigquery
from google.cloud import monitoring_v3
from datetime import datetime, timedelta
import time
import base64
import json


# Instantiate Google Clients
stackdriver_client = monitoring_v3.MetricServiceClient()
bigquery_client = bigquery.Client()
pdate = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

def bq_rowcount(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    payload = data['data']
    if 'data' in data:
        payload = base64.b64decode(data['data']).decode('utf-8')
    json_payload = json.loads(payload)
    gcp_project = '{}'.format(json_payload['project'])
    list_of_bq_tables = json_payload['bq_tables']
    monitoring_project = "projects/{}".format(json_payload['project'])

    for x in list_of_bq_tables:
        bq_project, query, query_devices, query_messages, query_dos, query_trips_devices, bq_dataset, bq_table = create_row_count_query(x)
        if bq_table == "pda_gps_data":
            metric_name = ["number_of_devices", "number_of_messages"]
            query_job_devices = bigquery_client.query(query_devices, project=gcp_project, location='EU')
            query_job_messages = bigquery_client.query(query_messages, project=gcp_project, location='EU')
            results_devices = query_job_devices.result()  # Waits for job to complete.
            results_df_devices = results_devices.to_dataframe()
            results_messages = query_job_messages.result()  # Waits for job to complete.
            results_df_messages = results_messages.to_dataframe()
            for metric in metric_name:
                descriptor_type_name = 'custom.googleapis.com/fdp-data-processing/' + bq_dataset + '/' + bq_table + '/' + metric
                if metric == "number_of_devices":
                    add_metric_point(descriptor_type_name, monitoring_project, results_df_devices, metric)
                else:
                    add_metric_point(descriptor_type_name, monitoring_project, results_df_messages, metric)

        elif bq_table == "route_summary":
            metric_name = ["total_DOs", "total_row_count_table_1"]
            query_job_dos = bigquery_client.query(query_dos, project=gcp_project, location='EU')
            results_dos = query_job_dos.result()  # Waits for job to complete.
            results_df_dos = results_dos.to_dataframe()
            for metric in metric_name:
                descriptor_type_name = 'custom.googleapis.com/fdp-data-processing/' + bq_dataset + '/' + bq_table + '/' + metric
                add_metric_point(descriptor_type_name, monitoring_project, results_df_dos, metric)

        elif bq_table == "trip":
            metric_name = ["total_trips_Devices", "total_row_count_table_1"]
            query_job_trips_devices = bigquery_client.query(query_trips_devices, project=gcp_project, location='EU')
            results_trips_devices = query_job_trips_devices.result()  # Waits for job to complete.
            results_df_trips_devices = results_trips_devices.to_dataframe()
            for metric in metric_name:
                descriptor_type_name = 'custom.googleapis.com/fdp-data-processing/' + bq_dataset + '/' + bq_table + '/' + metric
                add_metric_point(descriptor_type_name, monitoring_project, results_df_trips_devices,metric)
        else:
            metric_name = ["total_row_count_table_1"]
            query_job = bigquery_client.query(query, project=gcp_project, location='EU')
            results = query_job.result()  # Waits for job to complete.
            results_df = results.to_dataframe()
            for metric in metric_name:
                descriptor_type_name = 'custom.googleapis.com/fdp-data-processing/' + bq_dataset + '/' + bq_table + '/' + metric
                add_metric_point(descriptor_type_name, monitoring_project, results_df, metric)

def create_row_count_query(current_bq_table):
    """Creates the bq query which returns row count for a table
    Args:
        current_bq_table (str): The full of the bigquery table to be queried
    Returns:
        table project, the query to be used, the dataset and the table
    """
    project = '.'.join(current_bq_table.split('.')[0:1])
    dataset = '.'.join(current_bq_table.split('.')[1:2])

    full_dataset = '.'.join(current_bq_table.split('.')[0:2])

    table = current_bq_table.split('.')[-1]

    number_of_devices = """ """
    number_of_messages = """ """
    total_number_of_rows_query = """ """
    total_number_of_trips_devices = """ """
    total_number_of_dos = """ """

    if table == "pda_gps_data":

        number_of_devices = """
        SELECT
            count(DISTINCT deviceidentifier) as number_of_devices
        FROM `{0}.{1}`
        WHERE pdate = "{2}"
        """
        number_of_devices = number_of_devices.format(full_dataset, table, pdate)

        number_of_messages = """
        SELECT count(*) as number_of_messages FROM
        (
         SELECT DISTINCT headerflag,submittedtimestamp,receivedonservertimestamp,eventid,pdapname,appname,deviceidentifier
        FROM `{0}.{1}`
        WHERE pdate = "{2}" )
        """
        number_of_messages = number_of_messages.format(full_dataset, table, pdate)
    elif table == "route_summary":
        total_number_of_dos = """
        SELECT
            count(distinct locationid) as total_DOs , count(*) as total_row_count_table_1
        FROM `{0}.{1}`
        WHERE pdate = "{2}"
        """
        total_number_of_dos = total_number_of_dos.format(full_dataset, table, pdate)
    elif table == "trip":
        total_number_of_trips_devices = """
        SELECT
            count(distinct deviceidentifier) as total_trips_Devices , count(*) as total_row_count_table_1
        FROM `{0}.{1}`
        WHERE pdate = "{2}"
        """
        total_number_of_trips_devices = total_number_of_trips_devices.format(full_dataset, table, pdate)
    else:
        total_number_of_rows_query = """
        SELECT
            count(*) as total_row_count_table_1
        FROM `{0}.{1}`
        WHERE pdate = "{2}"
        """
        total_number_of_rows_query = total_number_of_rows_query.format(full_dataset, table,pdate)

    return project, total_number_of_rows_query, number_of_devices, number_of_messages, total_number_of_dos, total_number_of_trips_devices, dataset, table

def add_metric_point(descriptor_type, project, value_to_add, item_df):
    """Adds a value to the custom metric
    Args:
        descriptor_type (str):  Name of the custom metric
        project (str):          GCP project name
        value_to_add (str):     The number of rows returned by the bq
    Returns:
    """
    series = monitoring_v3.types.TimeSeries()
    series.metric.type = descriptor_type
    series.resource.type = 'global'
    point = series.points.add()
    metric = "{}".format(item_df)
    point.value.int64_value = value_to_add[metric][0]
    now = time.time()
    point.interval.end_time.seconds = int(now)
    point.interval.end_time.nanos = int(
        (now - point.interval.end_time.seconds) * 10**9)
    stackdriver_client.create_time_series(project, [series])