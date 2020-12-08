from __future__ import print_function

from airflow import utils
from airflow import models
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import gcs_to_bq
from airflow.operators.dummy_operator import DummyOperator

dag_config = models.Variable.get('variables', deserialize_json=True, default_var={})

bq_base_table_name = dag_config.get('bq_base_table_name')
bq_options_table_name = dag_config.get('bq_options_table_name')
gcs_bucket_name = dag_config.get('gcs_bucket_name')
csv_base_dataset = dag_config.get('csv_base_dataset')
csv_options_dataset = dag_config.get('csv_options_dataset')
bq_base_table_schema = dag_config.get('bq_base_table_schema')
bq_options_table_schema = dag_config.get('bq_options_table_schema')

default_dag_args = {
    'start_date': utils.dates.days_ago(2)
}

with models.DAG(
        'composer_transform',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    load_base_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_base',
        bucket='jlr-test-files2',
        source_objects=['Base_dataset.csv'],
        destination_project_dataset_table='airflow_jlr.base_table',  # bq_base_table_name,
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        schema_fields=bq_base_table_schema,
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    load_options_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='load_options',
        bucket='jlr-test-files2',
        source_objects=['Options_dataset.csv'],
        destination_project_dataset_table='airflow_jlr.options_table',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        schema_fields=bq_options_table_schema,
        write_disposition='WRITE_TRUNCATE',
        dag=dag)

    bq_calculate_profit = bigquery_operator.BigQueryOperator(
        task_id='calculate_profit',
        bql="""
        Create or replace View airflow_jlr.vw_option_profit AS
        SELECT
            b.Vehicle_ID,
            b.Option_Quantities,
            b.Option_Code,
            b.Option_Desc,
            b.Model_Text,
            b.Sales_Price,
            COALESCE(ZeroCost, b.Option_Quantities*Material_Cost, b.Option_Quantities*av.Average_Cost, Sales_Price*0.45) as Production_Cost,
            --below depends on business decision e.g. if no profit is expected from options costing zero; negative or positive, then profit is assumed 0
            CASE  
              WHEN ZeroCost = 0 THEN 0
              ELSE Sales_Price - COALESCE(ZeroCost, b.Option_Quantities*Material_Cost, b.Option_Quantities*av.Average_Cost, Sales_Price*0.45)
            END  as Profit
        FROM
        (
            SELECT
                Vehicle_ID,
                Option_Quantities,
                Option_Code,
                Option_Desc,
                Model_Text,
                CONCAT(SPLIT(Model_Text, ' ')[OFFSET(0)], "_", Option_Code) AS ModelOptionKey,
                Sales_Price,
                CASE
                    WHEN Sales_Price <= 0 THEN 0
                    ELSE null
                END AS ZeroCost
            FROM airflow_jlr.base_table
        ) b
        -- left join the average cost due to multiple occurrence of material cost for modeloptionkey in options table
        LEFT JOIN 
        (
            SELECT Concat(Model, "_", Option_Code) as ModelOptionKey, AVG(Material_Cost) as Material_Cost 
            FROM  airflow_jlr.options_table Group by 1
        ) o on b.ModelOptionKey = o.ModelOptionKey
        LEFT JOIN
        (
            SELECT Option_Code, AVG(Material_Cost) as Average_Cost FROM airflow_jlr.options_table 
            GROUP BY Option_Code
        ) av on b.Option_Code = av.Option_Code
        """,
        use_legacy_sql=False,
    )

    end = DummyOperator(
        trigger_rule='one_success',
        task_id='end')

    [load_base_csv, load_options_csv] >> bq_calculate_profit >> end
