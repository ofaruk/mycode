import argparse
import csv
import logging
import os
import sys

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict


class DataIngestion(object):
    """A helper class which contains the logic to translate the file into a
  format BigQuery will accept."""

    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        print(dir_path)
        self.bq_schema_str = ''
        self.base_csv_schema_str = ''
        self.option_csv_schema_str = ''
        # This is the schema of the destination table in BigQuery.
        bq_schema_file = os.path.join(dir_path,
                                      'resources/option_profits_schema.json')
        with open(bq_schema_file) \
                as f:
            bq_sc_data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
            self.bq_schema_str = '{"fields": ' + bq_sc_data + '}'

        base_csv_schema_file = os.path.join(dir_path,
                                            'resources/base_csv_schema.json')
        with open(base_csv_schema_file) \
                as f:
            base_csv_sc_data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
            self.base_csv_schema_str = '{"fields": ' + base_csv_sc_data + '}'

        option_csv_schema_file = os.path.join(dir_path,
                                              'resources/options_csv_schema.json')
        with open(option_csv_schema_file) \
                as f:
            option_csv_sc_data = f.read()
            # Wrapping the schema in fields is required for the BigQuery API.
            self.option_csv_schema_str = '{"fields": ' + option_csv_sc_data + '}'

    def parse_input_csv(self, string_input, csv_file_type):
        """This method translates a single line of comma separated values to a
            dictionary which can be loaded into BigQuery.

        Args:
            string_input: A comma separated list of values
            csv_file_type: type of the csv file, either base csv or options
        """
        schema = []
        # Strip out return characters and quote characters.
        if csv_file_type == 'base_csv':
            schema = parse_table_schema_from_json(self.base_csv_schema_str)
        elif csv_file_type[:-2] == 'option_csv':
            schema = parse_table_schema_from_json(self.option_csv_schema_str)
        else:
            return  # as this method serves specifically two types of csv files

        field_map = [f for f in schema.fields]

        # Use a CSV Reader which can handle quoted strings etc.
        reader = csv.reader(string_input.split('\n'))
        for csv_row in reader:
            if (sys.version_info.major < 3.0):
                values = [x.decode('utf8') for x in csv_row]
            else:
                values = csv_row

            row = {}
            i = 0
            # Iterate over the values from our csv file, applying any transformation logic.
            for value in values:
                row[field_map[i].name] = value
                i += 1

            # different logic for base csv file and options csv file to add the key field
            if csv_file_type == 'base_csv':
                row['ModelOptionKey'] = '_'.join([row['Model_Text'].split()[0], row['Options_Code']])

            # keep only model&option and material cost in the dict
            elif csv_file_type == 'option_csv_m':
                row['ModelOptionKey'] = '_'.join([row['Model'], row['Option_Code']])
                del row['Model']
                del row['Option_Code']
                del row['Option_Desc']

            # keep only option code and material cost
            elif csv_file_type == 'option_csv_o':
                del row['Model']
                del row['Option_Desc']

            return row


def run(argv=None):
    """The main function which creates the pipeline and runs it."""
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.   Specifically
    # we have the input file to load, mapping file and the output table to write to.
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Base Table csv file to read.',
        default='gs://jlr-test-files/Base_dataset.csv')

    parser.add_argument(
        '--mapping',
        dest='mapping',
        required=False,
        help='Options Table csv file to read.',
        default='gs://jlr-test-files/Options_dataset.csv')

    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output BQ table to write results to.',
        default='transform_access.option_profits')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line.  This includes information like where Dataflow should store
    #  temp files, and what the project id is
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    schema = parse_table_schema_from_json(data_ingestion.bq_schema_str)

    def find_exact_cost(row, model_option_to_cost_map):
        row['Production_Cost'] = model_option_to_cost_map[row['ModelOptionKey']]
        return row

    def find_avg_cost(row, model_option_to_cost_map):
        row['Production_Cost'] = model_option_to_cost_map[row['Options_Code']]
        return row

    material_cost_m = \
        (p | 'Read From Text 1' >> beam.io.ReadFromText(known_args.mapping,
                                                        skip_header_lines=1)
         | 'String to BigQuery Row 1' >>
         beam.Map(lambda s: data_ingestion.parse_input_csv(s, 'option_csv_m')))

    # material_cost_o = \
    #     (p | 'Read From Text 2' >> beam.io.ReadFromText(known_args.mapping,
    #                                                   skip_header_lines=1)
    #      | 'String to BigQuery Row 2' >>
    #      beam.Map(lambda s: data_ingestion.parse_input_csv(s, 'option_csv_o')))

    (p
     | 'Read From Text' >> beam.io.ReadFromText(known_args.input,
                                                skip_header_lines=1)
     # Translates from the raw string data in the CSV to a dictionary.
     # The dictionary is a keyed by column names with the values being the values
     # we want to store in BigQuery.
     | 'String to BigQuery Row Base' >>
     beam.Map(lambda s: data_ingestion.parse_input_csv(s, 'base_csv'))
     # Here we pass in a side input, which is data that comes from options
     # CSV source.  The side input contains a map of model&option to material cost.
     | 'Join Data Exact' >> beam.Map(find_exact_cost, AsDict(material_cost_m))
     | 'Join Data Avg' >> beam.Map(find_avg_cost, AsDict(material_cost_m))

     # This is the final stage of the pipeline, where we define the destination
     #  of the data.  In this case we are writing to BigQuery.
     | 'Write to BigQuery' >> beam.io.Write(
                beam.io.BigQuerySink(
                    # The table name is a required argument for the BigQuery sink.
                    # In this case we use the value passed in from the command line.
                    known_args.output,
                    # Here we use the JSON schema read in from a JSON file.
                    # Specifying the schema allows the API to create the table correctly if it does not yet exist.
                    schema=schema,
                    # Creates the table in BigQuery if it does not yet exist.
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # Deletes all data in the BigQuery table before writing.
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
