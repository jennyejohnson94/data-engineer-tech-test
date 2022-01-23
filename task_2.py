import apache_beam as beam
from datetime import datetime
import json


class TotalAmountPerDate(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'Split the rows into an array' >> beam.Map( lambda x: x.split(','))
            | 'Create set of date and amount' >> beam.Map(lambda x: (x[0].split(' ')[0], float(x[-1])))
            | 'Transactions with an amount greater than 20' >> beam.Filter(lambda x: x[1] > 20)
            | 'Exclude transactions before 2010' >> beam.Filter(lambda x: datetime.strptime(x[0], '%Y-%m-%d').year > 2010)
            | 'Sum total by dates' >> beam.CombinePerKey(sum)
            | 'To dictionary' >> beam.Map(lambda x: {'date': x[0], 'total_amount': x[1]})
            | 'To JSON' >> beam.Map(json.dumps)
        )


if __name__ == '__main__':
    with beam.Pipeline() as pipeline:
        transactions = (
            pipeline
            | 'Read the csv' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
            | TotalAmountPerDate()
            | 'Write to file' >> beam.io.WriteToText('./output/results', file_name_suffix='.json1.gz')
        )