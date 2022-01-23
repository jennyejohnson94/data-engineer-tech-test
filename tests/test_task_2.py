import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from task_2 import TotalAmountPerDate


class TestTotalAmountPerDate(unittest.TestCase):

    def test_total_amount_per_date(self):
        transactions = [
            '2005-03-10 13:24 UTC,wallet00000jdhf98ew7r39845,wallet00000khji78iyujgjkll,12366.09',
            '2017-01-16 12:17 UTC,wallet00000sdfdrte45dtyghf,wallet00000derte4tdfgdfhbn,108654.83',
            '2017-01-16 12:20 UTC,wallet00000dfter6te5gdfgff,wallet00000df4trergdfvxcvn,50634.14',
            '2014-11-20 17:47 UTC,wallet0000034setrer6ertryh,wallet00000cfvfsdfw43rsdfs,6456345.60',
            '2012-05-27 05:01 UTC,wallet00000szdfse45etefyrt,wallet00000asnbwekur23yher,19.30'
        ]

        expected = [
            {'date': '2017-01-16', 'total_amount': 159288.97},
            {'date': '2014-11-20', 'total_amount': 6456345.60}
        ]

        with TestPipeline() as tp:
            test_input = tp | beam.Create(transactions)

            output = test_input | TotalAmountPerDate()

            assert_that(output, equal_to(expected))
