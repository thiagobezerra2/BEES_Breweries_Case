import unittest
from dags.aggdata import aggregate_data
from dags.extraction_api import fetch_data, raw_data  
from dags.transform_data import transform_data  
  
import os
import json

class TestBreweryETL(unittest.TestCase):

    def setUp(self):
        self.sample_data = [{
            "id": "bnaf-llc-austin",
            "name": "BNAF, LLC",
            "brewery_type": "planning",
            "street": "",
            "city": "Austin",
            "state": "Texas",
            "country": "United States",
            "longitude": None,
            "latitude": None
        }]

    def test_fetch_data(self):
        data = fetch_data()
        self.assertIsInstance(data, list)

    def test_raw_data(self):
        raw_data(self.sample_data)
        self.assertTrue(os.path.exists('data/bronze/brewery_data.json'))
        with open('data/bronze/brewery_data.json') as f:
            data = json.load(f)
            self.assertEqual(data, self.sample_data)

    def test_transform_data(self):
        raw_data(self.sample_data)
        transform_data()
        self.assertTrue(os.path.exists('data/silver/brewery_data_Texas.parquet'))

    def test_aggregate_data(self):
        raw_data(self.sample_data)
        transform_data()
        aggregate_data()
        self.assertTrue(os.path.exists('data/gold/brewery_aggregated_data.parquet'))

if __name__ == '__main__':
    unittest.main()