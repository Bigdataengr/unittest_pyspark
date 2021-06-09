from main import extract_age_func
from tests.utils.dataframe_test_utils import PySparkTestCase, test_schema, test_data


class SimpleTestCase(PySparkTestCase):

    def test_dataparser_schema(self):
        input_df = self.spark.createDataFrame(
            data=[['Jan', 'Janson', 'jj@email.com', '20-504123'],
                  ['Jen', 'Jenny', 'jen@email.com', '55-357378'],
                  ['Bill', 'Bill', 'bill@email.com', '79-357378']],
            schema=['first_name', 'last_name', 'email', 'id'])

        transformed_df = extract_age_func(input_df, "id")

        expected_df = self.spark.createDataFrame(
            data=[['Jan', 'Janson', 'jj@email.com', '20-504123', '20'],
                  ['Jen', 'Jenny', 'jen@email.com', '55-357378', '55'],
                  ['Bill', 'Bill', 'bill@email.com', '79-357378', '79']],
            schema=['first_name', 'last_name', 'email', 'id', 'age'])

        self.assertTrue(test_schema(transformed_df, expected_df))

    def test_dataparser_data(self):
        input_df = self.spark.createDataFrame(
            data=[['Jan', 'Janson', 'jj@email.com', '20-504123'],
                  ['Jen', 'Jenny', 'jen@email.com', '55-357378'],
                  ['Bill', 'Bill', 'bill@email.com', '79-357378']],
            schema=['first_name', 'last_name', 'email', 'id'])

        transformed_df = extract_age_func(input_df, "id")

        expected_df = self.spark.createDataFrame(
            data=[['Jan', 'Janson', 'jj@email.com', '20-504123', '20'],
                  ['Jen', 'Jenny', 'jen@email.com', '55-357378', '55'],
                  ['Bill', 'Bill', 'bill@email.com', '79-357378', '79']],
            schema=['first_name', 'last_name', 'email', 'id', 'age'])

        self.assertTrue(test_data(transformed_df, expected_df))
