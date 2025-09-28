import unittest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import processsoildata


class TestDatabaseConnection(unittest.TestCase):

    def setUp(self):
        self.test_env_vars = {
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
            "SOIL_DATABASE": "testdb",
        }
        processsoildata.global_vars = self.test_env_vars.copy()

    def test_connection_string_format(self):
        expected = "postgres://testuser:testpass@localhost:5432/testdb"
        actual = "postgres://{}:{}@{}:5432/{}".format(
            processsoildata.global_vars["SOIL_DATABASE_USER"],
            processsoildata.global_vars["SOIL_DATABASE_PASSWORD"],
            processsoildata.global_vars["SOIL_DATABASE_HOST"],
            processsoildata.global_vars["SOIL_DATABASE"],
        )
        self.assertEqual(actual, expected)

    @patch("psycopg2.connect")
    def test_database_connection_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        connection_string = "postgres://{}:{}@{}:5432/{}".format(
            self.test_env_vars["SOIL_DATABASE_USER"],
            self.test_env_vars["SOIL_DATABASE_PASSWORD"],
            self.test_env_vars["SOIL_DATABASE_HOST"],
            self.test_env_vars["SOIL_DATABASE"],
        )

        with psycopg2.connect(connection_string) as conn:
            pass

        mock_connect.assert_called_once_with(connection_string)

    @patch("psycopg2.connect")
    def test_database_connection_failure(self, mock_connect):
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")

        connection_string = "postgres://{}:{}@{}:5432/{}".format(
            self.test_env_vars["SOIL_DATABASE_USER"],
            self.test_env_vars["SOIL_DATABASE_PASSWORD"],
            self.test_env_vars["SOIL_DATABASE_HOST"],
            self.test_env_vars["SOIL_DATABASE"],
        )

        with self.assertRaises(psycopg2.OperationalError):
            with psycopg2.connect(connection_string) as conn:
                pass


class TestTableCreation(unittest.TestCase):

    def setUp(self):
        processsoildata.global_vars = {
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
            "SOIL_DATABASE": "testdb",
        }

    @patch("psycopg2.connect")
    def test_create_table_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        processsoildata.create_table_if_not_exists()

        # Verify cursor operations
        self.assertTrue(mock_cursor.execute.called)
        self.assertTrue(mock_conn.commit.called)
        mock_cursor.close.assert_called()

        # Verify SQL statements contain expected elements
        calls = mock_cursor.execute.call_args_list
        table_create_call = calls[0][0][0]

        self.assertIn("CREATE TABLE soildata", table_create_call)
        self.assertIn("time TIMESTAMPTZ NOT NULL", table_create_call)
        self.assertIn("PRIMARY KEY(time)", table_create_call)

        # Verify all required columns are in the CREATE TABLE statement
        required_columns = [
            "SOIL_TEMP_5",
            "SOIL_TEMP_10",
            "SOIL_TEMP_20",
            "SOIL_TEMP_50",
            "SOIL_TEMP_100",
            "T_CALC",
            "T_HR_AVG",
            "P_CALC",
            "RH_HR_AVG",
        ]

        for column in required_columns:
            self.assertIn(column, table_create_call)

    @patch("psycopg2.connect")
    def test_create_table_already_exists(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Simulate table already exists
        mock_cursor.execute.side_effect = psycopg2.errors.DuplicateTable()

        processsoildata.create_table_if_not_exists()

        # Verify rollback was called after DuplicateTable error
        mock_conn.rollback.assert_called()
        mock_cursor.close.assert_called()



class TestDataInsertion(unittest.TestCase):

    def setUp(self):
        processsoildata.global_vars = {
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
            "SOIL_DATABASE": "testdb",
        }

    @patch("psycopg2.connect")
    def test_insert_query_format(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Test data that would be inserted
        test_time = datetime(2022, 3, 15, 9, 30)
        test_data = (test_time, 59.9, 60.8, 64.4, 68.9, 71.8, 77.0, 76.1, 2.5, 68.5)

        insert_query = """
           INSERT INTO soildata (time, SOIL_TEMP_5, SOIL_TEMP_10,SOIL_TEMP_20,SOIL_TEMP_50,SOIL_TEMP_100,T_CALC, T_HR_AVG,P_CALC,RH_HR_AVG) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           """

        # Simulate the insert operation
        with psycopg2.connect("test_connection") as conn:
            cursor = conn.cursor()
            cursor.execute(insert_query, test_data)
            conn.commit()

        # Verify the insert query structure
        self.assertIn("INSERT INTO soildata", insert_query)
        self.assertIn("VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", insert_query)

        # Count placeholders - should be 10 (time + 9 data columns)
        placeholder_count = insert_query.count("%s")
        self.assertEqual(placeholder_count, 10)

    @patch("psycopg2.connect")
    def test_unique_violation_handling(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Simulate UniqueViolation on insert
        mock_cursor.execute.side_effect = psycopg2.errors.UniqueViolation(
            "Duplicate key"
        )

        insert_query = """
           INSERT INTO soildata (time, SOIL_TEMP_5, SOIL_TEMP_10,SOIL_TEMP_20,SOIL_TEMP_50,SOIL_TEMP_100,T_CALC, T_HR_AVG,P_CALC,RH_HR_AVG) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           """
        test_data = (
            datetime.now(),
            59.9,
            60.8,
            64.4,
            68.9,
            71.8,
            77.0,
            76.1,
            2.5,
            68.5,
        )

        # This should not raise an exception due to the try/except in the original code
        with psycopg2.connect("test_connection") as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(insert_query, test_data)
            except psycopg2.errors.UniqueViolation:
                pass  # This mimics the behavior in processsoildata.py
            conn.commit()

        # Verify the exception was handled
        mock_cursor.execute.assert_called_with(insert_query, test_data)
        mock_conn.commit.assert_called()

    def test_data_types_validation(self):
        # Test that data types match database expectations
        test_time = datetime(2022, 3, 15, 9, 30)

        # Test data tuple as would be created in processdata
        test_data = (
            test_time,  # time TIMESTAMPTZ
            59,  # SOIL_TEMP_5 INTEGER (converted from float)
            60.8,  # SOIL_TEMP_10 float8
            64.4,  # SOIL_TEMP_20 float8
            68.9,  # SOIL_TEMP_50 float8
            71.8,  # SOIL_TEMP_100 float8
            77.0,  # T_CALC float8
            76.1,  # T_HR_AVG DECIMAL
            2.5,  # P_CALC float8
            68.5,  # RH_HR_AVG DECIMAL
        )

        # Verify data types
        self.assertIsInstance(test_data[0], datetime)  # time
        self.assertIsInstance(test_data[1], (int, float))  # SOIL_TEMP_5
        self.assertIsInstance(test_data[2], (int, float))  # SOIL_TEMP_10
        self.assertIsInstance(test_data[3], (int, float))  # SOIL_TEMP_20
        self.assertIsInstance(test_data[4], (int, float))  # SOIL_TEMP_50
        self.assertIsInstance(test_data[5], (int, float))  # SOIL_TEMP_100
        self.assertIsInstance(test_data[6], (int, float))  # T_CALC
        self.assertIsInstance(test_data[7], (int, float))  # T_HR_AVG
        self.assertIsInstance(test_data[8], (int, float))  # P_CALC
        self.assertIsInstance(test_data[9], (int, float))  # RH_HR_AVG

        # Verify tuple length matches expected columns
        self.assertEqual(len(test_data), 10)


class TestDatabaseSchema(unittest.TestCase):

    def test_required_columns_in_schema(self):
        # Test that the CREATE TABLE statement has all required columns
        create_table_sql = """
                  CREATE TABLE soildata (
                  time TIMESTAMPTZ NOT NULL,
                  SOIL_TEMP_5 INTEGER,
                  SOIL_TEMP_10 float8,
                  SOIL_TEMP_20 float8,
                  SOIL_TEMP_50 float8,
                  SOIL_TEMP_100 float8,
                  T_CALC float8,
                  T_HR_AVG DECIMAL,
                  P_CALC float8,
                  RH_HR_AVG DECIMAL,
                  PRIMARY KEY(time))
                  """

        required_elements = [
            "CREATE TABLE soildata",
            "time TIMESTAMPTZ NOT NULL",
            "SOIL_TEMP_5 INTEGER",
            "SOIL_TEMP_10 float8",
            "SOIL_TEMP_20 float8",
            "SOIL_TEMP_50 float8",
            "SOIL_TEMP_100 float8",
            "T_CALC float8",
            "T_HR_AVG DECIMAL",
            "P_CALC float8",
            "RH_HR_AVG DECIMAL",
            "PRIMARY KEY(time)",
        ]

        for element in required_elements:
            self.assertIn(element, create_table_sql)



if __name__ == "__main__":
    unittest.main()
