import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import processsoildata
import fieldmappings


class TestUtilityFunctions(unittest.TestCase):

    def test_formatdate_valid_input(self):
        date = "20220315"
        time = "1430"
        expected = datetime(2022, 3, 15, 14, 30)
        result = processsoildata.formatdate(date, time)
        self.assertEqual(result, expected)

    def test_formatdate_edge_cases(self):
        # Test midnight
        result = processsoildata.formatdate("20220101", "0000")
        expected = datetime(2022, 1, 1, 0, 0)
        self.assertEqual(result, expected)

        # Test end of day
        result = processsoildata.formatdate("20221231", "2359")
        expected = datetime(2022, 12, 31, 23, 59)
        self.assertEqual(result, expected)

    def test_removevaluesnotrecorded_invalid_values(self):
        # Test removal of invalid values
        self.assertIsNone(processsoildata.removevaluesnotrecorded(-99.000))
        self.assertIsNone(processsoildata.removevaluesnotrecorded(-9999.0))
        self.assertIsNone(processsoildata.removevaluesnotrecorded(-9999))

    def test_removevaluesnotrecorded_valid_values(self):
        # Test preservation of valid values
        self.assertEqual(processsoildata.removevaluesnotrecorded(25.5), 25.5)
        self.assertEqual(processsoildata.removevaluesnotrecorded(0), 0)
        self.assertEqual(processsoildata.removevaluesnotrecorded(-10.5), -10.5)

    def test_converttofarenheit_valid_celsius(self):
        # Test conversion from Celsius to Fahrenheit
        self.assertEqual(processsoildata.converttofarenheit(0), 32.0)
        self.assertEqual(processsoildata.converttofarenheit(100), 212.0)
        self.assertAlmostEqual(processsoildata.converttofarenheit(25), 77.0, places=1)
        self.assertAlmostEqual(processsoildata.converttofarenheit(-40), -40.0, places=1)

    def test_converttofarenheit_none_input(self):
        # Test handling of None input
        self.assertIsNone(processsoildata.converttofarenheit(None))


class TestEnvironmentVariables(unittest.TestCase):

    @patch.dict(
        os.environ,
        {
            "SOIL_DATA_LOCATION": "/test/path/*.txt",
            "SOIL_DATABASE": "testdb",
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
        },
    )
    def test_load_env_vars_success(self):
        var_names = [
            "SOIL_DATA_LOCATION",
            "SOIL_DATABASE",
            "SOIL_DATABASE_USER",
            "SOIL_DATABASE_PASSWORD",
            "SOIL_DATABASE_HOST",
        ]
        processsoildata.load_env_vars(var_names)

        self.assertEqual(
            processsoildata.global_vars["SOIL_DATA_LOCATION"], "/test/path/*.txt"
        )
        self.assertEqual(processsoildata.global_vars["SOIL_DATABASE"], "testdb")
        self.assertEqual(processsoildata.global_vars["SOIL_DATABASE_USER"], "testuser")
        self.assertEqual(
            processsoildata.global_vars["SOIL_DATABASE_PASSWORD"], "testpass"
        )
        self.assertEqual(processsoildata.global_vars["SOIL_DATABASE_HOST"], "localhost")

    @patch.dict(os.environ, {}, clear=True)
    @patch("sys.exit")
    def test_load_env_vars_missing_variable(self, mock_exit):
        var_names = ["MISSING_VAR"]
        processsoildata.load_env_vars(var_names)
        mock_exit.assert_called_once_with(1)


class TestDatabaseOperations(unittest.TestCase):

    @patch("psycopg2.connect")
    def test_create_table_if_not_exists_new_table(self, mock_connect):
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Set up global vars for database connection
        processsoildata.global_vars = {
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
            "SOIL_DATABASE": "testdb",
        }

        processsoildata.create_table_if_not_exists()

        # Verify table creation was attempted
        self.assertTrue(mock_cursor.execute.called)
        self.assertTrue(mock_conn.commit.called)

    @patch("psycopg2.connect")
    @patch("psycopg2.errors.DuplicateTable", Exception)
    def test_create_table_if_not_exists_existing_table(self, mock_connect):
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Simulate table already exists
        import psycopg2

        mock_cursor.execute.side_effect = [psycopg2.errors.DuplicateTable(), None]

        processsoildata.global_vars = {
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
            "SOIL_DATABASE": "testdb",
        }

        processsoildata.create_table_if_not_exists()

        # Verify rollback was called for duplicate table
        self.assertTrue(mock_conn.rollback.called)


class TestDataProcessing(unittest.TestCase):

    def setUp(self):
        self.sample_df = pd.DataFrame(
            {
                "UTC_DATE": ["20220315", "20220315"],
                "UTC_TIME": ["1430", "1445"],
                "LST_DATE": ["20220315", "20220315"],
                "LST_TIME": ["0930", "0945"],
                "SOIL_TEMP_5": [15.5, -99.000],
                "SOIL_TEMP_10": [16.2, 17.1],
                "SOIL_TEMP_20": [18.0, -9999.0],
                "SOIL_TEMP_50": [20.5, 21.3],
                "SOIL_TEMP_100": [22.1, -9999],
                "T_CALC": [25.0, 26.5],
                "T_HR_AVG": [24.5, 25.8],
                "P_CALC": [0.0, 2.5],
                "RH_HR_AVG": [65.2, 70.1],
            }
        )

    @patch("glob.glob")
    @patch("pandas.read_fwf")
    @patch("psycopg2.connect")
    def test_processdata_file_processing(self, mock_connect, mock_read_fwf, mock_glob):
        # Setup mocks
        mock_glob.return_value = ["/test/file1.txt"]
        mock_read_fwf.return_value = self.sample_df.copy()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        processsoildata.global_vars = {
            "SOIL_DATA_LOCATION": "/test/*.txt",
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
            "SOIL_DATABASE": "testdb",
        }

        processsoildata.processdata()

        # Verify file was processed
        mock_glob.assert_called_once_with("/test/*.txt")
        mock_read_fwf.assert_called_once()

        # Verify database operations - the code now uses bulk inserts
        self.assertTrue(mock_connect.called)

    def test_data_transformations(self):
        # Test the data transformation logic that would be applied in processdata
        df = self.sample_df.copy()

        # Apply transformations similar to processdata function
        df["UTC_DATETIME"] = df.apply(
            lambda row: processsoildata.formatdate(row["UTC_DATE"], row["UTC_TIME"]),
            axis=1,
        )
        df["LOCAL_DATETIME"] = df.apply(
            lambda row: processsoildata.formatdate(row["LST_DATE"], row["LST_TIME"]),
            axis=1,
        )

        # Test temperature conversions
        df["SOIL_TEMP_5"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["SOIL_TEMP_5"])
            ),
            axis=1,
        )
        df["SOIL_TEMP_20"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["SOIL_TEMP_20"])
            ),
            axis=1,
        )

        # Verify transformations
        self.assertAlmostEqual(df.iloc[0]["SOIL_TEMP_5"], 59.9, places=1)  # 15.5C to F
        # pandas converts None to NaN, so check for NaN instead
        import pandas as pd

        self.assertTrue(pd.isna(df.iloc[1]["SOIL_TEMP_5"]))  # -99.000 should become NaN
        self.assertTrue(
            pd.isna(df.iloc[1]["SOIL_TEMP_20"])
        )  # -9999.0 should become NaN

        # Verify datetime formatting
        expected_utc = datetime(2022, 3, 15, 14, 30)
        self.assertEqual(df.iloc[0]["UTC_DATETIME"], expected_utc)

        expected_local = datetime(2022, 3, 15, 9, 30)
        self.assertEqual(df.iloc[0]["LOCAL_DATETIME"], expected_local)


class TestFieldMappings(unittest.TestCase):

    def test_field_names_length(self):
        # Verify field names match column specifications
        self.assertEqual(len(fieldmappings.field_names), len(fieldmappings.colspecs))
        self.assertEqual(len(fieldmappings.field_names), len(fieldmappings.col_types))

    def test_required_fields_present(self):
        # Test that required fields are present
        required_fields = [
            "UTC_DATE",
            "UTC_TIME",
            "LST_DATE",
            "LST_TIME",
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

        for field in required_fields:
            self.assertIn(field, fieldmappings.field_names)
            self.assertIn(field, fieldmappings.col_types)

    def test_colspecs_format(self):
        # Verify column specifications are tuples with two integers
        for colspec in fieldmappings.colspecs:
            self.assertIsInstance(colspec, tuple)
            self.assertEqual(len(colspec), 2)
            self.assertIsInstance(colspec[0], int)
            self.assertIsInstance(colspec[1], int)
            self.assertLess(colspec[0], colspec[1])  # Start should be less than end


if __name__ == "__main__":
    unittest.main()
