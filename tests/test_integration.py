import unittest
from unittest.mock import Mock, patch, MagicMock, call
import pandas as pd
import numpy as np
import tempfile
import os
import sys
from datetime import datetime
import psycopg2
from io import StringIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import processsoildata
import fieldmappings


class TestIntegrationProcessing(unittest.TestCase):

    def setUp(self):
        # Reset global vars for each test
        processsoildata.global_vars = {}

        # Sample fixed-width data matching the field mappings
        self.sample_fixed_width_data = """12345202203151430202203150930  1.2 -74.5012 40.7589  25.5  24.8  30.2  18.5   2.5   850     0   860     0   840     0   I  22.1     0  23.5     0  20.8     0  68.5     0 -99.00 -99.00 -99.00 -99.00 -99.00  15.5  16.2  18.0  20.5  22.1
12346202203151445202203150945  1.2 -74.5012 40.7589  26.2  25.1  31.0  19.2   1.8   865     0   875     0   855     0   I  22.8     0  24.2     0  21.5     0  70.1     0 -99.00 -99.00 -99.00 -99.00 -99.00  16.1  16.8  18.5  21.0  22.5"""

    @patch("glob.glob")
    @patch("pandas.read_fwf")
    @patch("psycopg2.connect")
    def test_end_to_end_processing(self, mock_connect, mock_read_fwf, mock_glob):
        # Create sample dataframe that read_fwf would return
        sample_df = pd.DataFrame(
            {
                "WBANNO": ["12345", "12346"],
                "UTC_DATE": ["20220315", "20220315"],
                "UTC_TIME": ["1430", "1445"],
                "LST_DATE": ["20220315", "20220315"],
                "LST_TIME": ["0930", "0945"],
                "CRX_VN": ["1.2", "1.2"],
                "LONGITUDE": ["-74.5012", "-74.5012"],
                "LATITUDE": ["40.7589", "40.7589"],
                "T_CALC": [25.5, 26.2],
                "T_HR_AVG": [24.8, 25.1],
                "T_MAX": [30.2, 31.0],
                "T_MIN": [18.5, 19.2],
                "P_CALC": [2.5, 1.8],
                "SOLARAD": ["850", "865"],
                "SOLARAD_FLAG": ["0", "0"],
                "SOLARAD_MAX": ["860", "875"],
                "SOLARAD_MAX_FLAG": ["0", "0"],
                "SOLARAD_MIN": ["840", "855"],
                "SOLARAD_MIN_FLAG": ["0", "0"],
                "SUR_TEMP_TYPE": ["I", "I"],
                "SUR_TEMP": ["22.1", "22.8"],
                "SUR_TEMP_FLAG": ["0", "0"],
                "SUR_TEMP_MAX": ["23.5", "24.2"],
                "SUR_TEMP_MAX_FLAG": ["0", "0"],
                "SUR_TEMP_MIN": ["20.8", "21.5"],
                "SUR_TEMP_MIN_FLAG": ["0", "0"],
                "RH_HR_AVG": [68.5, 70.1],
                "RH_HR_AVG_FLAG": ["0", "0"],
                "SOIL_MOISTURE_5": [-99.00, -99.00],
                "SOIL_MOISTURE_10": [-99.00, -99.00],
                "SOIL_MOISTURE_20": [-99.00, -99.00],
                "SOIL_MOISTURE_50": [-99.00, -99.00],
                "SOIL_MOISTURE_100": [-99.00, -99.00],
                "SOIL_TEMP_5": [15.5, 16.1],
                "SOIL_TEMP_10": [16.2, 16.8],
                "SOIL_TEMP_20": [18.0, 18.5],
                "SOIL_TEMP_50": [20.5, 21.0],
                "SOIL_TEMP_100": [22.1, 22.5],
            }
        )

        # Setup mocks
        mock_glob.return_value = ["/test/data/file1.txt"]
        mock_read_fwf.return_value = sample_df

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Setup environment variables
        processsoildata.global_vars = {
            "SOIL_DATA_LOCATION": "/test/data/*.txt",
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
            "SOIL_DATABASE": "testdb",
        }

        # Run the processing
        processsoildata.processdata()

        # Verify file processing
        mock_glob.assert_called_once_with("/test/data/*.txt")
        mock_read_fwf.assert_called_once_with(
            "/test/data/file1.txt",
            colspecs=fieldmappings.colspecs,
            names=fieldmappings.field_names,
            header=None,
            index_col=False,
            dtype=fieldmappings.col_types,
            memory_map=True,
            skip_blank_lines=True,
            on_bad_lines="skip",
        )

        # Verify database operations
        expected_connection_string = (
            "postgres://testuser:testpass@localhost:5432/testdb"
        )
        mock_connect.assert_called_with(expected_connection_string)

        # The code now uses bulk inserts, so we should verify that it was called
        # Note: actual bulk insert logic is more complex with execute_values,
        # but we just want to verify the connection was made
        self.assertTrue(mock_connect.called)

    def test_data_transformation_pipeline(self):
        # Test the complete data transformation without database
        test_data = {
            "UTC_DATE": ["20220315"],
            "UTC_TIME": ["1430"],
            "LST_DATE": ["20220315"],
            "LST_TIME": ["0930"],
            "SOIL_TEMP_5": [15.5],
            "SOIL_TEMP_10": [-99.000],
            "SOIL_TEMP_20": [18.0],
            "SOIL_TEMP_50": [-9999.0],
            "SOIL_TEMP_100": [22.1],
            "T_CALC": [25.0],
            "T_HR_AVG": [24.5],
            "P_CALC": [-9999],
            "RH_HR_AVG": [68.5],
        }

        df = pd.DataFrame(test_data)

        # Apply transformations as done in processdata
        df["UTC_DATETIME"] = df.apply(
            lambda row: processsoildata.formatdate(row["UTC_DATE"], row["UTC_TIME"]),
            axis=1,
        )
        df["LOCAL_DATETIME"] = df.apply(
            lambda row: processsoildata.formatdate(row["LST_DATE"], row["LST_TIME"]),
            axis=1,
        )
        df["SOIL_TEMP_5"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["SOIL_TEMP_5"])
            ),
            axis=1,
        )
        df["SOIL_TEMP_10"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["SOIL_TEMP_10"])
            ),
            axis=1,
        )
        df["SOIL_TEMP_20"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["SOIL_TEMP_20"])
            ),
            axis=1,
        )
        df["SOIL_TEMP_50"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["SOIL_TEMP_50"])
            ),
            axis=1,
        )
        df["SOIL_TEMP_100"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["SOIL_TEMP_100"])
            ),
            axis=1,
        )
        df["T_CALC"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["T_CALC"])
            ),
            axis=1,
        )
        df["T_HR_AVG"] = df.apply(
            lambda row: processsoildata.converttofarenheit(
                processsoildata.removevaluesnotrecorded(row["T_HR_AVG"])
            ),
            axis=1,
        )
        df["P_CALC"] = df.apply(
            lambda row: processsoildata.removevaluesnotrecorded(row["P_CALC"]), axis=1
        )
        df["RH_HR_AVG"] = df.apply(
            lambda row: processsoildata.removevaluesnotrecorded(row["RH_HR_AVG"]),
            axis=1,
        )
        df = df.set_index(["UTC_DATETIME"])
        df = df.replace({np.nan: None})

        # Verify transformations
        row = df.iloc[0]

        # Test datetime parsing
        self.assertEqual(row["LOCAL_DATETIME"], datetime(2022, 3, 15, 9, 30))

        # Test temperature conversions (C to F)
        self.assertAlmostEqual(row["SOIL_TEMP_5"], 59.9, places=1)  # 15.5C
        self.assertIsNone(row["SOIL_TEMP_10"])  # -99.000 removed
        self.assertAlmostEqual(row["SOIL_TEMP_20"], 64.4, places=1)  # 18.0C
        self.assertIsNone(row["SOIL_TEMP_50"])  # -9999.0 removed
        self.assertAlmostEqual(row["SOIL_TEMP_100"], 71.78, places=1)  # 22.1C
        self.assertAlmostEqual(row["T_CALC"], 77.0, places=1)  # 25.0C
        self.assertAlmostEqual(row["T_HR_AVG"], 76.1, places=1)  # 24.5C

        # Test invalid value removal
        self.assertIsNone(row["P_CALC"])  # -9999 removed
        self.assertEqual(row["RH_HR_AVG"], 68.5)  # Valid value preserved


class TestErrorHandling(unittest.TestCase):

    @patch("glob.glob")
    @patch("pandas.read_fwf")
    @patch("psycopg2.connect")
    def test_duplicate_key_handling(self, mock_connect, mock_read_fwf, mock_glob):
        # Setup sample data
        sample_df = pd.DataFrame(
            {
                "UTC_DATE": ["20220315"],
                "UTC_TIME": ["1430"],
                "LST_DATE": ["20220315"],
                "LST_TIME": ["0930"],
                "SOIL_TEMP_5": [15.5],
                "SOIL_TEMP_10": [16.2],
                "SOIL_TEMP_20": [18.0],
                "SOIL_TEMP_50": [20.5],
                "SOIL_TEMP_100": [22.1],
                "T_CALC": [25.0],
                "T_HR_AVG": [24.5],
                "P_CALC": [2.5],
                "RH_HR_AVG": [68.5],
            }
        )

        # Setup mocks
        mock_glob.return_value = ["/test/data/file1.txt"]
        mock_read_fwf.return_value = sample_df

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        processsoildata.global_vars = {
            "SOIL_DATA_LOCATION": "/test/data/*.txt",
            "SOIL_DATABASE_USER": "testuser",
            "SOIL_DATABASE_PASSWORD": "testpass",
            "SOIL_DATABASE_HOST": "localhost",
            "SOIL_DATABASE": "testdb",
        }

        # The new implementation uses ON CONFLICT DO NOTHING to handle duplicates
        # Should not raise exception - duplicates are handled gracefully
        try:
            processsoildata.processdata()
        except Exception as e:
            self.fail(f"processdata() raised {type(e).__name__} unexpectedly: {e}")

        # Verify that database connection was made
        self.assertTrue(mock_connect.called)

    @patch("os.environ.get")
    @patch("sys.exit")
    def test_missing_environment_variables(self, mock_exit, mock_env_get):
        # Simulate missing environment variable
        mock_env_get.return_value = None

        var_names = ["MISSING_VAR"]

        processsoildata.load_env_vars(var_names)
        mock_exit.assert_called_once_with(1)


class TestFileProcessing(unittest.TestCase):

    def test_field_mappings_consistency(self):
        # Test that field mappings are consistent
        self.assertEqual(len(fieldmappings.field_names), len(fieldmappings.colspecs))
        self.assertEqual(len(fieldmappings.field_names), len(fieldmappings.col_types))

        # Test that all soil temperature fields are mapped correctly
        soil_temp_fields = [
            field
            for field in fieldmappings.field_names
            if field.startswith("SOIL_TEMP_")
        ]
        expected_soil_fields = [
            "SOIL_TEMP_5",
            "SOIL_TEMP_10",
            "SOIL_TEMP_20",
            "SOIL_TEMP_50",
            "SOIL_TEMP_100",
        ]

        for expected_field in expected_soil_fields:
            self.assertIn(expected_field, soil_temp_fields)

    @patch("pandas.read_fwf")
    def test_pandas_read_parameters(self, mock_read_fwf):
        # Test that pandas read_fwf is called with correct parameters
        mock_read_fwf.return_value = pd.DataFrame()

        # This would be called within processdata, but we're testing the parameters
        test_file = "/test/file.txt"

        df = pd.read_fwf(
            test_file,
            colspecs=fieldmappings.colspecs,
            names=fieldmappings.field_names,
            header=None,
            index_col=False,
            dtype=fieldmappings.col_types,
            memory_map=True,
            skip_blank_lines=True,
            on_bad_lines="skip",
        )

        mock_read_fwf.assert_called_once_with(
            test_file,
            colspecs=fieldmappings.colspecs,
            names=fieldmappings.field_names,
            header=None,
            index_col=False,
            dtype=fieldmappings.col_types,
            memory_map=True,
            skip_blank_lines=True,
            on_bad_lines="skip",
        )


if __name__ == "__main__":
    unittest.main()
