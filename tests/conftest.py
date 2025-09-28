import pytest
import os
import pandas as pd
from datetime import datetime
import sys

# Add parent directory to path for importing modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fieldmappings


@pytest.fixture
def sample_soil_dataframe():
    """Create a sample DataFrame with realistic soil data for testing."""
    return pd.DataFrame(
        {
            "WBANNO": ["12345", "12346", "12347"],
            "UTC_DATE": ["20220315", "20220315", "20220315"],
            "UTC_TIME": ["1430", "1445", "1500"],
            "LST_DATE": ["20220315", "20220315", "20220315"],
            "LST_TIME": ["0930", "0945", "1000"],
            "CRX_VN": ["1.2", "1.2", "1.2"],
            "LONGITUDE": ["-74.5012", "-74.5012", "-74.5012"],
            "LATITUDE": ["40.7589", "40.7589", "40.7589"],
            "T_CALC": [25.5, 26.2, -9999],
            "T_HR_AVG": [24.8, 25.1, -9999],
            "T_MAX": [30.2, 31.0, -9999],
            "T_MIN": [18.5, 19.2, -9999],
            "P_CALC": [2.5, 1.8, -9999],
            "SOLARAD": ["850", "865", "-999"],
            "SOLARAD_FLAG": ["0", "0", "3"],
            "SOLARAD_MAX": ["860", "875", "-999"],
            "SOLARAD_MAX_FLAG": ["0", "0", "3"],
            "SOLARAD_MIN": ["840", "855", "-999"],
            "SOLARAD_MIN_FLAG": ["0", "0", "3"],
            "SUR_TEMP_TYPE": ["I", "I", "I"],
            "SUR_TEMP": ["22.1", "22.8", "-999.0"],
            "SUR_TEMP_FLAG": ["0", "0", "3"],
            "SUR_TEMP_MAX": ["23.5", "24.2", "-999.0"],
            "SUR_TEMP_MAX_FLAG": ["0", "0", "3"],
            "SUR_TEMP_MIN": ["20.8", "21.5", "-999.0"],
            "SUR_TEMP_MIN_FLAG": ["0", "0", "3"],
            "RH_HR_AVG": [68.5, 70.1, -99.0],
            "RH_HR_AVG_FLAG": ["0", "0", "3"],
            "SOIL_MOISTURE_5": [-99.00, -99.00, -99.00],
            "SOIL_MOISTURE_10": [-99.00, -99.00, -99.00],
            "SOIL_MOISTURE_20": [-99.00, -99.00, -99.00],
            "SOIL_MOISTURE_50": [-99.00, -99.00, -99.00],
            "SOIL_MOISTURE_100": [-99.00, -99.00, -99.00],
            "SOIL_TEMP_5": [15.5, 16.1, -99.00],
            "SOIL_TEMP_10": [16.2, 16.8, -99.00],
            "SOIL_TEMP_20": [18.0, 18.5, -99.00],
            "SOIL_TEMP_50": [20.5, 21.0, -99.00],
            "SOIL_TEMP_100": [22.1, 22.5, -99.00],
        }
    )


@pytest.fixture
def sample_environment_vars():
    """Provide sample environment variables for testing."""
    return {
        "SOIL_DATA_LOCATION": "/test/data/*.txt",
        "SOIL_DATABASE": "test_soildb",
        "SOIL_DATABASE_USER": "test_user",
        "SOIL_DATABASE_PASSWORD": "test_password",
        "SOIL_DATABASE_HOST": "test_host",
    }


@pytest.fixture
def sample_processed_data():
    """Provide sample processed data that would result from transformations."""
    return [
        (
            datetime(2022, 3, 15, 9, 30),
            59.9,
            61.16,
            64.4,
            68.9,
            71.78,
            77.9,
            76.64,
            2.5,
            68.5,
        ),
        (
            datetime(2022, 3, 15, 9, 45),
            60.98,
            62.24,
            65.3,
            69.8,
            72.5,
            79.16,
            77.18,
            1.8,
            70.1,
        ),
        (
            datetime(2022, 3, 15, 10, 0),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    ]


@pytest.fixture
def sample_fixed_width_file(tmp_path):
    """Create a temporary fixed-width format file for testing."""
    content = """12345202203151430202203150930  1.2 -74.5012 40.7589  25.5  24.8  30.2  18.5   2.5   850     0   860     0   840     0   I  22.1     0  23.5     0  20.8     0  68.5     0 -99.00 -99.00 -99.00 -99.00 -99.00  15.5  16.2  18.0  20.5  22.1
12346202203151445202203150945  1.2 -74.5012 40.7589  26.2  25.1  31.0  19.2   1.8   865     0   875     0   855     0   I  22.8     0  24.2     0  21.5     0  70.1     0 -99.00 -99.00 -99.00 -99.00 -99.00  16.1  16.8  18.5  21.0  22.5"""

    test_file = tmp_path / "test_soil_data.txt"
    test_file.write_text(content)
    return str(test_file)


@pytest.fixture
def invalid_values_test_cases():
    """Provide test cases for invalid value handling."""
    return [
        (-99.000, None, "Standard invalid value"),
        (-9999.0, None, "Large invalid value float"),
        (-9999, None, "Large invalid value integer"),
        (0, 0, "Valid zero"),
        (25.5, 25.5, "Valid positive"),
        (-10.5, -10.5, "Valid negative"),
        (None, None, "None input"),
    ]


@pytest.fixture
def temperature_conversion_test_cases():
    """Provide test cases for temperature conversion (Celsius to Fahrenheit)."""
    return [
        (0, 32.0, "Freezing point"),
        (100, 212.0, "Boiling point"),
        (25, 77.0, "Room temperature"),
        (-40, -40.0, "Equal C and F point"),
        (15.5, 59.9, "Realistic soil temperature"),
        (None, None, "None input"),
    ]


@pytest.fixture
def datetime_test_cases():
    """Provide test cases for datetime formatting."""
    return [
        ("20220315", "1430", datetime(2022, 3, 15, 14, 30)),
        ("20220101", "0000", datetime(2022, 1, 1, 0, 0)),
        ("20221231", "2359", datetime(2022, 12, 31, 23, 59)),
        ("20220630", "1200", datetime(2022, 6, 30, 12, 0)),
    ]
