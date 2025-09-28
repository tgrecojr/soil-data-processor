import numpy as np

field_names = [
    "WBANNO",
    "UTC_DATE",
    "UTC_TIME",
    "LST_DATE",
    "LST_TIME",
    "CRX_VN",
    "LONGITUDE",
    "LATITUDE",
    "T_CALC",
    "T_HR_AVG",
    "T_MAX",
    "T_MIN",
    "P_CALC",
    "SOLARAD",
    "SOLARAD_FLAG",
    "SOLARAD_MAX",
    "SOLARAD_MAX_FLAG",
    "SOLARAD_MIN",
    "SOLARAD_MIN_FLAG",
    "SUR_TEMP_TYPE",
    "SUR_TEMP",
    "SUR_TEMP_FLAG",
    "SUR_TEMP_MAX",
    "SUR_TEMP_MAX_FLAG",
    "SUR_TEMP_MIN",
    "SUR_TEMP_MIN_FLAG",
    "RH_HR_AVG",
    "RH_HR_AVG_FLAG",
    "SOIL_MOISTURE_5",
    "SOIL_MOISTURE_10",
    "SOIL_MOISTURE_20",
    "SOIL_MOISTURE_50",
    "SOIL_MOISTURE_100",
    "SOIL_TEMP_5",
    "SOIL_TEMP_10",
    "SOIL_TEMP_20",
    "SOIL_TEMP_50",
    "SOIL_TEMP_100",
]

colspecs = [
    (1 - 1, 5),
    (7 - 1, 14),
    (16 - 1, 19),
    (21 - 1, 28),
    (30 - 1, 33),
    (35 - 1, 40),
    (42 - 1, 48),
    (50 - 1, 56),
    (58 - 1, 64),
    (66 - 1, 72),
    (74 - 1, 80),
    (82 - 1, 88),
    (90 - 1, 96),
    (98 - 1, 103),
    (105 - 1, 105),
    (107 - 1, 112),
    (114 - 1, 114),
    (116 - 1, 121),
    (123 - 1, 123),
    (125 - 1, 125),
    (127 - 1, 133),
    (135 - 1, 135),
    (137 - 1, 143),
    (145 - 1, 145),
    (147 - 1, 153),
    (155 - 1, 155),
    (157 - 1, 161),
    (163 - 1, 163),
    (165 - 1, 171),
    (173 - 1, 179),
    (181 - 1, 187),
    (189 - 1, 195),
    (197 - 1, 203),
    (205 - 1, 211),
    (213 - 1, 219),
    (221 - 1, 227),
    (229 - 1, 235),
    (237 - 1, 243),
]

col_types = {
    "WBANNO": object,
    "UTC_DATE": object,
    "UTC_TIME": object,
    "LST_DATE": object,
    "LST_TIME": object,
    "CRX_VN": object,
    "LONGITUDE": object,
    "LATITUDE": object,
    "T_CALC": object,  # Changed to object to handle parsing issues
    "T_HR_AVG": object,  # Changed to object to handle parsing issues
    "T_MAX": object,  # Changed to object to handle parsing issues
    "T_MIN": object,  # Changed to object to handle parsing issues
    "P_CALC": object,  # Changed to object to handle parsing issues
    "SOLARAD": object,
    "SOLARAD_FLAG": object,
    "SOLARAD_MAX": object,
    "SOLARAD_MAX_FLAG": object,
    "SOLARAD_MIN": object,
    "SOLARAD_MIN_FLAG": object,
    "SUR_TEMP_TYPE": object,
    "SUR_TEMP": object,
    "SUR_TEMP_FLAG": object,
    "SUR_TEMP_MAX": object,
    "SUR_TEMP_MAX_FLAG": object,
    "SUR_TEMP_MIN": object,
    "SUR_TEMP_MIN_FLAG": object,
    "RH_HR_AVG": object,  # Changed to object to handle parsing issues
    "RH_HR_AVG_FLAG": object,
    "SOIL_MOISTURE_5": object,  # Changed to object to handle parsing issues
    "SOIL_MOISTURE_10": object,  # Changed to object to handle parsing issues
    "SOIL_MOISTURE_20": object,  # Changed to object to handle parsing issues
    "SOIL_MOISTURE_50": object,  # Changed to object to handle parsing issues
    "SOIL_MOISTURE_100": object,  # Changed to object to handle parsing issues
    "SOIL_TEMP_5": object,  # Changed to object to handle parsing issues
    "SOIL_TEMP_10": object,  # Changed to object to handle parsing issues
    "SOIL_TEMP_20": object,  # Changed to object to handle parsing issues
    "SOIL_TEMP_50": object,  # Changed to object to handle parsing issues
    "SOIL_TEMP_100": object,  # Changed to object to handle parsing issues
}
