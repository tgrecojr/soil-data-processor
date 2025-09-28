import unittest
import sys
import os
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import fieldmappings


class TestFieldMappings(unittest.TestCase):

    def test_field_names_count(self):
        """Test that field_names contains the expected number of fields."""
        expected_count = 38  # Based on the actual fieldmappings.py
        actual_count = len(fieldmappings.field_names)
        self.assertEqual(
            actual_count,
            expected_count,
            f"Expected {expected_count} fields, got {actual_count}",
        )

    def test_colspecs_count(self):
        """Test that colspecs contains the expected number of column specifications."""
        expected_count = 38
        actual_count = len(fieldmappings.colspecs)
        self.assertEqual(
            actual_count,
            expected_count,
            f"Expected {expected_count} column specs, got {actual_count}",
        )

    def test_col_types_count(self):
        """Test that col_types contains the expected number of type definitions."""
        expected_count = 38
        actual_count = len(fieldmappings.col_types)
        self.assertEqual(
            actual_count,
            expected_count,
            f"Expected {expected_count} column types, got {actual_count}",
        )

    def test_consistency_between_mappings(self):
        """Test that field_names, colspecs, and col_types have matching lengths."""
        field_count = len(fieldmappings.field_names)
        colspec_count = len(fieldmappings.colspecs)
        col_type_count = len(fieldmappings.col_types)

        self.assertEqual(
            field_count,
            colspec_count,
            "field_names and colspecs should have same length",
        )
        self.assertEqual(
            field_count,
            col_type_count,
            "field_names and col_types should have same length",
        )
        self.assertEqual(
            colspec_count,
            col_type_count,
            "colspecs and col_types should have same length",
        )

    def test_required_soil_temperature_fields(self):
        """Test that all required soil temperature fields are present."""
        required_soil_fields = [
            "SOIL_TEMP_5",
            "SOIL_TEMP_10",
            "SOIL_TEMP_20",
            "SOIL_TEMP_50",
            "SOIL_TEMP_100",
        ]

        for field in required_soil_fields:
            self.assertIn(
                field,
                fieldmappings.field_names,
                f"Required field {field} not found in field_names",
            )
            self.assertIn(
                field,
                fieldmappings.col_types,
                f"Required field {field} not found in col_types",
            )

    def test_required_datetime_fields(self):
        """Test that all required datetime fields are present."""
        required_datetime_fields = ["UTC_DATE", "UTC_TIME", "LST_DATE", "LST_TIME"]

        for field in required_datetime_fields:
            self.assertIn(
                field,
                fieldmappings.field_names,
                f"Required datetime field {field} not found",
            )
            # These should be object type for string handling
            self.assertEqual(
                fieldmappings.col_types[field],
                object,
                f"Datetime field {field} should be object type",
            )

    def test_required_environmental_fields(self):
        """Test that all required environmental measurement fields are present."""
        required_env_fields = ["T_CALC", "T_HR_AVG", "P_CALC", "RH_HR_AVG"]

        for field in required_env_fields:
            self.assertIn(
                field,
                fieldmappings.field_names,
                f"Required environmental field {field} not found",
            )
            self.assertIn(
                field,
                fieldmappings.col_types,
                f"Required environmental field {field} not found in col_types",
            )

    def test_colspec_tuple_format(self):
        """Test that all column specifications are properly formatted tuples."""
        for i, colspec in enumerate(fieldmappings.colspecs):
            with self.subTest(i=i, colspec=colspec):
                self.assertIsInstance(
                    colspec, tuple, f"Column spec {i} should be a tuple"
                )
                self.assertEqual(
                    len(colspec), 2, f"Column spec {i} should have exactly 2 elements"
                )
                self.assertIsInstance(
                    colspec[0], int, f"Column spec {i} start should be integer"
                )
                self.assertIsInstance(
                    colspec[1], int, f"Column spec {i} end should be integer"
                )
                self.assertLess(
                    colspec[0],
                    colspec[1],
                    f"Column spec {i} start should be less than end",
                )

    def test_colspec_ordering(self):
        """Test that column specifications are in ascending order (no overlaps)."""
        for i in range(len(fieldmappings.colspecs) - 1):
            current_end = fieldmappings.colspecs[i][1]
            next_start = fieldmappings.colspecs[i + 1][0]
            self.assertLessEqual(
                current_end,
                next_start,
                f"Column specs {i} and {i+1} overlap or are not ordered",
            )

    def test_soil_temperature_data_types(self):
        """Test that soil temperature fields have appropriate data types."""
        soil_temp_fields = [
            f for f in fieldmappings.field_names if f.startswith("SOIL_TEMP_")
        ]

        for field in soil_temp_fields:
            data_type = fieldmappings.col_types[field]
            self.assertEqual(
                data_type,
                object,
                f"Soil temperature field {field} should be object type",
            )

    def test_environmental_data_types(self):
        """Test that environmental measurement fields have appropriate data types."""
        env_numeric_fields = ["T_CALC", "T_HR_AVG", "P_CALC", "RH_HR_AVG"]

        for field in env_numeric_fields:
            data_type = fieldmappings.col_types[field]
            self.assertEqual(
                data_type,
                object,
                f"Environmental field {field} should be object type",
            )

    def test_identification_fields(self):
        """Test that identification and metadata fields are present."""
        id_fields = ["WBANNO", "LONGITUDE", "LATITUDE", "CRX_VN"]

        for field in id_fields:
            self.assertIn(
                field,
                fieldmappings.field_names,
                f"Identification field {field} not found",
            )

    def test_no_duplicate_field_names(self):
        """Test that there are no duplicate field names."""
        field_names = fieldmappings.field_names
        unique_field_names = set(field_names)

        self.assertEqual(
            len(field_names),
            len(unique_field_names),
            "There should be no duplicate field names",
        )

    def test_field_name_format(self):
        """Test that field names follow expected naming conventions."""
        for field_name in fieldmappings.field_names:
            with self.subTest(field_name=field_name):
                self.assertIsInstance(
                    field_name, str, f"Field name {field_name} should be a string"
                )
                self.assertTrue(
                    field_name.isupper(), f"Field name {field_name} should be uppercase"
                )
                # Should not contain spaces or special characters except underscore
                allowed_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
                field_chars = set(field_name)
                invalid_chars = field_chars - allowed_chars
                self.assertEqual(
                    len(invalid_chars),
                    0,
                    f"Field name {field_name} contains invalid characters: {invalid_chars}",
                )

    def test_moisture_fields_present(self):
        """Test that soil moisture fields are present (even if not used in processing)."""
        moisture_fields = [f for f in fieldmappings.field_names if "MOISTURE" in f]
        expected_moisture_depths = ["5", "10", "20", "50", "100"]

        for depth in expected_moisture_depths:
            expected_field = f"SOIL_MOISTURE_{depth}"
            self.assertIn(
                expected_field,
                moisture_fields,
                f"Expected moisture field {expected_field} not found",
            )


if __name__ == "__main__":
    unittest.main()
