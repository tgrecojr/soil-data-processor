#!/usr/bin/env python3
"""
Test runner script for the soil data processing application.

This script provides different test execution options:
- Run all tests
- Run specific test categories (unit, integration, database)
- Run with coverage reporting
- Run in verbose mode
"""

import sys
import os
import subprocess
import argparse


def run_command(command, description):
    """Run a command and handle the result."""
    print(f"\n{description}")
    print("=" * 60)

    result = subprocess.run(command, shell=True, capture_output=False)

    if result.returncode == 0:
        print(f"\n✅ {description} completed successfully")
    else:
        print(f"\n❌ {description} failed with exit code {result.returncode}")

    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run tests for soil data processor")
    parser.add_argument("--unit", action="store_true", help="Run unit tests only")
    parser.add_argument(
        "--integration", action="store_true", help="Run integration tests only"
    )
    parser.add_argument(
        "--database", action="store_true", help="Run database tests only"
    )
    parser.add_argument(
        "--coverage", action="store_true", help="Run with coverage reporting"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--no-cov", action="store_true", help="Skip coverage reporting")

    args = parser.parse_args()

    # Base pytest command
    base_cmd = "python -m pytest"

    # Add verbosity
    if args.verbose:
        base_cmd += " -v"

    # Add coverage if not disabled
    if not args.no_cov:
        base_cmd += (
            " --cov=. --cov-report=term-missing --cov-report=html --cov-report=xml"
        )

    # Determine which tests to run
    test_commands = []

    if args.unit:
        cmd = f"{base_cmd} tests/test_processsoildata.py::TestUtilityFunctions tests/test_fieldmappings.py"
        test_commands.append((cmd, "Running Unit Tests"))

    if args.integration:
        cmd = f"{base_cmd} tests/test_integration.py"
        test_commands.append((cmd, "Running Integration Tests"))

    if args.database:
        cmd = f"{base_cmd} tests/test_database.py"
        test_commands.append((cmd, "Running Database Tests"))

    # If no specific test type selected, run all tests
    if not any([args.unit, args.integration, args.database]):
        cmd = f"{base_cmd} tests/"
        test_commands.append((cmd, "Running All Tests"))

    # Run the tests
    all_passed = True
    for command, description in test_commands:
        result = run_command(command, description)
        if result != 0:
            all_passed = False

    # Summary
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 All tests passed successfully!")

        # Show coverage report location if coverage was run
        if not args.no_cov:
            print("\n📊 Coverage report generated:")
            print("  - Terminal: see above")
            print("  - HTML: open htmlcov/index.html in your browser")

        sys.exit(0)
    else:
        print("💥 Some tests failed. Please check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
