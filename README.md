# Project Title

[Project description]

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

What things you need to install the software and how to install them:

- Python
- pandas
- unittest

### Installing

A step by step series of examples that tell you how to get a development environment running.

[Installation steps]

## Running the tests

This guide will help you run unit tests for the `test_api_handler.py` file, which tests the functionality of getting data from an API.

### Prerequisites

Ensure you have Python installed on your system. The codebase uses Python's built-in unittest module for testing.

### Steps to Run the Tests

1. Open your terminal or command prompt.
2. Navigate to the directory containing the `test_api_handler.py` file.
3. Run the following command:
python -m unittest test_api_handler.py

This command tells Python to run the unittest module as a script, with `test_api_handler.py` as an argument. The unittest module will then discover and run all the test methods in `test_api_handler.py`.

### Understanding the Tests

The `test_api_handler.py` file contains several test methods within the TestAPIHandler class. Each method tests a specific function from the `api_handler.py` file:

- test_get_data_from_api: Tests the get_data_from_api function.
- test_save_data_as_parquet: Tests the save_data_as_parquet function.
- test_get_all_stations: Tests the get_all_stations function.
- test_save_stations_as_parquet: Tests the save_stations_as_parquet function.

Each test method makes assertions about the expected behavior of the function it's testing. If all assertions pass, the test is successful. If any assertion fails, the test fails and unittest will print an error message.

Please replace `'your_api_key'` with your actual API key in the test_get_all_stations method before running the tests.

## Authors

[Your Name]

## License

[License details]

## Acknowledgments

[Acknowledgments]