from engine.read_data_pandas import DataReader
from engine.execute_checks import Validator

def main():
    print("Hello World")

    data = DataReader().read_all_files()

    validator = Validator()
    matched_files = validator.validate_files(data)
    #print(matched_files)

    _ = 1 + 1

if __name__ == "__main__":
    main()
