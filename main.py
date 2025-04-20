from engine.read_data_pandas import DataReader
from engine.execute_checks import Validator

def main():
    print("Process started...")

    data_paths = DataReader().read_all_files()

    validator = Validator()
    matched_files = validator.validate_files(data_paths)
    #print(matched_files)


    print("Process ended")

if __name__ == "__main__":
    main()
