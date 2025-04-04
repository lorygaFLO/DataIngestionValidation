import os
from utils.import_configs import get_config

class Reporter:
    def __init__(self, base_report_path=None):
        if base_report_path is None:
            base_report_path = get_config('validation_reports_folder')
            if not os.path.isabs(base_report_path):
                base_report_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), base_report_path)
        self.base_report_path = base_report_path
        if not os.path.exists(self.base_report_path):
            os.makedirs(self.base_report_path)

    def _create_report_path(self, input_file_path):
        relative_path = os.path.relpath(input_file_path, start=os.path.dirname(self.base_report_path))
        report_path = os.path.join(self.base_report_path, relative_path)
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        return os.path.splitext(report_path)[0]  # Remove the file extension

    def write_report(self, input_file_path, messages):
        if not messages or all("Passed" in message for message in messages):
            return
        report_path = self._create_report_path(input_file_path) + '.txt'
        with open(report_path, 'w') as report_file:
            for message in messages:
                report_file.write(message + '\n')
        print(f"Report written to {report_path}")

# Usage example
if __name__ == "__main__":
    reporter = Reporter()
    reporter.write_report(r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\data\test1.csv', ["Validation passed", "All required columns are present"])
