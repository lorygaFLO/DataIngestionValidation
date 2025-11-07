import os
from config.settings import *

class Reporter:
    def __init__(self, base_report_path):
        """
        Initialize the Reporter with a mandatory report path.

        Args:
            base_report_path (str): Path where reports will be saved. 
                                  Can be absolute or relative to the project root.

        Raises:
            ValueError: If base_report_path is not provided
        """
        if base_report_path is None:
            raise ValueError("base_report_path must be provided")

        self.S = get_settings()
        self.base_report_path = os.path.join(self.S.PATH_REPORTS_RUN, base_report_path) or self.S.PATH_REPORTS_RUN


    def _create_report_path(self, input_file_path):

        # Get just the filename if it's an absolute path
        input_filename = os.path.basename(input_file_path)
        report_path = os.path.join(self.base_report_path, input_filename)
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        return os.path.splitext(report_path)[0]  # Remove the file extension

    def write_report(self, input_file_path, messages):

        if not messages or all("Passed" in message for message in messages) or self.S.DISABLE_REPORTS:
            return
        
        report_path = self._create_report_path(input_file_path) + '.txt'
        with open(report_path, 'w') as report_file:
            for message in messages:
                report_file.write(message + '\n')
        print(f"Report written to {report_path}")

# Example usage with relative paths
if __name__ == "__main__":
    reporter = Reporter('reports/validation_reports')
    reporter.write_report('data/input/test1.csv', ["Validation passed", "All required columns are present"])
