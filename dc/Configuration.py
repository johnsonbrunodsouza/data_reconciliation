class Configuration:
    def __init__(self,operation,source_file,target_file,source_file_header,
                 target_file_header,source_file_delimiter,target_file_delimiter,
                 comparison_keys,ignore_keys,reconciliation_report,partition_files,
                 file_partition_directory,reconciliation_output):
        self.operation = operation
        self.source_file = source_file
        self.target_file = target_file
        self.source_file_header = source_file_header
        self.target_file_header = target_file_header
        self.source_file_delimiter = source_file_delimiter
        self.target_file_delimiter = target_file_delimiter
        self.comparison_keys = comparison_keys
        self.ignore_keys = ignore_keys
        self.reconciliation_report = reconciliation_report
        self.partition_files = partition_files
        self.file_partition_directory = file_partition_directory
        self.reconciliation_output = reconciliation_output
## word separator