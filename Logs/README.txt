log files for the ChangeDB daemon will be located in this directory

log.txt is the default process log and will contain runtime information
other logs will be created when tables are imported
their log files will be located in a directory with the same name as the table:

example_table/example_table.log - bcp STDOUT log
example_table/example_table.import_errors - bcp error log (-e) for import 
example_table/example_table.export_errors - bcp error log (-e) for export

BCP files will be stored (temporarily or permanently depending on configs) in the same directory:

example_table/example_table.bcp