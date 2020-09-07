__doc__ = """THis script is designed to replace the string inside field QUERY of the config 
              csv file with another string of your choice.
             Inputs: (1) a file containing key-value pairs with the table name and its new query
                     (2) the csv file you wish to modify
             Assumptions:
                        - Both files uses ; as a separator
                        - For the config file, we always use position [1] for our comparison
               """
__author__ = "Bruno Cervantes"
__version__ = "1.0"


import csv
import sys
import os
import argparse
import logging


class QueryChanger:
    def __init__(self, queries, csv_config):
        self.csv_file = csv_config
        self.pwd = os.getcwd()
        self.appName = csv_config.split(".", 1)[0]
        self.queries_path = os.path.join(self.pwd, queries)
        self.csv_path = os.path.join(self.pwd, csv_config)
        self.output_file = self.csv_path + "_transformed"
        self.logFile = os.path.join(self.pwd, self.appName + ".log")
        self.logger = logging.getLogger("Change Quieries: " + self.appName)
        self.logger.setLevel(logging.INFO)
        self.fileLogger = logging.FileHandler(self.logFile, mode='w')  # Create File handler for WARN+ messages
        self.fileLogger.setLevel(logging.WARNING)
        self.consoleLogger = logging.StreamHandler()  # Create console handler for INFO+ messages
        self.consoleLogger.setLevel(logging.INFO)
        self.logger.addHandler(self.consoleLogger)
        self.logger.addHandler(self.fileLogger)

    def __read_kv_file(self, input_file):
        """Reads the key_value file and returns a dictionary"""
        try:
            with open(input_file, 'r') as f_kv:
                reader = csv.reader(f_kv, delimiter=";")
                mydict = {rows[0].lower(): rows[1] for rows in reader}
            return mydict
        except IOError:
            raise Exception("IO error @ reading kv file: File doesn't exist or you have no permissions ")
        except Exception as E:
            raise Exception("Unexpected error @ reading kv file:", E)

    def __verify_queries(self, dic):
        """Attempts to do a quick analysis of the queries to be changed:
            Whether it contains an equal amount of quotes, doesn't contain invalid characters, basic SQL syntax checkup,
            and whether it contains the sqoop variable '\$CONDITIONS' and adds it if not found """
        condition = ['and \$conditions', 'where \$conditions']
        keywords = ["select", "from"]
        tilde = "`"
        quotes = "\'"
        valid_dict = {}
        parenthesis = ["(", ")"]

        def aux_query(record_value):
            try:
                if 'where' not in value.lower():
                    new_value = record_value[:-1] + 'WHERE \\$CONDITIONS"'
                else:
                    new_value = record_value[:-1] + 'AND \\$CONDITIONS"'
                return new_value
            except Exception as E2:
                raise Exception("aux_query method failed", E2)

        try:
            for key, value in dic.items():
                if value.count(quotes) % 2 != 0:
                    self.logger.warning(f"WARNING: Query contains odd number of ' : {key} - {value} ")
                elif value.count(parenthesis[0]) + value.count(parenthesis[1]) % 2 != 0:
                    self.logger.warning(f"WARNING: Query contains odd number of () : {key} - {value} ")
                elif tilde in value:
                    self.logger.warning(f"WARNING: Query contains the invalid character ` : {key} - {value} ")
                elif keywords[0] not in value.lower() or keywords[1] not in value.lower():
                    self.logger.warning(f"WARNING: Query doesn't contain either the 'SELECT' or the 'FROM' keywords : {key} - {value} ")
                elif condition[0] not in value.lower() or condition[1] not in value.lower():
                    self.logger.warning(f"WARNING: Query doesn't contain '\$CONDITIONS' : {key} - {value} ")
                    self.logger.warning(f"WARNING: Adding '\$CONDITIONS' to query of table : {key}")
                    valid_dict[key] = aux_query(value)
                elif condition[0] in value.lower() or condition[1] in value.lower():
                    valid_dict[key] = value
                    self.logger.warning(f"Successfully changed the query of table: {key}  ")
            return valid_dict
        except Exception as E3:
            raise Exception("Unexpected error @ verifying queries:", E3)

    def __read_csv(self, config):
        """Reads the config csv & creates a list where each element is a row """
        try:
            with open(config, 'r') as f_config:
                reader = csv.reader(f_config, delimiter=";")
                rows = [row for row in reader]
            return rows
        except IOError:
            raise Exception("IO error @ reading config file: File doesn't exist or you have nor permissions ")
        except Exception as E4:
            raise Exception("Unexpected error @ reading config file:", E4)

    def __replace_field(self, rows, header, queries):
        """Replaces the string in the desired position with the correcponsing value of the dictionary
            we are forcing keys to be in lowercase because the config file may contain strings that are mismatched"""
        target_field = header.index("QUERY")

        def aux_replace(row):
            try:
                if row[1].lower() in queries:
                    row[target_field] = queries[row[1].lower()]
                return row
            except Exception as E5:
                raise Exception("aux_replace method failed", E5)

        try:
            final_rows = [aux_replace(row) for row in rows]
            return final_rows
        except Exception as E6:
            raise Exception("Unexpected error @ replacing fields:", E6)

    def __write_output(self, changed_rows):
        """ Writes the output file"""
        try:
            with open(self.output_file, 'w+', newline='') as f_output:
                writer = csv.writer(f_output, delimiter=";")
                writer.writerow(changed_rows)
        except IOError:
            raise Exception("IO error @ saving output: There is no space in the FS or you have no permissions ")
        except Exception as E7:
            raise Exception("Unexpected error @ reading kv file:", E7)

    def run(self):
        try:
            self.logger.info(f"INFO: Processing {self.csv_file} \n")
            queries = self.__read_kv_file(self.queries_path)
            valid_queries = self.__verify_queries(queries)
            self.logger.info(f"INFO: We have verified that " + str(len(valid_queries)) + "out of the original " +
                             str(len(queries)) + "queries are valid")
            rows_to_transform = self.__read_csv(self.csv_path)
            extracted_header = rows_to_transform[0]
            rows_transformed = self.__replace_field(rows_to_transform, extracted_header, valid_queries)
            self.__write_output(rows_transformed)
            self.logger.info(f"SUCCESS: Saved file into {self.output_file}")
        except Exception as err:
            self.logger.info("Failure: ", err)
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("queries",
                        type=str,
                        help="A key-value pair file using ; to denote relationship. Each item being in a separate row")
    parser.add_argument("config_csv",
                        type=str,
                        help="The CSV file that has the configs")
    args = parser.parse_args()

    process = QueryChanger(args.queries, args.config_csv)

    process.run()


