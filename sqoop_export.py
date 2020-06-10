
# -*- encoding: utf-8 -*-
__doc__ = """This script allows the user to do a sqoop export with queries.
Basically  this script's function is to create an intermediate Impala table with the desired query (env constraints).
Then do a sqoop export of the aforementioned table into the destination mysql table
REMEMBER: you need to pass 2 arguments to this script:
    First, a text file with the desired table names (one name per line).
    Second: the name of the .hql file with the queries to launch"
    EX: python python_mysql.py listado_de_tablas.txt queries.hql"

TO-DO: Replace the file.hql + table names with a single json file to then construct the .hql file with the desired queries
Another one is improving the control of exceptions raised by task outside the script such as the sqoop"""


__author__ ="Bruno Cervantes"
__version__="1.0"

import argparse
import sys
import os
import subprocess


### housekeeping:

def housekeeping(table_names_file):
    """Makes sure that both the HDFS paths + Hive DB and tables do not exist, to prevent a failure of the script"""
    try:
        with open(table_names_file) as input:
            with open("drop_tables.hql", 'w') as output:
                for line in input:
                    output.write("DROP TABLE table." + (str(line).rstrip("\n") + ";\n"))
                output.write("DROP DATABASE database;")
        subprocess.call(
            "beeline -u \"jdbc:hive2://tuserver\" --force=true -f drop_tables.hql",
            shell=True)
        subprocess.call("hadoop fs -rm -r /hdfs/path ",
                        shell=True)
        output.close()
        input.close()
        return 0
    except IOError:
        raise Exception("IO Error @ housekeeping: Could not open file ", table_names_file)
    except KeyboardInterrupt:
        raise Exception('Interrupted @ housekeeping')
    except Exception as EX:
        raise Exception("Unexpected Error @ housekeeping: ", EX)


###lanzar query y crear tablas
def beeline(queries_file):
    """Executes via Beeline the desired queries to create the staging tables in Hive and HDFS for the sqoop export"""
    try:
        subprocess.call(
            "beeline -u \"jdbc:impala://tuserver\" --force=true -f " + str(
                queries_file), shell=True)
        return 0
    except IOError:
        raise Exception("IO Error @ beeline: Could not find file ", queries_file)
    except subprocess.CalledProcessError as E:
        raise Exception('Subprocess error @ beeline: ', E)
    except KeyboardInterrupt:
        raise Exception('Interrupted @ beeline')
    except Exception as EX:
        raise Exception("Unexpected Error @ beeline: ", EX)


####lanzar sqoop export
def sqoop_export(table_names_file):
    try:
        with open(table_names_file) as input:
            for line in input:
                subprocess.call(
                    "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/Path/to/the/mysql-connector-java-5.1.47.jar \n" +
                    "sqoop export -Dsqoop.export.records.per.statement=5000 -Dsqoop.export.statements.per.transaction=2000" +
                    " --connect \"jdbc:mysql://some.server:6969/bbdd\" --input-null-non-string \\N --input-null-string \\N" +
                    " --username user  --password pass" +
                    " --table " + (str(line).rstrip("\n")) + " --export-dir /path/to/hdfs" + (str(line).rstrip("\n")) + " --fields-terminated-by \001"
                    " --num-mappers 10 ", shell=True)
        input.close()
        return 0
    except IOError:
        raise Exception("IO Error @ sqoop_export: Could not find file ", table_names_file)
    except subprocess.CalledProcessError as E:
        raise Exception('Subprocess error @ sqoop_export: ', E)
    except KeyboardInterrupt:
        raise Exception('Interrupted @ sqoop_export')
    except Exception as EX:
        raise Exception("Unexpected Error @ sqoop_export: ", EX)


def wrap_up():
    """Ensures everything has been deleted after the successful or unsuccessful execution of the script"""
    try:
        subprocess.call(
            "beeline -u \"jdbc:hive2://yourserver\" --force=true -f drop_tables.hql",
            shell=True)  ##drop tables and database
        subprocess.call("hadoop fs -rm -r /path/to/hdfs ", shell=True)  ##delete intermediate directory
        os.remove("drop_tables.hql")
        return 0
    except subprocess.CalledProcessError as E:
        raise Exception('Subprocess error @ wrap_up: ', E)
    except KeyboardInterrupt:
        raise Exception('Interrupted @ wrap_up')
    except Exception as EX:
        raise Exception("Unexpected Error @ wrap_up: ", EX)


def main(function_list):
    try:
        if any(step != 0 for step in function_list):
            sys.exit(1)
        else:
            print("All went OK")
            sys.exit(0)
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(1)
    finally:
        wrap_up()



######### main ######
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('table_names_file',
                        type=str,
                        help="File containing table names.")
    parser.add_argument('queries_file',
                        type=str,
                        help="File (.hql) containing queries")

    args = parser.parse_args()

    os.chdir(os.path.dirname(os.path.abspath(args.table_names_file)))

    function_list = [housekeeping(args.table_names_file), beeline(args.queries_file), sqoop_export(args.table_names_file)]

    main(function_list)
