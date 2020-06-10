__doc__ = """A quick script to parse over an excel file and extract relevant information."
          Due to its simple nature, most variables were hardcoded """
__author__ = "Bruno Cervantes"
__version__ = "1.0"


import pandas as pd
import argparse
import sys
import os

useless_columns = ['Orden', 'Tabla', 'Grupo', 'Owner', 'Task Type', 'Aplicación', 'Datacenter', 'Node', 'Memname',
                   'Nombre Variable']


def read_excel(path_intro):
    """Reads input file and creates a pandas dataframe"""
    try:
        return pd.read_excel(path_intro, header=0, delimiter=';')
    except Exception:
        raise Exception("Error reading file")


def extract_param_columns(dataframe):
    """Takes the dataframe we created from the input and we extract the relevant rows"""
    try:
        df = dataframe[dataframe.apply(lambda row: row['Nombre Variable'] in {'%%PARM1', '%%PARM2', '%%PARM3'}, axis=1)]
        return df
    except Exception:
        raise Exception("Error extracting columns")


def drop_columns(dataframe):
    """Drops the useless columns"""
    try:
        df = dataframe.drop(useless_columns, axis=1)
        return df
    except Exception:
        raise Exception("DF not found or error dropping columns")

# group_df:
# First: creates a new column which will count the accumulation of each instance of the same value of the column chosen. So if there are 7 rows with value "A", they will have values in this columns from 0 to 6.
# Second: sets the index based on 2 columns, Job name and the accum. Therefore our df will be analogous of an excel pivot table.
# Final: unstack our 'pivot table' on level -1 (the second columns, in this case our accum). Our new DF now has 7 values for each index value. In this case each value of column 'job_name'
# a side effect of df_grouped is that the column names of the new columns are an array of 2 values thanks to the unstack. We fix this here
# TL:DR: Instead of 7 rows per label, each having a different value of parameter, we now have 1 row per label with the 7 parameters.


def group_df(dataframe):
    """Read brick wall of text above"""
    try:
        df_grouped = dataframe.assign(accum=dataframe.groupby(['Jobname']).cumcount()).set_index(['Jobname', 'accum']).unstack(-1)
        df_grouped.columns = [f'{x} {y}' for x, y in df_grouped.columns]
        df_aggregated = df_grouped.reset_index().rename(columns={'Valor Variable 0': 'G_FUNCIONAL', 'Valor Variable 1': 'IDENTIFICADOR', 'Valor Variable 2': 'FICHERO'})
        return df_aggregated
    except Exception:
        raise Exception("Unable to transform input DF to its final form")


def save_to_csv(dataframe, path):
    """Saves our cleaned DF to a csv file"""
    try:
        dataframe.to_csv(path, sep=';', index=False)
    except Exception:
        raise Exception("Unable to save final Dataframe to disk")


def main():
    try:
        print(f"Processing {os.path.basename(args.path_intro)} ")
        input_df = read_excel(args.path_intro)
        df_params_extracted = extract_param_columns(input_df)
        df_cleaned = drop_columns(df_params_extracted)
        df_transformed = group_df(df_cleaned)
        save_to_csv(df_transformed, args.path_output)
        print(f"SUCCESS: Saving File into {args.path_output} ")
    except Exception as err:
        print(f"FAILURE: Script failed in step: {err}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path_intro',
                        type=str,
                        help="Path of input, an Excel File")
    parser.add_argument('path_output',
                        type=str,
                        help="Path of output, a CSV file")

    args = parser.parse_args()

    main()
