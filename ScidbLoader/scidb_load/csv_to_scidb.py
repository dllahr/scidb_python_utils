'''
Created on Nov 27, 2012

@author: dlahr
'''
import commands

def convert_csv_to_scidb(csv_file_path):
    """converts the the csv file indicated by the csv_file_path to a scidb formatted file
    using the csv2scidb called via OS command.  Uses a chunk size of 1000000
    """
    base_name = csv_file_path.split(".")[0]
    scidb_name = base_name + ".scidb"
    
    commands.getoutput("csv2scidb -s 1 -c 1000000 -i " + csv_file_path + " -o " + scidb_name)

    return scidb_name
