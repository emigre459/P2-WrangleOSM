'''
Created on Feb 2, 2018

@author: emigre459

This code allows for queries to be sent and results received from our OpenStreetMap SQL database

'''
import sqlite3
import pandas as pd


DATABASE = "../SW_WV_OSM.db"

def run_query(query_text, db_name):
    '''
    Runs the query specified on the database specified, printing the results to the console
    
    query_text: str. Contains the text of your query. New lines of SQL comprising the same query (e.g.
                separating the SELECT statement from the FROM statement) should be separated with the \n newline
    db_name: str. Name of the database located one directory above this code's working directory.    
    '''
    conn = sqlite3.connect(db_name) # @UndefinedVariable
    
    results_df = pd.read_sql_query(query_text, conn)    
    print(results_df)
    
    conn.close()
    return results_df
    
###########     MAIN CODE EXECUTION SPACE       ###########
base_query = ["SELECT *",
         "FROM nodes_tags",
         "LIMIT 10"]

query = "\n".join(base_query) + ";"

df = run_query(query, DATABASE)