'''
Created on Feb 2, 2018

@author: emigre459

This code allows for queries to be sent and results received from our OpenStreetMap SQL database

'''
import sqlite3
import pandas as pd


DATABASE = "../SW_WV_OSM.db"

def run_query(query_list, db_name):
    '''
    Runs the query specified on the database specified, printing the results to the console
    
    query_text: str. Contains the text of your query. New lines of SQL comprising the same query (e.g.
                separating the SELECT statement from the FROM statement) should be separated with the \n newline
    db_name: str. Name of the database located one directory above this code's working directory.    
    '''
    
    query = "\n".join(query_list) + ";"
    
    conn = sqlite3.connect(db_name) # @UndefinedVariable
    
    #print("Single-line query text: {};\n\n".format(" ".join(query_list)))
    #print("**** QUERY TEXT ****\n\n{}\n\n".format(query))
    
    results_df = pd.read_sql_query(query, conn)    
    #print("**** QUERY RESULTS ****\n")
    #print(results_df)
    #print("RESULTS PRINT SUPPRESSED")
    
    conn.close()
    return results_df
    
###########     MAIN CODE EXECUTION SPACE       ###########
'''base_query = ["SELECT COUNT(value)",
         "FROM nodes_tags",
         "WHERE (key = 'amenity' or key = 'shope') and (value = 'ATV Trails' or value = 'Tiles')"]
        #"LIMIT 10"]

df = run_query(base_query, DATABASE)'''