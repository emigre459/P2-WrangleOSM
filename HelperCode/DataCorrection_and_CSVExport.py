'''
Created on Jan 24, 2018

@author: emigre459

The purpose of this module is to take what we've discovered in the WV OSM data through auditing and use it
to correct the XML data as it parses through each parent tag. As the data are corrected, they will also be
formatted to correspond with the SQL schema described in the file data_wrangling_schema.sql and, at the end
of the correcting and formatting process, these updated data will be exported to CSV format for later SQL
database ingestion.
'''

