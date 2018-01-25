'''
Created on Jan 24, 2018

@author: emigre459

The purpose of this module is to take what we've discovered in the WV OSM data through auditing and use it
to correct the XML data as it parses through each parent tag. As the data are corrected, they will also be
formatted to correspond with the SQL schema described in the file data_wrangling_schema.sql and, at the end
of the correcting and formatting process, these updated data will be exported to CSV format for later SQL
database ingestion.
'''
from FIPSCodeMapper import FIPS_to_Name


print(FIPS_to_Name("../2010_FIPSCodes.csv", '039', state_name=None, state_FIPS='54'))

#TODO: code here will need to track ALL tags for a given node or way in order to get the context of what state the node/way is in for FIPS code puropses