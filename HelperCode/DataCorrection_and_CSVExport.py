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
import Audit_Simple as audit
import xml.etree.cElementTree as ET
import pandas as pd
import re

PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


#print(FIPS_to_Name("../2010_FIPSCodes.csv", '039', state_name=None, state_FIPS='54'))

'''TODO: code here will need to track ALL tags for a given node or way in order to get the context of 
what state the node/way is in for FIPS code purposes'''

OSM_file = 'SW_WestVirginia.osm'

def data_correction(osm_file):
    '''
    Churns through the OSM file being investigated, checking different components using results of the
    previous audits and correcting data as needed. Data are then, as corrected, appended to CSV files.
    
    osm_file: str. Filepath for OpenStreetMap file of interest. 
    '''
    
    with open(osm_file, "rb") as fileIn:
        #Setting up the different DataFrames such that they mirror the SQL data schema
        #First row is a dummy row to allow pandas to infer column dtypes correctly
        nodes_df = pd.DataFrame(data = [[0, 0.0, 0.0, '', 0, 0, 0, '']], 
                                columns=['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp'])
        nodes_dict = {}
        
        nodes_tags_df = pd.DataFrame(data=[[0, '', '', '']],
                                     columns=['id', 'key', 'value', 'type'])
        
        nodes_tags_dict = {}
        
        ways_df = pd.DataFrame(data=[[0,'', 0, 0, 0, '']],
                               columns=['id', 'user', 'uid', 'version', 'changeset', 'timestamp'])
        
        ways_dict = {}
        
        ways_tags_df = pd.DataFrame(data=[[0, '', '', '']],
                                    columns=['id', 'key', 'value', 'type'])
        
        ways_tags_dict = {}
        
        ways_nodes_df = pd.DataFrame(data=[[0,0,0]],
                                     columns=['id', 'node_id', 'position'])
        
        ways_nodes_dict = {}
        
        #Iterating through the XML file
        for _, element in ET.iterparse(fileIn):
            
             ####################    NODES    ######################
    
            if element.tag == 'node':
                #REMEMBER: we need to check to see if the 'k' attrib of each tag is problematic
                    #If it is: ignore it entirely
                    #If it isn't: take only the chars after ":" (if one is present) as key and set 'type' to be
                        #the chars preceding ":"
                
                nodes_dict = {'id': int(element.attrib['id']), 
                              'lat': float(element.attrib['lat']), 
                              'lon': float(element.attrib['lon']), 
                              'user': element.attrib['user'], 
                              'uid': int(element.attrib['uid']), 
                              'version': int(element.attrib['version']), 
                              'changeset': int(element.attrib['changeset']), 
                              'timestamp': element.attrib['timestamp']}
                
                nodes_df.append(nodes_dict)
                        
                for elem in element.iter('tag'):
                    k = elem.attrib['k']
                    
                    #Only do anything meaningful with this tag if it isn't problematic
                    if not PROBLEMCHARS.search(k):
                        #TODO: this is where all node tag auditing needs to occur
                        
                        if ":" in k:
                            tag_k_labels = k.split(":")
                            #Make part before ":" the type, part after the key
                            nodes_tags_dict['type'] = tag_k_labels[0]
                            nodes_tags_dict['key'] = ":".join(tag_k_labels[1:])
                        else:
                            nodes_tags_dict['type'] = 'regular'
                            nodes_tags_dict['key'] = k
                    
                        nodes_tags_dict['value'] = elem.attrib['v']
                        nodes_tags_dict['id'] = nodes_dict['id']
                        
                        
                                                
                        nodes_tags_df.append(nodes_tags_dict)
                
                
                
                
                
            ####################    WAYS    ######################
            elif element.tag == 'way':
                #[0,'', 0, 0, 0, '']
                ways_dict = {'id': int(element.attrib['id']), 
                             'user': element.attrib['user'], 
                             'uid': int(element.attrib['uid']), 
                             'version': int(element.attrib['version']), 
                             'changeset': int(element.attrib['changeset']), 
                             'timestamp': element.attrib['timestamp']}
                
                ways_df.append(ways_dict)
                
                #REMEMBER: we need to check to see if the 'k' attrib of each tag is problematic
                    #If it is: ignore it entirely
                    #If it isn't: take only the chars after ":" (if one is present) as key and set 'type' to be
                        #the chars preceding ":"
                    #ALSO: I don't think LOWER_COLON regex is needed, will ignore tags without colons
                
                for elem in element.iter('tag'):
                    k = elem.attrib['k']
                    
                    #Only do anything meaningful with this tag if it isn't problematic
                    if not PROBLEMCHARS.search(k):
                        #TODO: this is where all ways tag auditing should occur
                        
                        if ":" in k:
                            tag_k_labels = k.split(":")
                            #Make part before ":" the type, part after the key
                            ways_tags_dict['type'] = tag_k_labels[0]
                            ways_tags_dict['key'] = ":".join(tag_k_labels[1:])
                        else:
                            ways_dict['type'] = 'regular'
                            ways_tags_dict['key'] = k
                    
                        ways_tags_dict['value'] = elem.attrib['v']
                        ways_tags_dict['id'] = ways_dict['id']
                        
                        
                    
                    ways_tags_df.append(ways_tags_dict)
                
                
                #Now for way_nodes:
                i = 0
                for elem in element.iter('nd'):
                    nd_dict = {'id': ways_dict['id'], 'node_id': elem.attrib['ref'], 'position': i}
                    ways_nodes_df.append(nd_dict)
                    i += 1
    
    ####################    Writing to CSV    ######################       
    nodes_df.to_csv('./CSV for SQL Tables/nodes.csv', index=False, encoding='utf-8')
    nodes_tags_df.to_csv('./CSV for SQL Tables/nodes_tags.csv', index=False, encoding='utf-8')
    ways_df.to_csv('./CSV for SQL Tables/ways.csv', index=False, encoding='utf-8')
    ways_tags_df.to_csv('./CSV for SQL Tables/ways_tags.csv', index=False, encoding='utf-8')
    ways_nodes_df.to_csv('./CSV for SQL Tables/ways_nodes.csv', index=False, encoding='utf-8')
    
    
    