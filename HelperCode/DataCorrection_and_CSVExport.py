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



OSM_file = 'SW_WestVirginia.osm'

def correct_and_record(osm_file):
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
                    '''TODO: add data_correction algorithm call here, remembering that you'll need to iterate
                    through the dict list returned, appending to the df each iteration. Don't forget to skip
                    appending to df if tag_dicts = None (means we had problem chars in tag key'''
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
                    '''TODO: add data_correction algorithm call here, remembering that you'll need to iterate
                    through the dict list returned, appending to the df each iteration. Don't forget to skip
                    appending to df if tag_dicts = None (means we had problem chars in tag key'''
                    ways_tags_df.append(ways_tags_dict)
                
                
                #Now for way_nodes:
                i = 0
                for elem in element.iter('nd'):
                    nd_dict = {'id': ways_dict['id'], 'node_id': elem.attrib['ref'], 'position': i}
                    ways_nodes_df.append(nd_dict)
                    i += 1
    
    ####################    Writing to CSV    ######################       
    #TODO: don't forget to remove first row of dummy data from each df before export!
    
    
    nodes_df.to_csv('./CSV for SQL Tables/nodes.csv', index=False, encoding='utf-8')
    nodes_tags_df.to_csv('./CSV for SQL Tables/nodes_tags.csv', index=False, encoding='utf-8')
    ways_df.to_csv('./CSV for SQL Tables/ways.csv', index=False, encoding='utf-8')
    ways_tags_df.to_csv('./CSV for SQL Tables/ways_tags.csv', index=False, encoding='utf-8')
    ways_nodes_df.to_csv('./CSV for SQL Tables/ways_nodes.csv', index=False, encoding='utf-8')
    
    
    
def data_correction(elem, parent_dict, df, state_needed=False):
    '''
    Corrects data in an individual child tag of a node or a way (excluding nd tags) and returns
    a list of dicts, each of which can be used as the nodes_tags_dict or ways_tags_dict, as appropriate. This is
    a list because the zip code correction may generate multiple dicts, one for each zip code identified when a list
    existed in the original data set. 
    
    As ways and nodes in my data set appear to have a lot of similar attributes (e.g. both have zip codes, 
    lists of zip codes at times, etc.), this data correction algorithm is agnostic with respect to its 
    treatment of nodes vs. ways
    
    elem: ET element representing a child tag of a node or way
    parent_dict: dict that describes the parent node or way.
    type: str. Allowed values are 'node' or 'way'. Some specific correction steps DO need knowledge about the way/
                node status of what the data are, although the majority of corrections are agnostic.
    df: DataFrame. Since some corrections (e.g. county FIPS code translation) require context from other
                    child tags of the node/way in question (e.g. knowledge of the state for the county FIPS code
                    translation), this needs knowledge of the current state of the relevant DataFrame to provide
                    proper context.
    state_needed: bool. Default False. Indicates if a previous iteration of this algorithm identified that
                    it lacked knowledge of the state of the node/way in question and still needs that info.
                    The expectation is that, with the full list of node/way child tags in hand, there will
                    always be sufficient context for determining what the state is.
    '''
    
    k = elem.attrib['k']
    tag_dicts = []
    tag_dict = {}
    
    #Only do anything meaningful with this tag if it isn't problematic
    if not PROBLEMCHARS.search(k):
        
        
        ############ ZIP CODES ############
        
        #Deal with the zip identified earlier that needs special attention for correction
        if audit.isZipCode(elem):
            zipList = []
            
            
            if tag_dict['id'] == '2625119248' and type == 'node':
                zipList = ['25314']
                print("'WV' zip code corrected!")
        
            else:
                #Expect that anything in this category will be of the following forms (based on audit):
                    #12345-0000
                    #12345:123400
                    #12345;23456;34567;...
                v = elem.attrib['v']
                if not v.isdigit():
                    if "-" in v:
                        zipList = [v.strip()[:5]]                                
                    elif ":" in v:
                        unstripped_zipList = v.split(":")
                        zipList = map(str.strip(), unstripped_zipList)
                    elif ";" in v:
                        unstripped_zipList = v.split(";")
                        zipList = map(str.strip(), unstripped_zipList)
            
            for zipCode in zipList:
                tag_dict['value'] = zipCode
                #For ease of later analysis, set everything to have tag key = "addr:postcode"
                tag_dict['key'] = 'postcode'
                tag_dict['type'] = 'addr'
                
                
                
        ############ STATES ############
        
        
        
        
        ############ COUNTIES ############
        
        '''TODO: need to return a bool flag that indicates if context was lacking for the state of a way/node
        when the FIPS code for a county was being determined, so that when that whole parent node/way is processed,
        a re-run of the FIPS code algorithm can be done (assuming there is sufficient state context at that time...
        This bool will also indicate if I've already extracted a state from a county,state record and skip
        recording the state if the record is already there in df, or throw an error if the states are different
        (making sure to account for different naming possibilities, like Wv or WV)'''
        
        
        ############ AMENITIES/SHOPS ############
        
        
        
            
            
        ############ ALL OTHER TAG TYPES ############
    
        
    
        tag_dict['value'] = elem.attrib['v']
        tag_dict['id'] = parent_dict['id']
        
        
        if ":" in k:
            tag_k_labels = k.split(":")
            #Make part before ":" the type, part after the key
            nodes_tags_dict['type'] = tag_k_labels[0]
            nodes_tags_dict['key'] = ":".join(tag_k_labels[1:])
        else:
            nodes_tags_dict['type'] = 'regular'
            nodes_tags_dict['key'] = k
        
        tag_dicts = [tag_dict]
        
    else: #This is scenario wherein tag key had problem characters in it, and we're skipping
        tag_dicts = None
            
    
    