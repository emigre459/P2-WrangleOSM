'''
Created on Jan 24, 2018

@author: emigre459

The purpose of this module is to take what we've discovered in the WV OSM data through auditing and use it
to correct the XML data as it parses through each parent tag. As the data are corrected, they will also be
formatted to correspond with the SQL schema described in the file data_wrangling_schema.sql and, at the end
of the correcting and formatting process, these updated data will be exported to CSV format for later SQL
database ingestion.
'''
import FIPSCodeMapper as fips
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
        
        nodes = []
        nodes_tags = []        
               
        ways = []
        ways_tags = []
        ways_nodes = []
        
        

        
        #Iterating through the XML file
        for _, element in ET.iterparse(fileIn):
            '''Each time a new node or way is parsed, create a new temporary list of lists
            to contain only data about that specific node/way'''
            temp_childTag_data = []
            need_state = False
            
            ####################    NODES    ######################
    
            if element.tag == 'node':
                #REMEMBER: we need to check to see if the 'k' attrib of each tag is problematic
                    #If it is: ignore it entirely
                    #If it isn't: take only the chars after ":" (if one is present) as key and set 'type' to be
                        #the chars preceding ":"
                
                #dict is needed for clear input into data correction algorithm
                nodes_dict = {'id': element.attrib['id'],
                              'lat': element.attrib['lat'],
                              'lon': element.attrib['lon'],
                              'user': element.attrib['user'],
                              'uid': element.attrib['uid'],
                              'version': element.attrib['version'],
                              'changeset': element.attrib['changeset'],
                              'timestamp': element.attrib['timestamp']}
                
                nodes.append([nodes_dict['id'],
                              nodes_dict['lat'],
                              nodes_dict['lon'],
                              nodes_dict['user'],
                              nodes_dict['uid'],
                              nodes_dict['version'],
                              nodes_dict['changeset'],
                              nodes_dict['timestamp']])

                
                
                        
                for elem in element.iter('tag'):
                    '''TODO: add data_correction algorithm call here, appending to nodes_tags and temp_childTag_data
                    with result. Don't forget to skip appending if tag_dicts = None (means we had problem chars 
                    in tag key)'''
                    
                
                
                
                
                
            ####################    WAYS    ######################
            elif element.tag == 'way':
                wayID = elem.attrib['id']
                
                #dict is needed for clear input into data correction algorithm
                ways_dict = {'id': wayID,
                             'user': element.attrib['user'],
                             'uid': element.attrib['uid'],
                             'version': element.attrib['version'],
                             'changeset': element.attrib['changeset'],
                             'timestamp': element.attrib['timestamp']}
                
                ways.append([ways_dict['id'],
                             ways_dict['user'],
                             ways_dict['uid'],
                             ways_dict['version'],
                             ways_dict['changeset'],
                             ways_dict['timestamp']])
                
                
                
                for elem in element.iter('tag'):
                    '''TODO: add data_correction algorithm call here, appending to ways_tags and temp_childTag_data
                    with result. Don't forget to skip appending if tag_dicts = None (means we had problem chars 
                    in tag key)'''
                    
                
                
                #Now for way_nodes:
                i = 0
                for elem in element.iter('nd'):
                    ways_nodes.append([wayID,
                                       elem.attrib['ref'],
                                       i])
                    i += 1
    
    ####################    Writing to CSV    ######################  
         
    #First, let's throw these lists into DataFrames, for ease of writing to CSV and clarity in code
    nodes_df = pd.DataFrame(data = nodes,
                            columns=['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp'])
    nodes_tags_df = pd.DataFrame(data = nodes_tags,
                                 columns=['id', 'key', 'value', 'type'])
        
    ways_df = pd.DataFrame(data = ways,
                           columns=['id', 'user', 'uid', 'version', 'changeset', 'timestamp'])
    ways_tags_df = pd.DataFrame(data = ways_tags,
                                columns=['id', 'key', 'value', 'type'])
    ways_nodes_df = pd.DataFrame(data = ways_nodes,
                                 columns=['id', 'node_id', 'position'])
    
    
    #Now let's write the DataFrames to CSV
    nodes_df.to_csv('./CSV for SQL Tables/nodes.csv', index=False, encoding='utf-8')
    nodes_tags_df.to_csv('./CSV for SQL Tables/nodes_tags.csv', index=False, encoding='utf-8')
    ways_df.to_csv('./CSV for SQL Tables/ways.csv', index=False, encoding='utf-8')
    ways_tags_df.to_csv('./CSV for SQL Tables/ways_tags.csv', index=False, encoding='utf-8')
    ways_nodes_df.to_csv('./CSV for SQL Tables/ways_nodes.csv', index=False, encoding='utf-8')
    
    
    
def data_correction(elem, parent_dict, parsed_singleTag_data, county_fips_to_find=None):
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
    parsed_singleTag_data: list of lists. Each individual list is a row of data representing a child tag
                            of the parent singleTag (e.g. a single node or way). Since some corrections 
                            (e.g. county FIPS code translation) require context from other child tags of the 
                            node/way in question (e.g. knowledge of the state for the county FIPS code translation),
                            this def needs knowledge of the current state of the relevant node/way to provide
                            proper context.
    county_fips_to_find: str. If not None, indicates that a previous iteration of this algorithm identified that
                    it lacked knowledge of the state of the node/way in question and still needs that info to identify
                    the county FIPS in question. The expectation is that, with the full list of node/way child tags
                    in hand, there will be sufficient context for determining what the state is.
    '''
    
    k = elem.attrib['k'].strip()
    v = elem.attrib['v'].strip()
    tag_dict = {}
    
    #Only do anything meaningful with this tag if it isn't problematic
    if not PROBLEMCHARS.search(k):
        
        
        ############ ZIP CODES ############
        
        #Deal with the zip identified earlier that needs special attention for correction
        if audit.isZipCode(elem):
            zipList = []
            
            #latter condition checks if we're looking at a node or a way
            if tag_dict['id'] == '2625119248' and 'lon' in parent_dict.keys():
                zipList = ['25314']
                print("'WV' zip code corrected!")
        
            else:
                #Expect that anything in this category will be of the following forms (based on audit):
                    #12345-0000
                    #12345:123400
                    #12345;23456;34567;...
                if not v.isdigit():
                    if "-" in v:
                        zipList = [v.strip()[:5]]                                
                    elif ":" in v or ";" in v:
                        if ":" in v: delimiter = ":"
                        elif ";" in v: delimiter = ";"
                
                        unstripped_zipList = v.split(delimiter)
                        for e in unstripped_zipList:
                            zipList.append(e.strip())
                    else:
                        zipList = [v.strip()]
                else:
                    zipList = [v.strip()]
                    
                print(zipList)
                
            #Build the tag_dict
            for zipCode in zipList:
                tag_dict = {}
            
                tag_dict['id'] = parent_dict['id']
                tag_dict['value'] = zipCode
            
                #For ease of later analysis, set everything to have tag key = "addr:postcode"
                tag_dict['key'] = 'postcode'
                tag_dict['type'] = 'addr'
                
                parsed_singleTag_data.append(tag_dict['id'],
                                tag_dict['key'],
                                tag_dict['value'],
                                tag_dict['type'])
                
                
        
        ############ COUNTIES ############
        
        #Need lists for counties and states, as some nodes/ways have multiple county,state entries in a list
        elif audit.isCounty(elem):
            countyList = []
            stateList = []
            
            #Dealing with lists of counties here
            if ":" in v or ";" in v:
                if ":" in v: delimiter = ":"
                elif ";" in v: delimiter = ";"
                
                unstripped_countyList = v.split(delimiter)
                print(unstripped_countyList)
                for county in unstripped_countyList:
                    countyList.append(county.strip())
            
            
                #Now, if there are states to be pulled out, let's pull them out
                for i, county in enumerate(countyList):
                    if "," in county:
                        countyList[i] = county.split(",")[0].strip()
                        stateList.append(county.split(",")[1].strip())
                    else:
                        pass
                    
            #Now to deal with a single county,state combo
            elif ',' in v:
                countyList = [county.split(",")[0].strip()]
                stateList = [county.split(",")[1].strip()]
                
            #Now we look at counties that are only given as a FIPS code
            #Note that there are no lists of FIPS codes expected (only a single one per node/way), as per the audit
            elif v.isdigit():
                #No need to worry, county is fully described as 5-digit FIPS code
                if len(v) == 5:
                    countyList = [fips.FIPS_to_Name('./2010_FIPSCodes.csv', v, state_name = None)]
                
                #Now we need to worry about what the state is so we can get the county
                else:
                    '''Does this node/way already have one (or more) entries for the state that will allow us to
                    determine the proper county to name, given a 3-digit county code?'''
                    stateFound = False #tracks identification of a state already recorded for the parent node/way
                    for row in parsed_singleTag_data:
                        if 'state' in row and not stateFound:
                            #row[2] corresponds to 'value' in our schema
                            countyList = [fips.FIPS_to_Name('./2010_FIPSCodes.csv', v, state_name = row[2])]
                            stateFound = True
                        #Is there more than one state associated with this node/way?
                        elif 'state' in row and stateFound:
                            countyList = ['Unidentifiable (FIPS ambiguity)']
                            
                    #Did we not find anything to put as the county, presumably due to a lack of state presence?
                    if not countyList:
                        county_fips_to_find = v
                        '''TODO: this variable isn't used until isState() returns True, so need to record county
                        name at that point'''
            
            #is there anything in countyList? If so, our work here is done!
            if countyList:
                #Build the tag_dict
                for county in countyList:
                    tag_dict = {}
                
                    tag_dict['id'] = parent_dict['id']
                    tag_dict['value'] = county
                
                    #For ease of later analysis, set everything to have tag key = "addr:postcode"
                    tag_dict['key'] = 'county'
                    tag_dict['type'] = 'addr'
                    
                    parsed_singleTag_data.append(tag_dict['id'],
                                    tag_dict['key'],
                                    tag_dict['value'],
                                    tag_dict['type'])
            
            #Don't forget about the states we (may have) found!
            if stateList:
                for state in stateList:
                    tag_dict = {}
                
                    tag_dict['id'] = parent_dict['id']
                    tag_dict['value'] = state
                
                    #For ease of later analysis, set everything to have tag key = "addr:postcode"
                    tag_dict['key'] = 'state'
                    tag_dict['type'] = 'addr'
                    
                    parsed_singleTag_data.append(tag_dict['id'],
                                    tag_dict['key'],
                                    tag_dict['value'],
                                    tag_dict['type'])
        
                
        ############ STATES ############

        elif audit.isState(elem):
            #TODO: if parentdict ID is 398603731, set state to WV (it's currently CA)
            
            #TODO: record the state as addr:state, after passing it through the name transformer
            
            
            #TODO: find the state name using a 2-digit FIPS
            
            
            #TODO: check the county_fips_to_find variable and record county too if needed
        
        
        
        
        
        
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
        
    else: #This is scenario wherein tag key had problem characters in it, and we're skipping it
        pass
        
        
            
    ######    RETURN CORRECTED DATA    ######
    
    return parsed_singleTag_data, county_fips_to_find
    