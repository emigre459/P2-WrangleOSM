'''
Created on Jan 11, 2018

@author: emigre459

The purpose of this Python module is to do a lot of relatively simple auditson the map data. The more involved 
audits will be contained within their own dedicated modules (which right now is only StreetTypeAudit.py).

'''



import xml.etree.cElementTree as ET
import pprint
import re
from collections import defaultdict

#OSMFILE = "../data_sample_100_elemsWithTags_UTF-8Encoding.osm"
OSMFILE = "../SW_WestVirginia.osm"


def audit(osmfile, options=None):
    '''
    Audits the OSM file using the different audit functions defined herein.
    
    osm_file: str. Filepath to the OSM file being audited
    options: list of str. Dictates what types of audits are run. Allowed options values:
                        'counting'
                        'zips'
                        'county/state counting'
                        'county/state reporting'
                        'lat/long'
                        'amenities'
                        'property types'
                        'property type counts'
    
    '''
    
    with open(osmfile, "rb") as fileIn:
        if options:
            #Setting up the necessary beginning parameters for each function
            if 'counting' in options:
                tag_counts = {}
            if 'zips' in options:
                zipLength = 5
                zipLengthDict = {zipLength:0, "Non-number": 0}
                known_zips = set()
                knownZipTags = set()
                zip_tags_ignored = []
            if 'county/state counting' in options:
                county_tags = {}
                state_tags = {}
                state_tags_ignored = ['state_capital', 'source:hgv:state_network', 'hgv:state_network']
            if 'county/state reporting' in options:
                counties_found = set()
                states_found = set()
                countyKeys = ['gnis:County',
                              'gnis:County_num',
                              'gnis:county_id',
                              'gnis:county_name',
                              'is_in:county',
                              'tiger:county']
                stateKeys = ['addr:state',
                             'gnis:ST_alpha',
                             'gnis:state_id',
                             'nist:state_fips',
                             'ST_num']
            if 'lat/long' in options:
                badNodes = defaultdict(list) #ensures that each new key will automatically have an empty list value
            if 'amenities' in options:
                known_amenities=defaultdict(set)
            if 'property types' in options:
                propTypes = defaultdict(set)
            if 'property type counts' in options:
                propRecords = defaultdict(int)
                allowed_propTypes = {'landuse': ['residential',
                                                 'village_green',
                                                 'recreation_ground',
                                                 'allotments',
                                                 'commercial',
                                                 'depot',
                                                 'industrial',
                                                 'landfill',
                                                 'orchard',
                                                 'plant_nursery',
                                                 'port',
                                                 'quarry',
                                                 'retail'],
                                     'building': ['apartments',
                                                  'farm',
                                                  'house',
                                                  'detached',
                                                  'residential',
                                                  'dormitory',
                                                  'houseboat',
                                                  'bungalow',
                                                  'static_caravan',
                                                  'cabin',
                                                  'hotel',
                                                  'commercial',
                                                  'industrial',
                                                  'retail',
                                                  'warehouse',
                                                  'kiosk',
                                                  'hospital',
                                                  'stadium']
                                     }
                
    #----------------------------------------------------------------------
            #Iterating through the XML file
            for _, elem in ET.iterparse(fileIn):
                if 'counting' in options:
                    tag_counts = count_tags(elem, tag_counts)
                
                if 'zips' in options:
                    zipLengthDict, known_zips, knownZipTags = zipCheck(elem, zipLengthDict, known_zips, \
                                                                       knownZipTags, zip_tags_ignored, digits=zipLength)
                
                if 'county/state counting' in options:
                    county_tags, state_tags = countyStateTypeCounter(elem, county_tags, state_tags, state_tags_ignored)
                    
                if 'county/state reporting' in options:
                    counties_found, states_found = countyStateReporter(elem, countyKeys, stateKeys, counties_found, states_found)
                    
                if 'lat/long' in options:
                    badNodes = lat_long_checker(elem, badNodes)
                    
                if 'amenities' in options:
                    known_amenities = amenityFinder(elem, known_amenities)
                    
                if 'property types' in options:
                    propTypes = propertyType(elem, propTypes)
                
                if 'property type counts' in options:
                    propRecords = propertyCounter(elem, allowed_propTypes, propRecords)
    
    #----------------------------------------------------------------------    
            #printing everything once done iterating
            if 'counting' in options:
                print("Tags Found")
                pprint.pprint(tag_counts)
            if 'zips' in options:
                print("\nZip Lengths")
                pprint.pprint(zipLengthDict) 
                print("\nUnique Zip Codes")
                pprint.pprint(known_zips)
                print("\nZip Code Tag Keys Found")
                pprint.pprint(knownZipTags)
            if 'county/state counting' in options:
                print("\nTypes of County Tags")
                pprint.pprint(county_tags)
                print("\nTypes of State Tags")
                pprint.pprint(state_tags)
            if 'county/state reporting' in options:
                print("\nStates Identified")
                pprint.pprint(states_found)
                print("\nCounties Identified")
                pprint.pprint(counties_found)
            if 'lat/long' in options:
                print("\nNodes with Incorrect Latitudes and/or Longitudes")
                pprint.pprint(badNodes)
            if 'amenities' in options:
                print("\nUnique Amenity and Shop Types Identified")
                pprint.pprint(known_amenities)
            if 'property types' in options:
                print("\nUnique Landuse Types")
                pprint.pprint(propTypes)                
            if 'property type counts' in options:
                print("\nCounts of Relevant Landuse Types")
                pprint.pprint(propRecords)
    
    

def count_tags(elem, tag_dict):
    '''
    Looks at how many tags in the OSM sample file there are, binning them by unique tag type 
    with tag type as the key and the count found of each tag type as the value.
    
    Reference: This code is taken from the Udacity Data Analyst Nanodegree case study of OpenStreetMap data
    
    elem: ET element.
    tag_dict: dict. Lists all of the tag types seen so far as keys and their frequency of observation as values.
    
    Return: dict. Keys are tag type strings and values are the integer count of that tag type
    '''
    
    if elem.tag not in tag_dict.keys():
        tag_dict[elem.tag] = 1
    else:
        tag_dict[elem.tag] += 1
    
    return tag_dict


def zipCheck(elem, zip_length_dict={}, knownZips=set(), known_zip_tags=set(), tagsToIgnore = [], digits = 5):
    '''
    Checks all of the zip/postal codes contained in an OSM file and counts how many digits are included 
    in each code and then compares that count to a predefined number of digits. Returns a tuple:
    (the different lengths of zip codes it finds as a dict of the same format as zip_length_dict,
    a set of str that represents all of the unique zip codes it has discovered,
    a set of the tag keys it has identified through regex that it thinks describe zip codes)
    
    elem: ET element.
    zip_length_dict: dict containing lengths (keys) of different zip codes discovered and counts how many times
                        that length is observed (values)
    knownZips: set of str. Reports the different zip code values observed.
    known_zip_tags: set of str. Reports the tag keys found that are being observed
    digits: int. Number of digits expected for a zip code
    tagsToIgnore: list of str. Lists tag keys that are considered not useful for this analysis and thus shouldn't
                    have their values (nor tag keys) reported
    
    Returns: dict. Keys are zip code type identifiers (primarily digit counts) and values are the number of those
                    type identified.
    '''
    #makes sure any tag key with the text 'zip' or 'postcode' is accounted for
    zip_re = re.compile('postcode|zip',re.IGNORECASE)  # @UndefinedVariable
    
    
    if elem.tag == "node" or elem.tag == "way":
        nodeID = elem.attrib['id']
        for tag in elem.iter("tag"):
            if nodeID == '2625119248':
                print("WV zip - tag key=value is {}={}".format(tag.attrib['k'],tag.attrib['v']))
            zip_match = zip_re.search(tag.attrib['k'])
            if zip_match is not None and tag.attrib['k'] not in tagsToIgnore:
                tempZip = tag.attrib['v'].strip()
                knownZips.add(tempZip)
                known_zip_tags.add(tag.attrib['k'])
                
                #check to see if tempZip only has numbers in it
                if tempZip.isdigit():                    
                    if len(tempZip) in zip_length_dict.keys():
                        zip_length_dict[len(tempZip)] += 1                        
                    else:
                        zip_length_dict[len(tempZip)] = 1
                else:
                    '''TODO: leave the code below this out from here and include in CSV export code instead.
                    TODO: Also, need code for picking out only the first 5 digits (after letters are stripped)'''
                    #tempZip = re.sub("\D", "", tempZip) #replaces every non-digit char in tempZip with ""
                    
                    #print("Found a zip code with more than numbers!")
                    zip_length_dict["Non-number"] += 1
                    
                    print("Non-standard zip - ID#{}, tag key = {}, zip = {}".format(nodeID, tag.attrib['k'], tempZip))
                
                

    return zip_length_dict, knownZips, known_zip_tags

def countyStateTypeCounter(elem, county_types={}, state_types={}, tags_to_ignore=[]):
    '''
    Checks for the presence of a tag with 'county' included in the key and returns a dict with that tag
    key recorded as a key in the dict, mapped to a value representing the frequency that key is observed
    
    elem: ET element.
    county_types: dict. Keeps track of the different county-related tags identified (keys) and counts how often they
                    appear (value).
    state_types: dict. Keeps track of the different state-related tags identified (keys) and counts how often they
                    appear (value).
    tags_to_ignore: list of str. Identifies those county- or state-related tags that don't seem relevant to our
                        wrangling activities (e.g. we're interested in identifying all the states recorded in here,
                        but the '' key doesn't necessarily provide this info
    '''
    county_re = re.compile('county',re.IGNORECASE)  # @UndefinedVariable
    state_re = re.compile('state',re.IGNORECASE)  # @UndefinedVariable
    state_re_2 = re.compile('ST')
    
    if elem.tag == "node" or elem.tag == "way":
        for tag in elem.iter("tag"):
            if tag.attrib['k'] in tags_to_ignore:
                pass
            else:
                cty_match = county_re.search(tag.attrib['k'])
                state_match = state_re.search(tag.attrib['k'])
                state_match_2 = state_re_2.search(tag.attrib['k'])
                if cty_match is not None: 
                    if tag.attrib['k'] not in county_types.keys():
                        county_types[tag.attrib['k']] = 1
                    else:
                        county_types[tag.attrib['k']] += 1
                elif state_match is not None:
                    if tag.attrib['k'] not in state_types.keys():
                        state_types[tag.attrib['k']] = 1
                    else:
                        state_types[tag.attrib['k']] += 1
                #Special case wherein use of regex is non-obvious
                elif state_match_2 is not None:
                    if tag.attrib['k'] not in state_types.keys():
                        state_types[tag.attrib['k']] = 1
                    else:
                        state_types[tag.attrib['k']] += 1
                
                
    return county_types, state_types

def countyStateReporter(elem, county_keys, state_keys, counties=set(), states=set()):
    '''
    Returns the list of unique counties and states discovered in the OSM file as a tuple in the
    form (counties,states).
    
    elem: ET element.
    county_keys: list of str. Indicates what OSM tag keys have values that are of interest for county reporting.
    state_keys: list of str. Indicates what OSM tag keys have values that are of interest for state reporting.
    counties: set of str. Unique county names.
    states: set of str. Unique state names.
    '''
    
    tempState = ""
    tempCty = ""
    
    
    if elem.tag == "node" or elem.tag == "way":
        nodeID = elem.attrib['id']
        
        for tag in elem.iter("tag"):
            if nodeID == '398603731':
                print("CA tag - k:{}, v:{}".format(tag.attrib['k'],tag.attrib['v'])) 
                        
            if tag.attrib['k'] in state_keys:                    
                tempState = tag.attrib['v']
                states.add(tempState)
            elif tag.attrib['k'] in county_keys:
                tempCty = tag.attrib['v']
                counties.add(tempCty)
            
            
    
    return counties,states

def lat_long_checker(elem, badNodes=defaultdict(list), targetLatRange=[37.15,39.05], targetLongRange=[-82.67,-80.20]):
    '''
    Checks that the latitude and longitude of all nodes in the OSM file are within the bounds 
    expected for the region of interest. Returns a dict wherein the keys are node IDs and the value for each key
    is a list with up to 2 values, "Bad lon" and/or "Bad lat" to indicate what needs to be corrected.
    
    elem: ET element.
    badNodes: dict. Keys are integer node IDs, values are lists of strings indicating if the latitude and/or 
                    longitude are the problem(s) with the indicated node.
    targetLatRange: list of 2 double values; first value is the lower end of the range, 
                    second value is the upper end of the range, exclusive. This defines the allowed range
                    of values for node latitudes.
    targetLongRange: list of 2 double values, following the same format as targetLatRange. This defines the allowed
                        range of values for node longitudes.    
    '''
    if elem.tag == "node":
        if float(elem.attrib['lat']) < targetLatRange[0] or float(elem.attrib['lat']) > targetLatRange[1]:
            badNodes[elem.attrib['id']].append("Bad lat")
            
        
        if float(elem.attrib['lon']) < targetLongRange[0] or float(elem.attrib['lon']) > targetLongRange[1]:
            badNodes[elem.attrib['id']].append("Bad lon")
            
    return badNodes
    

def amenityFinder(elem, amenities=defaultdict(set)):
    '''
    Checks the element for the presence of an amenity, shop, or healthcare tag and returns 
    the input set + any new amenity types identified
    
    elem: ET element.
    amenities: dict with an empty set as the default value for each new key.
                Each key is a str describing a tag key type (e.g. 'amenity' or 'shop') 
                and each value is a list of the type that that key describes (e.g. 'amenity': set(graveyard, 
                cafe, etc.))
    '''
    
    allowed_tag_keys = ['amenity', 'shop', 'healthcare']
    
    if elem.tag == "node" or elem.tag == "way":
        for tag in elem.iter("tag"):
            if tag.attrib['k'] in allowed_tag_keys:
                amenities[tag.attrib['k']].add(tag.attrib['v'])
                
    return amenities

def propertyType(elem, types=defaultdict(set)):
    '''
    Searches through the OSM file's ways and node tags and determines what their landuse/building types are, 
    cataloging them as it goes. Returns a dict of sets that includes all of the unique landuse/building tag values
    it's found thus far, associated with either the key 'landuse' or 'building' as appropriate.
    
    elem: ET element.
    types: defaultdict(set). Includes all unique landuse and building types identified.
    '''
    
    if elem.tag == "way" or elem.tag == "node":
        for tag in elem.iter("tag"):
            if tag.attrib['k'] == 'landuse':
                types['landuse'].add(tag.attrib['v'])
            elif tag.attrib['k'] == 'building':
                types['building'].add(tag.attrib['v'])
                
    return types

def propertyCounter(elem, allowed_property_types, prop_records=defaultdict(int)):
    '''
    Counts how many ways and node tags exist that are of the type provided by allowed_property_types and returns
    an updated dict with the new count after inspecting a new elem.
    
    elem: ET element.
    allowed_property_types: dict of lists of str. Each list value should be equivalent to a landuse/building 
                            tag value (e.g. 'residential', 'commercial', etc.) and the keys should be "landuse"
                            and "building"
    prop_records: defaultdict(int). Each key value should be a str with a landuse/building tag value 
                    (e.g. 'residential', 'commercial', etc.) and each key's value should be an integer count of
                    how many times the corresponding landuse/building tag value has been observed.
    '''
    
    if elem.tag == "way" or elem.tag == "node":
        for tag in elem.iter("tag"):
            if tag.attrib['k'] == 'landuse' and tag.attrib['v'] in allowed_property_types['landuse']:
                prop_records["landuse:" + tag.attrib['v']] += 1
            elif tag.attrib['k'] == 'building' and tag.attrib['v'] in allowed_property_types['building']:
                prop_records["building:" + tag.attrib['v']] += 1
    
    
    return prop_records
    

#---------------------------------------------
#Main code execution space

#audit(OSMFILE, options=['zips', 'county/state counting', 'county/state reporting'])

#Unused options: ['counting', 'lat/long', 'amenities', 'property types', 'property type counts']