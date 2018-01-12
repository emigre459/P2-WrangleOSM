'''
Created on Jan 11, 2018

@author: emigre459
'''



import xml.etree.cElementTree as ET
import pprint
import re

OSMFILE = "../data_sample_100_elemsWithTags.osm"


def audit(osmfile, options=None):
    '''
    Audits the OSM file using the different audit functions defined herein.
    
    osm_file: str. Filepath to the OSM file being audited
    options: 
    
    Returns: 
    '''
    
    with open(osmfile, "r") as fileIn:
        if options:
            #Setting up the necessary beginning parameters for each function
            if 'counting' in options:
                tag_counts = {}
            if 'zips' in options:
                zipLength = 5
                zipDict = {zipLength:0, "Non-number": 0}
            
            for _, elem in ET.iterparse(fileIn):
                if 'counting' in options:
                    tag_counts = count_tags(elem, tag_counts)
                
                if 'zips' in options:
                    zipDict = zipCheck(elem, zipDict, digits=zipLength)
        
            #print everything once done iterating
            if 'counting' in options:
                print("Tags Found")
                pprint.pprint(tag_counts)
            if 'zips' in options:
                print("\nZips")
                pprint.pprint(zipDict) 
    
    
    
    

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


def zipCheck(elem, zip_dict, digits = 5):
    '''
    Checks all of the zip/postal codes contained in an OSM file and counts how many digits are included 
    in each code and then compares that count to a predefined number of digits. Returns the different lengths
    of zip codes it finds. NOTE: this assumes that zip codes are only found as the value of the child tag key
    "addr:postcode"
    
    elem:    ET element.
    digits:     int. Number of digits expected for a zip code
    
    Returns: dict. Keys are zip code type identifiers (primarily digit counts) and values are the number of those
                    type identified.
    '''
    
    if elem.tag == "node" or elem.tag == "way":
        for tag in elem.iter("tag"):
            if tag.attrib['k'] == "addr:postcode":
                tempZip = tag.attrib['v']
                
                #check to see if tempZip only has numbers in it
                if tempZip.isdigit():
                    if len(tempZip) in zip_dict.keys():
                        zip_dict[len(tempZip)] += 1
                    else:
                        zip_dict[len(tempZip)] = 1
                else:
                    tempZip = re.sub("\D", "", tempZip) #replaces every non-digit char in tempZip with ""
                    print("Found a zip code with more than numbers!")
                    zip_dict["Non-number"] += 1

    return zip_dict 

audit(OSMFILE, options=['counting', 'zips'])
