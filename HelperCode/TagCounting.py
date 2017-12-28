'''
Created on Dec 28, 2017

@author: emigre459
'''

#This code is taken from the Udacity Data Analyst Nanodegree case study of OpenStreetMap data
#count_tags() looks at how many tags in the OSM sample file there are, binning them by tag type

import xml.etree.cElementTree as ET
import pprint

def count_tags(filename):
    tag_dict = {}
    osm_file = open(filename, "r")
    for event, elem in ET.iterparse(osm_file):
        if elem.tag not in tag_dict.keys():
            tag_dict[elem.tag] = 1
        else:
            tag_dict[elem.tag] += 1
    
    return tag_dict

tags = count_tags("../data_sample.osm")
pprint.pprint(tags)