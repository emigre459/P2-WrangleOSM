'''
Created on Nov 12, 2017

@author: emigre459
'''

#This code is taken from the Udacity Data Analyst Nanodegree project page for Project #2: Wrangle OpenStreetMap Data

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

k = 1 # Parameter: take every k-th top level element

OSM_FILE = "../SW_WestVirginia.osm"
#SAMPLE_FILE = "../data_sample_"+ str(k) + "_elemsWithTags.osm"
SAMPLE_FILE = "../SW_WestVirginia_ASCIIEncoded.osm"

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags and list(elem.iter(tag = "tag")) != []:
            #the last piece of the preceding conditional filters out any nodes/ways/relations 
            #that don't have any children of 'tag' type, for clarity of reading the sampled data
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'w') as output:
    output.write('<?xml version="1.0" encoding="us-ascii"?>\n')
    output.write('<osm>\n  ')

    
    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            
            ''''NOTE: I was receiving an encoding error regarding unrecognized symbols when trying to 
            use the ET.tostring() method, so the only way I could find to fix it is by encoding in ascii
            and then decoding with the 'ignore' option to ignore any characters that aren't included in the codec
            This is not ideal, as some information is likely lost in doing so, but it is likely not a big loss as
            the map data are for the United States, so there should be no meaningful data represented by non-ascii
            characters'''
            encoded_str = ET.tostring(element, encoding="us-ascii").decode('ascii', 'ignore')          
            output.write(encoded_str)

    output.write('</osm>')