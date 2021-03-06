'''
Created on Nov 12, 2017

@author: emigre459
'''

#This code is taken from the Udacity Data Analyst Nanodegree project page for Project #2: Wrangle OpenStreetMap Data

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

k = 100 # Parameter: take every k-th top level element

OSM_FILE = "../SW_WestVirginia.osm"
SAMPLE_FILE = "../data_sample_"+ str(k) + "_elemsWithTags_UTF-8Encoding.osm"
#SAMPLE_FILE = "../SW_WestVirginia_ASCIIEncoded.osm"

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


with open(SAMPLE_FILE, 'wb') as output:
    output.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write(b'<osm>\n  ')

    
    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:                              
            output.write(ET.tostring(element, encoding="utf-8"))

    output.write(b'</osm>')