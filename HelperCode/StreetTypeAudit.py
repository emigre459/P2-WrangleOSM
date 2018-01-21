'''
Created on Dec 28, 2017

@author: emigre459

This code parses through a given OSM data file and compares the list of expected street types (e.g. Road, Avenue,
etc.) to the street types identified through regex searches (which may turn up Rd, Rd., Av, Ave, etc.) and reports
back on what it found so you may iteratively build up a list of known abbreviations/mistakes and then modify
those data points as needed to make them all consistent.

The goal here is to make it so you can easily do summary statistics on all nodes of type "Road" without worrying
about missing ones that are of type "Rd"

This code is taken from the Udacity Data Analyst Nanodegree case study of OpenStreetMap data
'''
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

#OSMFILE = "../data_sample_100_elemsWithTags.osm"
OSMFILE = "../SW_WestVirginia.osm"
#comment code at end of next line needed to ignore false Eclipse Undefined Variable error for re.IGNORECASE
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)  # @UndefinedVariable


#This is the list of street type words we DO want to end up with everywhere
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Circle", "Terrace", "Way", "Alley", "Ridge", "Turnpike",
            "Pass", "Pike", "Branch", "Crossing", "Creek", "Highway", "Plaza", "Center", "Mall", "Run",
            "Hill", "Village"]

# This is the list of non-ideal street type words that we want to map to the ideal, expected types,
# generated iteratively by running this code and updating this list (manually) until no new versions are discovered
end_mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "Rd": "Road",
            "Rd.": "Road",
            "Rd,": "Road",
            "RD": "Road",
            "Dr.": "Drive",
            "Dr": "Drive",
            "Ln": "Lane",
            "Avevue": "Avenue",
            "Driver": "Drive",
            "Hwy": "Highway",
            "HWY": "Highway",
            "Ctr.": "Center",
            "hill": "Hill"
            }

'''TODO: make sure the auditing for the starting stuff basically replaces anything in the beginning that isn't a number
with the value from this dict (e.g. "US--60" should become "US Highway 60")'''
#These mappings are imperfect, given that "State Route" doesn't specify the state in question
#TODO: if available, have it pull the value of addr:state and throw that in front of "State Highway"
start_mapping = {"County Rd": "County Road",
                 "State Route": "State Highway",
                 "WV": "West Virginia Highway",
                 "US": "US Highway",
                 "KY": "Kentucky Highway",
                 "US Route": "US Highway"                 
                 }

to_be_mapped = set()

def mapping_check(to_be_mapped, street_type):
    '''
    Tests to see if the street type of street_name is already a key in the mapping variable. If it isn't,
    adds that street type string to to_be_mapped list
    
    to_be_mapped: set(str). String values are non-ideal street types that are not yet keys in mapping
    street_type: str. Street type whose presence in mapping is to be checked.
    
    Returns: set(str). Updated (if appropriate) to_be_mapped variable
    '''
    
    if street_type not in mapping:
        to_be_mapped.add(street_type)
    
    return to_be_mapped


def audit_street_type(street_types, street_name):
    '''
    Checks to see if a given street name contains a street type that is considered ideal.
    If it doesn't, then that street name is added to the street_types input set.
    
    street_types:    dict of sets. Each key is a street type string (e.g. 'St.') and each key's value
                     is a set containing all street names that use that exact street type string
                     (e.g. set(
    street_name:    str. Contains the name of the street in the 'addr:street' tag value
    '''
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        #Look to see if the street type string identified by the regex is non-ideal
        #Basically this just ignores any street names that are already idealized
        if street_type not in expected:
            #print("Found one!")
            street_types[street_type].add(street_name)
            mapping_check(to_be_mapped, street_type)


def is_street_name(elem):
    '''
    Determines if the element input is actually a street name
    
    elem: ET element.
    
    Returns: boolean. Represents status of elem as a street name or not (True or False, resp.)
    '''
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    '''
    Audits the OSM file to determine what street addresses need to be updated to have the ideal street types
    
    osmfile: str. Filepath to the OSM file being audited
    
    Returns: dict of sets. EXAMPLE: {'Ave': set(['N. Lincoln Ave', 'North Lincoln Ave']),
                                    'Rd.': set(['Baldwin Rd.']),
                                    'St.': set(['West Lexington St.'])}
    '''
    osm_file = open(osmfile, "rb")
    street_types = defaultdict(set)
    
    #parses through the XML file provided and pulls out the tags iteratively
    for _, elem in ET.iterparse(osm_file, events=("start",)):
        #elem.iter("tag") here parses through the child-level tags of elem and builds an iterable out of those of
            #'tag' type, if the parent tag is 'node' or 'way' type and audits that child tag if it is a street name
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types


def update_name(name, mapping, printUpdate = True):
    '''
    For those street names that have non-ideal street types, this returns a version of that street name
    with an ideal street type
    
    name: str. original street name
    mapping: dict. Keys are non-ideal street type strings, with values that are each key's ideal 
                street type string
    printUpdate: bool. If set to True, outputs a print statement showing the old and new street names
    
    Returns: str. Same as name input, but with idealized street type
    '''
    
    #First we need to make sure we're only looking at the end of the street name
    #(in other words, the last 'word' in the string that comprises the address, such as 
    #101 Braddock Rd., pulling out "Rd." for that example) - name_list[-1] achieves this
    name_list = name.split(" ")
    newName = None
        
    #Now let's update the street name if the street type exists in our mapping dict
    if name_list[-1] in mapping.keys():
        newName = " ".join(name_list[:-1]) + " " + mapping[name_list[-1]]
        
    if printUpdate:
        outputStr = "{} => {}".format(name, newName)
        print(outputStr)
    
    return newName



''''This section of code goes through the OSM file, audits it, provides the dict of street types which we need to
check for any non-ideal street types not already captured by our mapping variable, then updates the names it
can, using the existing mapping variable'''


#Building the street_types dict and checking it out
st_types = audit(OSMFILE)
pprint.pprint(dict(st_types))
print("\nNeed to map these:")
pprint.pprint(to_be_mapped)

#We'll now go through each set in the st_types dict and update the street names to have idealized street types
for st_type, st_names in st_types.items():
        for name in st_names:
            better_name = update_name(name, mapping)




