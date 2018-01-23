'''
Created on Jan 21, 2018

@author: emigre459

This module uses a CSV data file download from the US Census Bureau for 2010 FIPS codes to take a FIPS code for a
state or county and return the name of that state or county.
'''
import pprint as pp
import pandas as pd

#CSV columns organized as: StateName (2-letter); State_FIPS (2-digit); County_FIPS (3-digit); County_Name; FIPS_Class
#NOTE: the full FIPS code for a county is structured with the State_FIPS followed by County_FIPS, making a 5-digit #

'''
FIPS Class Codes
H1:  identifies an active county or statistically equivalent entity that does not qualify under subclass C7 or H6.
H4:  identifies a legally defined inactive or nonfunctioning county or statistically equivalent entity that does 
    not qualify under subclass H6.
H5:  identifies census areas in Alaska, a statistical county equivalent entity.
H6:  identifies a county or statistically equivalent entity that is areally coextensive or governmentally 
    consolidated with an incorporated place, part of an incorporated place, or a consolidated city. 
C7:  identifies an incorporated place that is an independent city; that is, it also serves as a county 
    equivalent because it is not part of any county, and a minor civil division (MCD) equivalent because 
    it is not part of any MCD.
''' 

#NOTE: it's possible that there will be a string encoding issue when you pass the names back to the 
#correction and exporting algorithm. If necessary, use encoding='utf-8' as an arg
census_df = pd.read_csv('../2010_FIPSCodes.csv', header=None, dtype = 'str', names=['State_Name',
                                                                                    'State_FIPS',
                                                                                    'County_FIPS',
                                                                                    'County_Name',
                                                                                    'FIPS_Class_Code'])

print(census_df.head())

def FIPS_to_Name(FIPS_code, state_name=None, state_FIPS=None):
    '''
    Takes a FIPS code for a state or county and returns a tuple of the form (county_name,state_name)
    
    FIPS_code: str. FIPS code (including leading zeroes) of a state or county. 
                    The true, most unique FIPS code for a county
                    is a 5-digit code in the format SSCCC where S = state digit and C = county digit. However,
                    it is common in OpenStreetMap for counties to only have 3 digits, with the state being inferred
                    from GIS context. It is assumed if the FIPS code is 2 digits long that it is a state code,
                    3 digits is a county code WITHOUT state specification, and 5 digits is a county code
                    WITH state specification.
    state_name: str. This is the two-letter representation of a state. Use this arg if you expect to be providing
                a 3-digit county code and therefore need to provide the state as a reference.
    state_FIPS: str. This is the two-digit FIPS code for a state. Use this arg if you expect to be providing
                a 3-digit county code and therefore need to provide the state as a reference.
    '''
    
    digits = len(FIPS_code)
    if digits == 2:
        #need to return row index of the first row wherein FIPS matches the code given
        census_df_oneState = census_df[census_df['State_FIPS'] == FIPS_code]
        return census_df_oneState['State_Name'].tolist()[0]
    
    #must have a state name or FIPS code in order to pull the county with only 3 digits available
    elif digits == 3 and (state_name or state_FIPS):
        if state_name:
            census_df_oneState = census_df[census_df['State_Name'] == state_name]
        else:
            census_df_oneState = census_df[census_df['State_FIPS'] == FIPS_code]
            
        census_df_oneCounty = census_df_oneState[census_df_oneState['County_FIPS'] == FIPS_code]
        return census_df_oneCounty['County_Name'].values[0]
        
    elif digits == 5:
        #TODO: just use the approach in the preceding elif statement, but extract state code from first 2 digits first
        census_df_oneState = census_df[census_df['State_FIPS'] == FIPS_code[:2]]
        census_df_oneCounty = census_df_oneState[census_df_oneState['County_FIPS'] == FIPS_code[2:]]
        
        return census_df_oneCounty['County_Name'].values[0]
    
    else:
        return None
    
print("\n")
#should give "Braxton County"
print(FIPS_to_Name('01009'))