#--------------------------------#
# CANTS GEOCODE AND SPATIAL JOIN #
# Ian Matthew Morey              #
#--------------------------------#

import arcpy, os


# # # # # SET WORKSPACE
myMainPath = r"C:\\Geoprocessing\\Work\\"
myAddLPath = r"C:\\Geoprocessing\\Addr_Loc\\"
myDataPath = myMainPath + "CANTS\\"
myGeocPath = myDataPath + "Geocode\\"
myJoinPath = myDataPath + "Joins\\"


# # # # # GEOCODE OBSERVATIONS
runGeocode = 1

if runGeocode == 1:
    print "GEOCODING ADDRESSES"

    addrTable = myDataPath + "cants_geoin_201512.dbf"
    addrLocat = myAddLPath + "USA_2011R2_NT_StreetAddress"
    geoResult = myGeocPath + "geocode_result"

    arcpy.GeocodeAddresses_geocoding(addrTable, addrLocat, "Street STREET_G VISIBLE NONE; City CITY_G VISIBLE NONE; State STATE_G VISIBLE NONE; ZIP ZIP_G VISIBLE NONE", geoResult)


# # # # # DICTIONARY OF SPATIAL JOINS
# universal to all joins
trgtFeat = myGeocPath + "geocode_result.shp"
joinOper = "JOIN_ONE_TO_ONE"
joinType = "KEEP_ALL"

# dictionary of unique spatial join fields 
# joinDict = [({RUN: 1 or 0}, {MESSAGE: character string}, {SHAPEFILE: path and file}, {OUTPUT: character string})] 
joinDict = [(1, "JOINING XY TO 2010 MCD", "Shapefiles_IL\\MCD2010\\tl_2010_17_cousub10.shp", "MCD2010_Join_Output"),
            (1, "JOINING XY TO PLACE", "Shapefiles_IL\\Place2010\\tl_2012_17_place.shp", "Place_Join_Output"),
            (0, "JOINING XY TO LAN", "Shapefiles_IL\\LANS_IDCFS\\IDCFS_LANS.shp", "LANS_Join_Output"),
            (0, "JOINING XY TO 2000 TRACTS", "Shapefiles_IL\\Tract2000\\tl_2010_17_tract00.shp", "Tract00_Join_Output"),
            (1, "JOINING XY TO 2010 TRACTS", "Shapefiles_IL\\Tract2010\\tl_2010_17_tract10.shp", "Tract10_Join_Output"),
            (1, "JOINING XY TO CCA", "Shapefiles_Chicago\\CommunityAreas\\CommAreas.shp", "CCA_Join_Output")] 


# # # # # JOIN LOOP
for n in range(0, len(joinDict)):
    if joinDict[n][0] == 1:
        print joinDict[n][1]
        joinFeat = myMainPath + joinDict[n][2]
        outFeatC = myJoinPath + joinDict[n][3]
        arcpy.SpatialJoin_analysis(trgtFeat, joinFeat, outFeatC, joinOper, joinType)
        
print "COMPLETED ALL WORK"
