## This script will grab population and language data from Census Bureau ACS API and output to Geodatabase of user's choice. Designed as a ArcGIS script tool.
## Developed by Kayne Neigherbauer for Wisconsin Emergency Management Sept 2017

#import modules
try:
    import sys, urllib2, json, string, csv, arcpy, os, socket
    from datetime import date
except ImportError:
    sys.exit("Error importing 1 or more required modules.")

def main():

    def fetchData(url):
        arcpy.AddMessage("Fetching Data...")
        try:
            response = urllib2.urlopen(url)
        except urllib2.HTTPError as e:
            arcpy.AddMessage("HTTP Error: " + str(e.code))
            return None
        except urllib2.URLError as e:
            arcpy.AddMessage("URL Error: " + str(e.reason))
            return None
        else:
            code = response.getcode()
            if code == 200:
                # if success (200) then read the data
                data = json.load(response)
                if data == []:
                    data = None
                #print json.dumps(data, indent =2)
                return data
            else:
                return None
     
    def joinData(data,geom,path,clip):
        
        #create temp data scratch GDB
        tempData = arcpy.CreateScratchName(workspace=arcpy.env.scratchGDB)
        arcpy.env.scratchWorkspace = tempData
        arcpy.env.overwriteOutput = True
        tempPath = arcpy.GetSystemEnvironment("TEMP") 
        ## some variables ##
        clipped = "clipped"
        tracts = "tracts"
        ##update_file = "WEMAPP.WEM$OWN.test_census"
        tracts_clip = "tracts_clip"
        final_tracts = "final"
        csv_file = os.path.join(tempPath,'languages.csv')
        clipJSON = os.path.join(tempPath,'clip.json')
        
        #open and write data to file as csv with a header row
        with open(os.path.join(csv_file), 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(data.pop(0)) #header row
            for item in data:
                #strip the excess long digits for GEOID
                item[1] = item[1].lstrip('14000US')
            writer.writerows(data)
        with open(clipJSON, 'wb') as f:
            json.dump(clip, f)

        arcpy.AddMessage("Processing Data...")
        fields = fieldsList
        if fields == ['B16001_001','EB16001_005E','B16001_020E','B16001_026E','B16001_068E','B16001_080E']:
            aliases = ["Total - 5+ yrs of age", "Spanish or Spanish Creole: Speak English less than 'very well'", "German: Speak English less than 'very well'", "Other West Germanic languages: Speak English less than 'very well'" , "Chinese: Speak English less than 'very well'" , "Hmong: Speak English less than 'very well'"]
        else:
            aliases = []
        # Geometry json to feature class
        #arcpy.JSONToFeatures_conversion(geomJSON, tracts)
        if arcpy.Exists(clipped):
            arcpy.Delete_management(clipped)
        arcpy.JSONToFeatures_conversion(os.path.join(tempPath,'clip.json'), clipped)
        # Convert csv to table
        if arcpy.Exists("csv_table"):
            arcpy.Delete_management("csv_table")
        arcpy.CopyRows_management(csv_file, "csv_table")
        # Add Field for string verison of GEOID
        arcpy.AddField_management("csv_table", "GID_TEXT", "TEXT")
        # Calculate the new Field (GEOID)
        arcpy.CalculateField_management("csv_table", "GID_TEXT", "[GEO_ID]")
        # Process: Add Join - join the table to the layer
        arcpy.JoinField_management(geom, "GEOID", "csv_table", "GID_TEXT")
        # Copy features to new file - saves join.
        if arcpy.Exists("new_tracts"):
            arcpy.Delete_management("new_tracts")
        arcpy.CopyFeatures_management(geom, "new_tracts")
        #get rid of text version of GEOID field
        arcpy.DeleteField_management("new_tracts", ["GID_TEXT"])
        #Change field names and aliases.
        fList = arcpy.ListFields("new_tracts")
        for f in fList:     
            for i in range(0,len(fields)):
                if f.name == fields[i]:
                    arcpy.AlterField_management("new_tracts",f.name, fields[i], aliases[i])
                    break
        # Clip the census tracts to wi boundary
        if arcpy.Exists(tracts_clip):
            arcpy.Delete_management(tracts_clip)
        arcpy.Clip_analysis("new_tracts", clipped, tracts_clip)
        # Copy to save field names
        if arcpy.Exists(final_tracts):
            arcpy.Delete_management(final_tracts)
        arcpy.AddMessage("Writing File...")
        arcpy.CopyFeatures_management(tracts_clip, final_tracts)
        # Cleanup temp files
        arcpy.Delete_management("new_tracts")
        arcpy.Delete_management(tracts_clip)
        arcpy.Delete_management("csv_table")
        arcpy.Delete_management(csv_file)
        arcpy.Delete_management(tracts)
        arcpy.Delete_management(clipped)
        arcpy.Delete_management(clipJSON)

        #set up cursors to update dataset feature class
        sfc = final_tracts #search cursor feature class
        ufc = str(os.path.join(dbpath))#, update_file)) #update cursor feature class
        with arcpy.da.SearchCursor(sfc, '*') as sCur:
            with arcpy.da.UpdateCursor(ufc, '*') as uCur:
                for sRow in sCur:
                    for uRow in uCur:
                        if sRow[1] == uRow[1]:
                            uRow = [sRow]
                            uCur.updateRow(uRow)
                            break
        # Cleanup last temp file
        arcpy.Delete_management(final_tracts)
        
        arcpy.AddMessage("Census data updated.")
        
            
    #set timeout for web requests
    timeout = 10
    socket.setdefaulttimeout(timeout)
    
    # path of workspace dataset
    global dbpath
    dbpath = (arcpy.GetParameterAsText(0))
    geom = (arcpy.GetParameterAsText(1))
    
    # current year
    global year
    year = date.today().year
    global acsYear
    acsYear = arcpy.GetParameterAsText(4)
    
    ## census api key
    key = (arcpy.GetParameterAsText(2)) 
    ##census fields from user input
    fields = arcpy.GetParameterAsText(3)  ##B16001_001E,B16001_005E,B16001_020E,B16001_026E,B16001_068E,B16001_080E
    global fieldsList
    fieldsList = fields.split(';')
    fieldsStr = string.join(fieldsList,',')
    arcpy.AddMessage(fieldsStr)
    arcpy.AddMessage(fieldsList)

    fcFieldList = arcpy.ListFields(dbpath)
    deleteList = []
    for f in fcFieldList:
        if "_" in f.name[(len(f.name)-2):len(f.name)]:
            deleteList.append(f.name)
    try:
        arcpy.DeleteField_management(dbpath,deleteList)
        arcpy.AddMessage("Deleted Fields: ",deleteList)
    except Exception as e:
        arcpy.AddMessage(e)
    ### call functions ###
    # get data from ACS REST ENDs
    url = "https://api.census.gov/data/" + str(acsYear) + "/acs/acs5?get=NAME,GEO_ID," + fields + "&for=tract:*&in=state:55&key=" + key
    arcpy.AddMessage(url)
    #go get it
    data = fetchData(url)
##    while acsYear >= 2015:
##        # url for 2015 ACS 5yr estimate for:
##        # Language spoken at home by ability to speak english less than well, specific languages
##        url = "https://api.census.gov/data/" + str(acsYear) + "/acs/acs5?get=NAME,GEO_ID," + fields + "&for=tract:*&in=state:55&key=" + key
##        arcpy.AddMessage(url)
##        #go get it
##        data = fetchData(url)
##        #try year before this one if no data
##        if data == None:
##            acsYear -= 1
##        else:
##            break

    # State boundary url from DMA public end point
    clipUrl = "https://widmamaps.us/dma/rest/services/WEM/WI_State_Boundary/MapServer/0/query?where=STATE_FIPS+%3D+55&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&returnGeometry=true&f=pjson"
    #go get it
    clip = fetchData(clipUrl)
    
    # join csv data with census tract layer geometry
    joinData(data,geom,dbpath,clip)
    


if __name__ == "__main__":
    main()
