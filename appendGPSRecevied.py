#-------------------------------------------------------------------------------
# Name:        Append GPS received into GDB
# Purpose:     This script is designed to take new meters that have passed through
#              Boomi and subsequently the GIS Geoprocessing script and ultimately 
#              loads it into a file geodatabase.  The processing of the script to
#              successfully complete this task includes copying the data over to 
#              a scratch DB, purging non-relevant records, dropping fields, 
#              renaming the remaining fields, projecting the data and finally 
#              appending the data to the awaiting file geodatabase.
# Author:      John Spence
#
# Created:     09/04/2019
# Modified:    09/07/2019
# Modification Purpose:
#              09/07/2019 - Complete code review (internal) and cleaned up messy
#                           code.  Expanding reporting capabilities along with 
#                           making several changes that would take the code from
#                           a 1-off solution to something that could be ported
#                           fairly fast to a new requirement.
#
#-------------------------------------------------------------------------------

# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Pretty simple setup.  Just change your settings/configuration below.  Do not
# go below the "DO NOT UPDATE...." line.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Source DB connection
source_db_connection = r'Database Connections\\Connection.sde'

# Scratch DB connection
processing_db_connection = r'Database Connections\\UTIL.sde'

# Destination DB connection
destination_db_connection = r'\\888888888888888888\data\888888888888888\UtilitiesAssetMapping\data\gdb\Survey.gdb'

# Configure database update type here. (Prod, Stg, Test, Other)
db_type = 'Prod'

# Source Configurations
# Source Data Schema
sd_schema = 'UTIL'
# Source Dataset
sd_dataset = ''
# Source Data Table
sd_table = 'TOHOST2GIS_AMI'

# Target Configurations
# Target Data Schema
target_schema = ''
# Target Dataset
target_dataset = 'EPSG2926'
# Target Data Table
target_table = 'AMIReplacement2020'

# Lookback window...To be used ONLY during testing.  Set to 0 otherwise.
lookback = 0

# Projection settings
# Set projection WKID
pub_projectSRID = 2926

# Set Transformation Method to be used.
pub_transMethod = 'NAD_1983_HARN_To_WGS_1984_2'


# Send confirmation of rebuild to
email_target = ''

# Configure the e-mail server and other info here.
mail_server = ''
mail_from = ''

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import arcpy, time, smtplib, string, re, os
import pandas as pd
import datetime
from arcpy import env
from datetime import datetime

def check4udpate(source_db_connection, sd_schema, sd_table):

    global pending_update
    global update_count

 #   check_update_SQL = '''
 #   select [WorkOrderNumber] from [{0}].[{1}]
	#where convert(date, [SentToGIS_Date]) >= convert (date, getdate()-{2})
	#and [SentToGIS_Status] = 'Complete'
	#and FoundLatitude is not NULL
	#and FoundLongitude is not NULL
 #   '''.format(sd_schema, sd_table, lookback)

    check_update_SQL = '''
    select count(*) from [{0}].[{1}]
    where convert(date, [SentToGIS_Date]) >= convert (date, getdate()-{2})
    and [SentToGIS_Status] = 'Complete'
    '''.format(sd_schema, sd_table, lookback)

    check_update_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(check_update_SQL)

    #if check_update_return == True:
    #    pending_update = 0
    #elif check_update_return == None:
    #    pending_update = 0
    #else:
    #    pending_update = [row for row in check_update_return]
    #    pending_update = len(pending_update)

    if check_update_return == None:
        pending_update = 0
    else:
        pending_update = check_update_return

    # Build Connection String
    data_source = source_db_connection + '\\{0}.{1}'.format(sd_schema, sd_table)

    # Check record count
    update_count = int(arcpy.GetCount_management(data_source).getOutput(0))

    # Report out
    print ("**METER INFORMATION AT SOURCE DATABASE**")
    print ("   Total meters available at source: {0}".format(update_count))
    print ("   Total meter GPS records to be transferred:  {0}\n\n".format(pending_update))

    return pending_update

def prepData(source_db_connection, processing_db_connection, sd_schema, sd_dataset, sd_table, lookback, pub_projectSRID, pub_transMethod):

    # Begin processing of existing data
    print ("**PREPPING GPS DATA FOR INSERTION TO LONG TERM STORAGE**\n")

    # Prep data for insertion into long term storage.
    input_connection = processing_db_connection
    if sd_dataset != '':
        pub_layerfullname = '{0}.{1}.{2}'.format(sd_schema, sd_dataset, sd_table)
    else:
        pub_layerfullname = '{0}.{1}'.format(sd_schema, sd_table)

    # Check for existance in target DB
    check_for_existance(input_connection, pub_layerfullname)

    # Set connections for Extraction / Copy Over
    input_connection = source_db_connection
    output_connection = processing_db_connection

    # Copy the data from WebGIS to GISScratch
    copy_layer_over (input_connection, output_connection, pub_layerfullname)

    # Cleanup data in layer
    cleanup_layer (processing_db_connection, pub_layerfullname, sd_schema, sd_table, lookback)

    # Project layer to desired coordinate system
    project_layer (processing_db_connection, pub_layerfullname, pub_projectSRID, pub_transMethod)

    return

def check_for_existance(input_connection, pub_layerfullname):

    # Configure variables for use.
    conn_string = '{0}'.format(input_connection)
    print ("   Using {0}".format(input_connection))
    item_check = '{0}'.format(pub_layerfullname)
    print ("   Checking {0}\n".format (pub_layerfullname))

    # Get current database name
    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(input_connection).execute(check_db_sql)
    target_db = check_db_return
    print ("     Current Database:  {0}\n\n".format(target_db))

    # Set current workspace environment
    arcpy.env.workspace = input_connection

    # Check for existance at target database
    if arcpy.Exists(pub_layerfullname):

        print ("  Item exist at target.\n")

        # Delete Existing Layer
        try:
            print ("  Removing from target.....")
            delete_existing_layer(input_connection, target_db, pub_layerfullname)
        except:
            print ("  Can not delete.  Probably due to existing layer locks.")
        return

    else:

        print ("  Item does not exist at target.\n\n")

        return

def delete_existing_layer(input_connection, target_db, pub_layerfullname):

    # Configure variables for use.
    item_to_delete = '{0}\\{1}.{2}'.format(input_connection, target_db, pub_layerfullname)

    # Delete Existing Layer
    arcpy.Delete_management (item_to_delete)

    # Print Verification of Deletion
    print ("     {0} has been successfully deleted.\n\n".format (pub_layerfullname))

    return


def copy_layer_over (input_connection, output_connection, pub_layerfullname):

    print ("  Preparing to copy layer over for processing.....\n")

    print ("   Finding source DB...")

    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(input_connection).execute(check_db_sql)
    current_db = check_db_return

    print ("     Source Database:  {0}\n".format(current_db))

    print ("   Finding target DB...")
    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(output_connection).execute(check_db_sql)
    target_db = check_db_return
    print ("     Target Database:  {0}\n\n".format(target_db))

    # Configure Connections
    input_connection = input_connection + '\\' + current_db + '.' + pub_layerfullname
    output_connection = output_connection + '\\' + target_db + '.' + pub_layerfullname

    # Set workspace and keyword
    arcpy.env.workspace = output_connection
    arcpy.env.configKeyword= "Geometry"

    # Copy Over
    try:
        print ("  Copying {0} from {1} to {2}.....".format(pub_layerfullname, current_db, target_db))
        arcpy.Copy_management(input_connection, output_connection)
        print ("     {0} successfully copied to {1}\n\n".format(pub_layerfullname, target_db))

    except:
        print ("  FAILURE:  Layer has not been deleted.  Please remove lock and try again.\n\n")

    return

def cleanup_layer (processing_db_connection, pub_layerfullname, sd_schema, sd_table, lookback):

    print ("  Preparing to scrub layer of undesireable data and reformating.....\n")

    # Delete records that do not need to be added.
    print ("  Removing non-complete and NULL Lat/Long Record records...")
    try:
        clean_tbl_sql = '''
        delete from [{0}].[{1}]
        where convert(date, [SentToGIS_Date]) >= convert (date, getdate()-{2})
        and [SentToGIS_Status] <> 'Complete' 
        or ([FoundLatitude] is NULL and [SentToGIS_Status] = 'Complete')
        or ([FoundLongitude] is NULL and [SentToGIS_Status] = 'Complete')
        '''.format(sd_schema, sd_table, lookback)
        clean_tbl_return = arcpy.ArcSDESQLExecute(processing_db_connection).execute(clean_tbl_sql)
        print ("     Records removed.\n\n")

    except Exception as clean_tbl_return:
        print ("     FAILURE:  Data cleaning failed.\n\n")
        print (clean_tbl_return.args[0])

    # Delete records that do not need to be added.
    print ("  Removing NULL Record records...")
    try:
        clean_tbl_sql = '''
        delete from [{0}].[{1}]
        where [SentToGIS_Status] is NULL
        '''.format(sd_schema, sd_table)
        clean_tbl_return = arcpy.ArcSDESQLExecute(processing_db_connection).execute(clean_tbl_sql)
        print ("     Records removed.\n\n")

    except Exception as clean_tbl_return:
        print ("     FAILURE:  Data cleaning failed.\n\n")
        print (clean_tbl_return.args[0])

    print ("   Finding current DB...")
    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(processing_db_connection).execute(check_db_sql)
    target_db = check_db_return
    print ("     Current Database:  {0}\n\n".format(target_db))

    # Build Connection String
    output_connection = processing_db_connection + '\\{0}.{1}'.format(target_db, pub_layerfullname)

    # Prep for field removal.
    print ("   Disable Editor Tracking...")
    try:
        arcpy.DisableEditorTracking_management(output_connection,
                                           "DISABLE_CREATOR",
                                           "DISABLE_CREATION_DATE",
                                           "DISABLE_LAST_EDITOR",
                                           "DISABLE_LAST_EDIT_DATE")
        print ("     Success!\n\n")
    except Exception as editor_tracking_fail:
        print ("     FAILURE:  Failed to disable editor tracking.\n\n")
        print (editor_tracking_fail.args[0])

    print ("   Removing Fields.....")
    try:
        # Build list of fields to remove from FC.
        # Excluded fields from deletion.
        exclude = ["WorkOrderNumber", "FoundLatitude", "FoundLongitude", "FoundGPSPDOP", "FoundGPSHDOP", "FoundGPSVDOP", "FoundAltitude"]
        fullList = []
        fieldfullList = arcpy.ListFields(output_connection)

        print ("     Removing the following fields:")
        for field in fieldfullList:
             if not field.required:
                  if not field.name in exclude:
                       fullList.append(field.name)

        for field in fullList:
            print ("        {0}".format(field))

        arcpy.DeleteField_management(output_connection, fullList)
        print ("     Success!\n\n")

    except Exception as field_removal_fail:
        print ("     FAILURE:  Failed to remove fields!\n\n")
        print (field_removal_fail.args[0])


    print ("   Renaming Fields.....")
    print ("     Renaming WorkorderNumber to FacilityID")
    try: 
        arcpy.AlterField_management(output_connection, 'WorkOrderNumber', 'FacilityID')
        print ("     Success!\n")
    except Exception as field_rename_fail:
        print ("     FAILURE:  Failed to remove fields!\n\n")
        print (field_rename_fail.args[0])

    print ("     Renaming FoundLatitude to Latitude")
    try:  
        arcpy.AlterField_management(output_connection, 'FoundLatitude', 'Latitude')
        print ("     Success!\n")
    except Exception as field_rename_fail:
        print ("     FAILURE:  Failed to remove fields!\n\n")
        print (field_rename_fail.args[0])

    print ("     Renaming FoundLongitude to Longitude")
    try:  
        arcpy.AlterField_management(output_connection, 'FoundLongitude', 'Longitude')
        print ("     Success!\n")
    except Exception as field_rename_fail:
        print ("     FAILURE:  Failed to remove fields!\n\n")
        print (field_rename_fail.args[0])

    print ("     Renaming FoundGPSPDOP to PDOP")
    try:  
        arcpy.AlterField_management(output_connection, 'FoundGPSPDOP', 'PDOP')
        print ("     Success!\n")
    except Exception as field_rename_fail:
        print ("     FAILURE:  Failed to remove fields!\n\n")
        print (field_rename_fail.args[0])

    print ("     Renaming FoundGPSHDOP to HDOP")
    try:  
        arcpy.AlterField_management(output_connection, 'FoundGPSHDOP', 'HDOP')
        print ("     Success!\n")
    except Exception as field_rename_fail:
        print ("     FAILURE:  Failed to remove fields!\n\n")
        print (field_rename_fail.args[0])

    print ("     Renaming FoundGPSVDOP to VDOP")
    try:  
        arcpy.AlterField_management(output_connection, 'FoundGPSVDOP', 'VDOP')
        print ("     Success!\n")
    except Exception as field_rename_fail:
        print ("     FAILURE:  Failed to remove fields!\n\n")
        print (field_rename_fail.args[0])

    print ("     Renaming FoundAltitude to Altitude")
    try:  
        arcpy.AlterField_management(output_connection, 'FoundAltitude', 'Altitude')
        print ("     Success!\n")
    except Exception as field_rename_fail:
        print ("     FAILURE:  Failed to remove fields!\n\n")
        print (field_rename_fail.args[0])

    print ("  Layer scrub complete.....\n\n")

    return

def project_layer (processing_db_connection, pub_layerfullname, pub_projectSRID, pub_transMethod):

    pub_layerfullname_chk = pub_layerfullname + '_PR'
    input_connection = processing_db_connection
    
    print ("  Preparing to project layer.....\n")

    print ("   Finding DB...")

    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(input_connection).execute(check_db_sql)
    current_db = check_db_return

    print ("     Source Database:  {0}\n".format(current_db))

    check_for_existance(input_connection, pub_layerfullname_chk)

    input_connection = processing_db_connection + '\\' + current_db + '.' + pub_layerfullname
    output_connection = processing_db_connection + '\\' + current_db + '.' + pub_layerfullname + '_PR'

    out_coordinate_system = arcpy.SpatialReference(pub_projectSRID)

    print ("   Projecting Layer...")
    try:
        arcpy.Project_management(input_connection, output_connection, out_coordinate_system, pub_transMethod)  
        print ("     Success!\n\n")
    except Exception as projection_failure:
        print ("     FAILURE:  To project layer.\n\n")
        print (projection_failure.args[0])

    return

def loadData(destination_db_connection, processing_db_connection, sd_schema, sd_dataset, sd_table, target_dataset, target_table):
    
    # Begin processing of existing data
    print ("**LOADING GPS DATA TO LONG TERM STORAGE**\n")

    print ("  Preparing to load layer to storage.....\n")

    print ("   Finding DB...")

    check_db_sql = '''SELECT DB_NAME() AS [Database]'''
    check_db_return = arcpy.ArcSDESQLExecute(processing_db_connection).execute(check_db_sql)
    current_db = check_db_return

    print ("     Source Database:  {0}\n".format(current_db))

    if sd_dataset != '':
        pub_layerfullname = '{0}.{1}.{2}'.format(sd_schema, sd_dataset, sd_table)
    else:
        pub_layerfullname = '{0}.{1}'.format(sd_schema, sd_table)
  
    input_connection = processing_db_connection + '\\' + current_db + '.' + pub_layerfullname
    output_connection = destination_db_connection + '\\' + target_dataset + '\\' + target_table

    print ("   Loading to storage...")
    try:
        arcpy.Append_management(input_connection, output_connection, "NO_TEST")
        print ("     Success!\n\n")
        completion_status = 1
        failure_info = None
    except Exception as add_failure:
        print ("     FAILURE:  Loading data failed.\n\n")
        print (add_failure.args[0])
        failure_info = add_failure.args[0]
        completion_status = 0
    return completion_status, failure_info

def sendcompletetioninfo(email_target, mail_server, mail_from, completion_status, failure_info, pending_update):

    if completion_status == 1:
        mail_priority = '5'
        mail_subject = 'Success:  Meters GPS Data has successfully been transferred'
        mail_msg = ('{0} meters were transferred to long term storage.\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(pending_update))

    else:
        mail_priority = '1'
        mail_subject = 'Failure:  Meters GPS Data has failed'
        mail_msg = ('There was a failure to transfer all or some of the {0} meters.  '.format(pending_update))
        mail_msg = mail_msg + ('Review and attempt to run the script again.\n')
        mail_msg = mail_msg + ('Error:  {0}\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(failure_info))

    # Set SMTP Server and configuration of message.
    server = smtplib.SMTP(mail_server)
    email_target = email_target
    mail_priority = mail_priority
    mail_subject =  mail_subject
    mail_msg =  mail_msg

    send_mail = 'To: {0}\nFrom: {1}\nX-Priority: {2}\nSubject: {3}\n\n{4}'.format(email_target, mail_from, mail_priority, mail_subject, mail_msg)
    # Double commented out code hides how to send a BCC as well.
    send_mail = 'To: {0}\nFrom: {1}\nBCC: {2}\nX-Priority: {3}\nSubject: {4}\n\n{5}'.format(email_target, mail_from, mail_bcc, mail_priority, mail_subject, mail_msg)
    
    server.sendmail(mail_from, email_target, send_mail)

    server.quit()

    return

def sendcompletetion_noUpdates(email_target, mail_server, mail_from):
    mail_priority = '5'
    mail_subject = 'Success:  Process ran, but no new GPS data available.'
    mail_msg = 'The process successfully ran, but no new GPS data was available to transport.\n\n[SYSTEM AUTO GENERATED MESSAGE]'

    # Set SMTP Server and configuration of message.
    server = smtplib.SMTP(mail_server)
    email_target = email_target
    mail_priority = mail_priority
    mail_subject =  mail_subject
    mail_msg =  mail_msg

    # Build package.
    send_mail = 'To: {0}\nFrom: {1}\nX-Priority: {2}\nSubject: {3}\n\n{4}'.format(email_target, mail_from, mail_priority, mail_subject, mail_msg)

    # Send mail.
    server.sendmail(mail_from, email_target, send_mail)

    # Close connection to mail server.
    server.quit()

    return

# ------ Main ------

pending_update = check4udpate(source_db_connection, sd_schema, sd_table)
if pending_update == 0:
    #sendcompletetion_noUpdates(email_target, mail_server, mail_from)
    arcpy.ClearWorkspaceCache_management(source_db_connection)
else:
    prepData(source_db_connection, processing_db_connection, sd_schema, sd_dataset, sd_table, lookback, pub_projectSRID, pub_transMethod)
    completion_status, failure_info = loadData(destination_db_connection, processing_db_connection, sd_schema, sd_dataset, sd_table, target_dataset, target_table)
    sendcompletetioninfo(email_target, mail_server, mail_from, completion_status, failure_info, pending_update)
    arcpy.ClearWorkspaceCache_management(source_db_connection)
