#-------------------------------------------------------------------------------
# Name:        Merge ODS Received into wServiceConnection
# Purpose:  This script merges data consumed from the ODS staging table and
#           merges the data into the wServiceConnection via a version within the
#           target database.  Some QAQC checks occur prior to moving the data to
#           ensure data integrity.
#
# Author:      John Spence
#
# Created:  2 May 2019
# Modified:  15 March 2020
# Modification Purpose:
#  15 April 2020    Synced up code to make sure counts are accurate on completions.
#  14 April 2020    Code adjustment to ensure NULLS for Previous Meters are pushed.
#  16 March 2020    Adjusted code to support large water meter installation along with setting XMT values to 
#                   NULL or NA.
#  06 September 2019 Removed qualifiers from update statements where AccountID and MetSerial Number are checked.
#                    Updated Q1 and Q2 to run correctly when called.  Numbering was off.
#  05 September 2019 Removed and remapped where XMTModel is coming from.
#  23 August 2019  Removed comments from both MC and EI installations.  Removed MetMFG and Size from EI.
#  22 August 2019  Added NewMeterModel to the mix to be transferred to MetModel
#  21 August 2019  Fixed a minor issue w/ the failure tracking.
#  17 August 2019  Bug fix for tracking errors.
#  06 August 2019  Completed adjustment to the error checking at the end.  I can't do math.
#  04 August 2019  Added in additional questions.  Adjusted comments during run.  Added in several failovers.
#  10 July 2019 Tweaked application to support Python 3.x and added in all additional fields.
#               Completed additional modifications that cleared out PMETInstall when METInstall NULL.
#  10 May 2019 Added all other fileds, save for ones not present in ToHost.
#  9 May 2019 Added cross ref/uncross.
#  6 May 2019 Added e-mail error checking.
#
#
#-------------------------------------------------------------------------------

# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Pretty simple setup.  Just change your settings/configuration below.  Do not
# go below the "DO NOT UPDATE...." line.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Configure hard coded db connection here.
#db_connection = r'Database Connections\\Connection to Utilities (DEV) COBSSDB16TS18B.sde'
db_connection = r'\database.sde'

#source_db_connection = r'Database Connections\\Connection to AMIWebGIS COBSSDB16TS18B.sde'
source_db_connection = r'database.sde'

# Configure database update type here. (Prod, Stg, Test, Other)
db_type = 'Test'

# Source Data Schema
sd_schema = 'UTIL'

# Source Data Table
sd_table = 'TOHOST2GIS_AMI'

# Target Data Schema
target_schema = 'UTIL'

# Target Data Table
target_table = 'wServiceConnection'

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

def check4udpate():
#-------------------------------------------------------------------------------
# Name:        Function - Check 4 Update
# Purpose:  This looks into WebGIS and identifies if there are awaiting installs.
#-------------------------------------------------------------------------------

    global pending_update
    global update_count

    print ("Entering Check For Updates---->\n")

    check_update_SQL = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where convert(date, [InstallDate]) <= convert (date, getdate()) and [SentToGIS_Date] is NULL'''

    check_update_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(check_update_SQL)

    if check_update_return == True:
        pending_update = 0
    elif check_update_return == None:
        pending_update = 0
    else:
        pending_update = [row for row in check_update_return]
        pending_update = len(pending_update)

    # Build Connection String
    data_source = source_db_connection + '\\{0}.{1}'.format(sd_schema, sd_table)

    # Check record count
    update_count = int(arcpy.GetCount_management(data_source).getOutput(0))
    print ("Meters available at target: {0}".format(update_count))
    print ("Meter records pending insertion:  {0}\n".format(pending_update))
    print ("Leaving Check For Updates----< \n\n")

    return (update_count, pending_update)

def correctInstallDates():

    print ("Entering Installation Date Correction---->\n")

    find_null_sql = '''
    select [ObjectID], [WorkEndDatetime]
    from [UTIL].[ToHost2GIS_AMI]
    where [InstallDate] is NULL
    '''

    try:
        located_nulls = arcpy.ArcSDESQLExecute(source_db_connection).execute(find_null_sql)

        for row in located_nulls:
            objectID = row[0]
            date_target = '{}'.format(int(row[1]))
            try:
                print (date_target)
                date_target = datetime.strptime(date_target, "%m%d%Y%H%M%S")
                print (date_target)
                date_target = date_target.strftime("%m-%d-%Y")

                update_sql = '''
                update [UTIL].[ToHost2GIS_AMI]
                set [InstallDate] = '{}',
                [XMTInstDate] = '{}'
                where [ObjectID] = '{}'
                '''.format(date_target, date_target, objectID)

                try:
                    arcpy.ArcSDESQLExecute(source_db_connection).execute(update_sql)
                except:
                    print ("Status:  Failure to correct installation dates!  Check for no NULLS in [UTIL].[TOHOST2GIS_AMI] before raising alarm.")
            except:
                print ('Unable to adjust date.  Check date.')
    except:
        print ('Status:  No updates needed')

    print ("Leaving Installation Date Correction----< \n\n")

    return

def mergeODS2GIS():

    global update_attempt_count
    update_attempt_count = 0

    print ("Entering ODS Merge---->\n")
    print ("Entering Meter Installation---->\n")

    mergeMeterInstalls()

    print ("\n\n")
    print ("Install Count:  {0}\n\n".format(mc_count))
    print ("Leaving Meter Installation----< \n\n")
    print ("Entering Endpoint Installation---->\n")

    mergeXMITInstalls(mc_count)

    print ("\n\n")
    print ("Install Count:  {0}\n\n".format(ei_count))
    print ("Leaving Endpoint Installation----< \n\n")

    update_attempt_count = mc_count + ei_count

    print ("Leaving ODS Merge----< \n\n")

    return (update_attempt_count, mc_count, ei_count)

def mergeMeterInstalls():

    global mc_count
    mc_count = 0

    pull_update_sql = '''select
    [ProvidedPremiseNumber]
    ,[FoundMeterManufacturer]
    ,[FoundMeterNumber]
    ,[FoundMeterReading]
    ,[FoundMeterSizeCode]
    ,[NewMeterManufacturer]
    ,[NewMeterNumber]
    ,[NewMeterReading]
    ,[NewMeterNumberOfDials]
    ,[NewMeterSizeCode]
    ,[Comments]
    ,[WorkOrderNumber]
    ,cast (convert (date,[InstallDate]) as varchar(10)) as [InstallDate]
    ,cast (convert (date, [XMTInstDate]) as varchar (10)) as [XMTInstDate]
    ,[XMTMFG]
    ,[XMTModel]
    ,[XMTSerialNum]
    ,[XMTMTType]
    ,cast (convert (date, [XMTShipDate]) as varchar (10)) as [XMTShipDate]
    ,[XMTPart]
    ,[XMTPSerial]
    ,[AsLeftQ1]
    ,[AsLeftQ2]
    ,[AsLeftQ9]
    ,[NewMeterModel]
    ,[NewEndpointType]
    ,[CompletedWorkType]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] <> 'EI'
	and convert(date, [InstallDate]) <= convert (date, getdate()) and [SentToGIS_Date] is NULL'''

    try:
        pull_update_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(pull_update_sql)
        for row in pull_update_return:
            AccountID = row[0]  ##ProvidedPremiseNumber <compare> accountid
            PMeterManufacturer = row[1] ## FoundMeterManufacturer -> PMeterManufacturer
            PMetSerial = row[2] ## FoundMeterNumber -> PMetSerial
            PMetFinalRead = row[3] ## FoundMeterReading -> PMetFinalRead
            PMeterSize = row[4] ##  FoundMeterSizeCode ->  PMeterSize
            MeterManufacturer = row[5] ## NewMeterManufacturer -> MeterManufacturer
            MetModel = row[24]  ## NewMeterModel -> MetModel
            MetSerialNum = row[6] ## NewMeterNumber -> MetSerialNum
            MetInitialRead = row[7] ##  NewMeterReading -> MetInitialRead
            DialCount = row[8] ##  NewMeterNumberOfDials -> DialCount
            MeterSize = row[9] ## NewMeterSizeCode -> MeterSize
            Comments = row[10] ##  Comments -> Comments
            FacilityID = row[11] ## WorkdOrderNumber = FacilityID
            MetInstDate = row[12] ##  InstallDate ->  MetInstDate
            completed_work = row[26]

            if completed_work == 'ME':
                XMTInstDate = 'NULL'
                XMTMFG = "'NA'"
                XMTModel = "'NA'"
                XMTSerialNum = 'NULL'
                XMTMTType = "'NA'"
                XMTShipDate = 'NULL'
                XMTPart = 'NULL'
                XMTPSerial = 'NULL'

            else:
                XMTInstDate = "'{}'".format(row[13]) ## XMTInstDate -> XMTInstDate
                XMTMFG = "'{}'".format(row[14])  ## XMTMFG -> XMTMFG
                XMTModel = "'{}'".format(row[15])  ## XMTModel -> XMTModel
                XMTSerialNum = "'{}'".format(row[16])  ## XMTSerialNum -> XMTSerialNum
                XMTMTType = "'{}'".format(row[17]) ## XMTMTType -> XMTMTType
                XMTShipDate = "'{}'".format(row[18]) ## XMTShipDate -> XMTShipDate
                XMTPart = "'{}'".format(row[19]) ## XMTPart -> XMTPart
                XMTPSerial = "'{}'".format(row[20]) ## XMTPSerial -> XMTPSerial

            AsLeftQ1 = row[21]  ## If NULL, Do not change [InstallDate].  If <> NULL, update [InstallDate] = row [12] and [BoxModel] = row [21]
            AsLeftQ2 = row[22]  ## If NULL do not update [BoxCover].  If <> NULL, update [BoxCover]
            AsLeftQ9 = row[23]  ## AsLeftQ9 -> XMTMTType

            print ("Attempting Update of Asset ID: {0}".format(FacilityID))
            print ("     Found Meter Number: {0}".format(PMetSerial))

            if MetModel == None:
                MetModel = 'None'
            else:
                MetModel = MetModel

            if "." not in MetInitialRead:
                MetInitialRead = MetInitialRead
            else:
                MetInitialRead = '0'

            if PMeterManufacturer == None:
                try:
                    old_wServiceConnectionMeterManufacturer_SQL = '''select
                    [METERMANUFACTURER]
                    from [UTIL].[wServiceConnection]
                    where [FacilityID] = {0} '''.format(FacilityID)
                    old_wServiceConnectionMeterManufacturer_return = arcpy.ArcSDESQLExecute(db_connection).execute(old_wServiceConnectionInstDate_SQL)

                    if old_wServiceConnectionMeterManufacturer_return == True:
                        print ("     No current manufacturer found.")
                        PMeterManufacturer = 'NULL'
                    elif old_wServiceConnectionMeterManufacturer_return == None:
                        print ("     No current manufacturer found.")
                        PMeterManufacturer = 'NULL'
                    else:
                        PMeterManufacturer = old_wServiceConnectionMeterManufacturer_return
                        print ("     Current manufacturer found:  {0}".format(PMeterManufacturer))
                        PMeterManufacturer = "'{}'".format(PMeterManufacturer)

                except Exception as old_wServiceConnectionMeterManufacturer_return:
                    print ("Status:  Failure to pull original MeterManufacturer!")
                    print (old_wServiceConnectionMeterManufacturer_return.args[0])
            else:
                PMeterManufacturer = "'{0}'".format(PMeterManufacturer)

            try:
                check_wServiceConnection_SQL = '''
                select [FacilityID] from [UTIL].[wServiceConnection] where [FacilityID] = '{0}' '''.format(FacilityID)
                check_wServiceConnection_return = arcpy.ArcSDESQLExecute(db_connection).execute(check_wServiceConnection_SQL)

                if check_wServiceConnection_return == True:
                    error_catch = 1
                elif check_wServiceConnection_return == None:
                    error_catch = 1
                else:
                    error_catch = 0


            except Exception as check_wServiceConnection_return:
                print ("Status:  Failure to pull original Service Type!")
                print (check_wServiceConnection_return.args[0])

                error_catch = 1

                update_ToHost2GIS_AMI_SQL ='''
                Update [UTIL].[ToHost2GIS_AMI]
                set [SentToGIS_Status] = 'Failure'
                , [SentToGIS_Date] = SYSDATETIME()
                , [SentToGIS_Confirmed] = 'Pending'
                where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                FacilityID, MetSerialNum, MetInstDate)

                arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)

                return

            if error_catch == 0:

                # ADDED 2019 Sept 13 - Addressed PMetSerial issue for bug 980.
                #                      Additionally, condensed 3 separate DB
                #                      calls into 1 call for 4 items.
                try:
                    old_wServiceConnectionData_SQL = '''select
                    [MetSerialNum]
                    , cast (convert (date, [MetInstDate]) as varchar(10)) as [MetInstDate]
                    , [MetModel]
                    , [ServiceType]
                    , [XMTSerialNum]
                    from [UTIL].[wServiceConnection]
                    where [FacilityID] = {0} '''.format(FacilityID)
                    old_wServiceConnectionData_return = arcpy.ArcSDESQLExecute(db_connection).execute(old_wServiceConnectionData_SQL)

                    if old_wServiceConnectionData_return == True:
                        print ("     No Meter Serial Number found.")
                        PMetSerial = 'NULL'
                        print ("     No Installation date found.")
                        PMetInstDate = 'NULL'
                        print ("     No Installation model found.")
                        PMETModel = 'NULL'
                        print ("     No service type found.\n")
                        PServiceType = 'NULL'
                        print ("     No previous transmitter found.\n")
                        XMTPSerial = 'NULL'
                    elif old_wServiceConnectionData_return == None:
                        print ("     No Meter Serial Number found.")
                        PMetSerial = 'NULL'
                        print ("     No Installation date found.")
                        PMetInstDate = 'NULL'
                        print ("     No Installation model found.")
                        PMETModel = 'NULL'
                        print ("     No service type found.\n")
                        PServiceType = 'NULL'
                        print ("     No previous transmitter found.\n")
                        XMTPSerial = 'NULL'
                    else:
                        for row in old_wServiceConnectionData_return:
                            PMetSerial = row[0]
                            if PMetSerial == None:
                                print ("     No Serial Number Found.")
                                PMetSerial = 'NULL'
                            else:
                                print ("     Serial Number found:  {0}".format(PMetSerial))
                                PMetSerial = "'{0}'".format(PMetSerial)

                            PMetInstDate = row[1]
                            if PMetInstDate == None:
                                print ("     No Installation date found.")
                                PMetInstDate = 'NULL'
                            else:
                                print ("     Installation date found:  {0}".format(PMetInstDate))
                                PMetInstDate = "'{0}'".format(PMetInstDate)

                            PMETModel = row[2]
                            if PMETModel == None:
                                print ("     No Installation model found.")
                                PMETModel = 'NULL'
                            else:
                                print ("     Meter Model found:  {0}".format(PMETModel))
                                PMETModel = "'{0}'".format(PMETModel)

                            PServiceType = row[3]
                            if PServiceType == None:
                                print ("     No service type found.")
                                PServiceType = 'NULL'
                            else:
                                print ("     Service type found:  {0}".format(PServiceType))
                                PServiceType = "'{0}'".format(PServiceType)

                            XMTPSerial = row[4]
                            if XMTPSerial == None:
                                print ("     No transmitter serial number found.\n")
                                XMTPSerial = 'NULL'
                            else:
                                print ("     Transmitter serial number found:  {0}\n".format(XMTPSerial))
                                XMTPSerial = "'{0}'".format(XMTPSerial)

                except Exception as old_wServiceConnectionData_return:
                    print ("Status:  Failure to pull original Meter Variables for Serial Number, Installation Date, Installation Model and Service Type!")
                    print (old_wServiceConnectionData_return.args[0])

                try:
                    # Begin update of UTIL.wServiceConnection mass data.

                    # Begin Transaction
                    arcpy.ArcSDESQLExecute(db_connection).startTransaction()

                    # Set SQL statement for update based upon current record row.
                    update_wServiceConnection_SQL = '''
                    Update [UTIL].[wServiceConnection]
                    set [PMeterManufacturer] = {0}
                    , [PMetSerial] = {1}
                    , [PMetFinalRead] = '{2}'
                    , [PMeterSize] = '{3}'
                    , [PMetInstDate] = {4}
                    , [PMETModel] = {5}
                    , [PServiceType] = {6}
                    , [MeterManufacturer] = '{7}'
                    , [MetModel] = '{8}'
                    , [MetSerialNum] = '{9}'
                    , [MetInitialRead] = '{10}'
                    , [DialCount] = '{11}'
                    , [MeterSize] = '{12}'
                    , [MetInstDate] = '{13}'
                    , [XMTInstDate] = {14}
                    , [XMTMFG] = {15}
                    , [XMTModel] = {16}
                    , [XMTSerialNum] = {17}
                    , [XMTMTType] = {18}
                    , [XMTShipDate] = {19}
                    , [XMTPart] = {20}
                    , [XMTPSerial] = {21}
                    , [SysChangeDate] = SYSDATETIME()
                    , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                    where [FacilityID] = '{22}' '''.format(
                    PMeterManufacturer, PMetSerial, PMetFinalRead, PMeterSize, PMetInstDate, PMETModel, PServiceType, MeterManufacturer, MetModel, MetSerialNum,
                    MetInitialRead, DialCount, MeterSize, MetInstDate, XMTInstDate, XMTMFG, XMTModel, XMTSerialNum, XMTMTType, XMTShipDate, XMTPart, XMTPSerial,
                    FacilityID)

                    # Make database edit.
                    update_wServiceConnection_return = arcpy.ArcSDESQLExecute(db_connection).execute(update_wServiceConnection_SQL)

                    if update_wServiceConnection_return == True:

                        # Commit data to database
                        arcpy.ArcSDESQLExecute(db_connection).commitTransaction()
                        error_catch = 0

                    else:
                        error_catch = 1
                        update_ToHost2GIS_AMI_SQL ='''
                        Update [UTIL].[ToHost2GIS_AMI]
                        set [SentToGIS_Status] = 'Failure'
                        , [SentToGIS_Date] = SYSDATETIME()
                        , [SentToGIS_Confirmed] = 'Pending'
                        where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                        FacilityID, MetSerialNum, MetInstDate)

                        arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)

                        return

                except Exception as error_test_accountID_return:
                    error_catch = 1
                    print ("Status:  Failure to push update to record!")
                    print (error_test_accountID_return.args[0])
                    update_ToHost2GIS_AMI_SQL ='''
                    Update [UTIL].[ToHost2GIS_AMI]
                    set [SentToGIS_Status] = 'Failure'
                    , [SentToGIS_Date] = SYSDATETIME()
                    , [SentToGIS_Confirmed] = 'Pending'
                    where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                    FacilityID, MetSerialNum, MetInstDate)

                    arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)

                    return

                if AsLeftQ1 is None:
                    AsLeftQ1 = 'No (Default)'

                if (AsLeftQ1 != 'No' and AsLeftQ1 != 'No (Default)') and error_catch == 0:

                    try:
                        # Begin update of UTIL.wServiceConnection Box Installation Related.

                        # Begin Transaction
                        arcpy.ArcSDESQLExecute(db_connection).startTransaction()

                        # Set SQL statement for update based upon current record row.
                        update_wServiceConnectionBoxInstall_SQL = '''
                        Update [UTIL].[wServiceConnection]
                        set [InstallDate] = '{0}'
                        , [BoxModel] = '{1}'
                        , [SysChangeDate] = SYSDATETIME()
                        , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                        where [FacilityID] = '{2}' '''.format(MetInstDate, AsLeftQ1, FacilityID)

                        print ("MC Asleft Q1")

                        # Make database edit.
                        arcpy.ArcSDESQLExecute(db_connection).execute(update_wServiceConnectionBoxInstall_SQL)

                        # Commit data to database
                        arcpy.ArcSDESQLExecute(db_connection).commitTransaction()

                    except Exception as error_test_accountID_return:
                        print ("Status:  Failure to push Q1 update to record!")
                        print (error_test_accountID_return.args[0])

                if AsLeftQ2 is None:
                    AsLeftQ2 = 'No (Default)'

                if (AsLeftQ2 != 'No' and AsLeftQ2 != 'No (Default)') and error_catch == 0:

                    try:
                        # Begin update of UTIL.wServiceConnection Box Installation Related.

                        # Begin Transaction
                        arcpy.ArcSDESQLExecute(db_connection).startTransaction()

                        # Set SQL statement for update based upon current record row.
                        update_wServiceConnectionBoxCover_SQL = '''
                        Update [UTIL].[wServiceConnection]
                        set [BoxCover] = '{0}'
                        , [SysChangeDate] = SYSDATETIME()
                        , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                        where [FacilityID] = '{1}' '''.format(AsLeftQ2, FacilityID,)

                        print ("MC Asleft Q2")

                        # Make database edit.
                        arcpy.ArcSDESQLExecute(db_connection).execute(update_wServiceConnectionBoxCover_SQL)

                        # Commit data to database
                        arcpy.ArcSDESQLExecute(db_connection).commitTransaction()

                    except Exception as error_test_accountID_return:
                        print ("Status:  Failure to push Q2 update to record!")
                        print (error_test_accountID_return.args[0])

                if error_catch == 0:

                    update_ToHost2GIS_AMI_SQL ='''
                    Update [UTIL].[ToHost2GIS_AMI]
                    set [SentToGIS_Status] = 'Complete'
                    , [SentToGIS_Date] = SYSDATETIME()
                    , [SentToGIS_Confirmed] = 'Yes'
                    where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                    FacilityID, MetSerialNum, MetInstDate)

                    arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)

                    mc_count += 1

                print ("\n\nCompleted {0} of {1} updates.\n\n".format(mc_count, pending_update))

            else:

                update_ToHost2GIS_AMI_SQL ='''
                Update [UTIL].[ToHost2GIS_AMI]
                set [SentToGIS_Status] = 'Failure'
                , [SentToGIS_Date] = SYSDATETIME()
                , [SentToGIS_Confirmed] = 'Pending'
                where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                FacilityID, MetSerialNum, MetInstDate)

                arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)

    except Exception as error_pull_update_return:
        print ("Status:  Failure to pull update return!")
        print (error_pull_update_return.args[0])

    return (mc_count)


def mergeXMITInstalls(mc_count):

    global ei_count
    ei_count = 0

    remaining_updates = pending_update - mc_count

    pull_update_sql = '''select
    [ProvidedPremiseNumber]
    ,[FoundMeterManufacturer]
    ,[FoundMeterNumber]
    ,[FoundMeterReading]
    ,[FoundMeterSizeCode]
    ,[Comments]
    ,[WorkOrderNumber]
    ,cast (convert (date, [XMTInstDate]) as varchar (10)) as [XMTInstDate]
    ,[XMTMFG]
    ,[XMTModel]
    ,[XMTSerialNum]
    ,[XMTMTType]
    ,cast (convert (date, [XMTShipDate]) as varchar (10)) as [XMTShipDate]
    ,[XMTPart]
    ,[XMTPSerial]
    ,[AsLeftQ1]
    ,[AsLeftQ2]
    ,[AsLeftQ9]
    ,[InstallDate]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'EI'
	and convert(date, [InstallDate]) <= convert (date, getdate()) and [SentToGIS_Date] is NULL'''

    try:
        pull_update_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(pull_update_sql)

        if pull_update_return == True:
            print ("No endpoint installations found.")
        elif pull_update_return == None:
            print ("No endpoint installations found.")
        else:
            for row in pull_update_return:
                AccountID = row[0]  ##ProvidedPremiseNumber <compare> accountid
                MeterManufacturer = row[1] ## FoundMeterManufacturer -> MeterManufacturer
                MetSerialNum = row[2] ## FoundMeterNumber -> MetSerialNum
                MetInitialRead = row[3] ##  FoundMeterReading -> MetInitialRead
                MeterSize = row[4] ## FoundMeterSizeCode -> MeterSize
                Comments = row[5] ##  Comments -> Comments
                FacilityID = row[6] ## WorkdOrderNumber = FacilityID
                XMTInstDate = row [7] ## XMTInstDate -> XMTInstDate
                XMTMFG = row [8]  ## XMTMFG -> XMTMFG
                XMTModel = row [9]  ## XMTModel -> XMTModel
                XMTSerialNum = row [10]  ## XMTSerialNum -> XMTSerialNum
                XMTMTType = row [11] ## XMTMTType -> XMTMTType
                XMTShipDate = row [12] ## XMTShipDate -> XMTShipDate
                XMTPart = row [13] ## XMTPart -> XMTPart
                XMTPSerial = row [14] ## XMTPSerial -> XMTPSerial
                AsLeftQ1 = row [15]  ## If NULL, Do not change [InstallDate].  If <> NULL, update [InstallDate] = row [12] and [BoxModel] = row [21]
                AsLeftQ2 = row [16]  ## If NULL do not update [BoxCover].  If <> NULL, update [BoxCover].
                AsLeftQ9 = row [17]  ## AsLeftQ9 -> XMTMTType

                print ("Attempting Update of Asset ID: {0}".format(FacilityID))
                print ("     Found Meter Number: {0}".format(MetSerialNum))

                try:
                    check_wServiceConnection_SQL = '''
                    select [FacilityID] from [UTIL].[wServiceConnection] where [FacilityID] = '{0}' '''.format(FacilityID)
                    check_wServiceConnection_return = arcpy.ArcSDESQLExecute(db_connection).execute(check_wServiceConnection_SQL)

                    if check_wServiceConnection_return == True:
                        error_catch = 1
                    elif check_wServiceConnection_return == None:
                        error_catch = 1
                    else:
                        error_catch = 0


                except Exception as check_wServiceConnection_return:
                    print ("Status:  Failure to pull original Service Type!")
                    print (check_wServiceConnection_return.args[0])

                    error_catch = 1

                    update_ToHost2GIS_AMI_SQL ='''
                    Update [UTIL].[ToHost2GIS_AMI]
                    set [SentToGIS_Status] = 'Failure'
                    , [SentToGIS_Date] = SYSDATETIME()
                    , [SentToGIS_Confirmed] = 'Pending'
                    where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                    FacilityID, XMTSerialNum, XMTInstDate)

                    arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)

                if error_catch == 0:

                    # ADDED 2019 Sept 13 - Addressed PMetSerial issue for bug 980.
                    #                      Additionally, condensed 3 separate DB
                    #                      calls into 1 call for 4 items.
                    try:
                        old_wServiceConnectionData_SQL = '''select
                        [XMTSerialNum]
                        from [UTIL].[wServiceConnection]
                        where [FacilityID] = {0} '''.format(FacilityID)
                        old_wServiceConnectionData_return = arcpy.ArcSDESQLExecute(db_connection).execute(old_wServiceConnectionData_SQL)

                        if old_wServiceConnectionData_return == True:
                            print ("     No previous transmitter found.\n")
                            XMTPSerial = 'NULL'
                        elif old_wServiceConnectionData_return == None:
                            print ("     No previous transmitter found.\n")
                            XMTPSerial = 'NULL'
                        else:
                            XMTPSerial = old_wServiceConnectionData_return
                            if XMTPSerial == None:
                                print ("     No transmitter serial number found.\n")
                                XMTPSerial = 'NULL'
                            else:
                                print ("     Transmitter serial number found:  {0}\n".format(XMTPSerial))
                                XMTPSerial = "'{0}'".format(XMTPSerial)

                    except Exception as old_wServiceConnectionData_return:
                        print ("Status:  Failure to pull original Meter Variables for Serial Number, Installation Date, Installation Model and Service Type!")
                        print (old_wServiceConnectionData_return.args[0])

                if error_catch == 0:

                        try:
                            # Begin update of UTIL.wServiceConnection data.

                            # Begin Transaction
                            arcpy.ArcSDESQLExecute(db_connection).startTransaction()

                            # Set SQL statement for update based upon current record row.
                            update_wServiceConnection_SQL = '''
                            Update [UTIL].[wServiceConnection]
                            set [XMTInstDate] = '{0}'
                            , [XMTMFG] = '{1}'
                            , [XMTModel] = '{2}'
                            , [XMTSerialNum] = '{3}'
                            , [XMTMTType] = '{4}'
                            , [XMTShipDate] = '{5}'
                            , [XMTPart] = '{6}'
                            , [XMTPSerial] = {7}
                            , [SysChangeDate] = SYSDATETIME()
                            , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                            where [FacilityID] = '{8}' '''.format(XMTInstDate, XMTMFG, XMTModel, XMTSerialNum, XMTMTType,
                            XMTShipDate, XMTPart, XMTPSerial, FacilityID)

                            # Make database edit.
                            update_wServiceConnection_return = arcpy.ArcSDESQLExecute(db_connection).execute(update_wServiceConnection_SQL)

                            if update_wServiceConnection_return == True:

                                # Commit data to database
                                arcpy.ArcSDESQLExecute(db_connection).commitTransaction()
                                error_catch = 0

                            else:
                                error_catch = 1
                                update_ToHost2GIS_AMI_SQL ='''
                                Update [UTIL].[ToHost2GIS_AMI]
                                set [SentToGIS_Status] = 'Failure'
                                , [SentToGIS_Date] = SYSDATETIME()
                                , [SentToGIS_Confirmed] = 'Pending'
                                where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                                FacilityID, MetSerialNum, InstallDate)

                                arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)

                        except Exception as error_test_accountID_return:
                            print ("Status:  Failure to push update to record!")
                            print (error_test_accountID_return.args[0])

                        if AsLeftQ1 is None:
                            AsLeftQ1 = 'No (Default)'

                        if (AsLeftQ1 != 'No' and AsLeftQ1 != 'No (Default)') and error_catch == 0:

                            try:
                                # Begin update of UTIL.wServiceConnection Box Installation Related.

                                # Begin Transaction
                                arcpy.ArcSDESQLExecute(db_connection).startTransaction()

                                # Set SQL statement for update based upon current record row.
                                update_wServiceConnectionBoxInstall_SQL = '''
                                Update [UTIL].[wServiceConnection]
                                set [InstallDate] = '{0}'
                                , [BoxModel] = '{1}'
                                , [SysChangeDate] = SYSDATETIME()
                                , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                                where [FacilityID] = '{2}' '''.format(XMTInstDate, AsLeftQ1, FacilityID)

                                print ("EI Asleft Q1")

                                # Make database edit.
                                arcpy.ArcSDESQLExecute(db_connection).execute(update_wServiceConnectionBoxInstall_SQL)

                                # Commit data to database
                                arcpy.ArcSDESQLExecute(db_connection).commitTransaction()

                            except Exception as error_test_accountID_return:
                                print ("Status:  Failure to push Q1 update to record!")
                                print (error_test_accountID_return.args[0])

                        if AsLeftQ2 is None:
                            AsLeftQ2 = 'No (Default)'

                        if (AsLeftQ2 != 'No' and AsLeftQ2 != 'No (Default)') and error_catch == 0:

                            try:
                                # Begin update of UTIL.wServiceConnection Box Installation Related.

                                # Begin Transaction
                                arcpy.ArcSDESQLExecute(db_connection).startTransaction()

                                # Set SQL statement for update based upon current record row.
                                update_wServiceConnectionBoxCover_SQL = '''
                                Update [UTIL].[wServiceConnection]
                                set [BoxCover] = '{0}'
                                , [SysChangeDate] = SYSDATETIME()
                                , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                                where [FacilityID] = '{1}' '''.format(AsLeftQ2, FacilityID)

                                print ("EI Asleft Q2")

                                # Make database edit.
                                arcpy.ArcSDESQLExecute(db_connection).execute(update_wServiceConnectionBoxCover_SQL)

                                # Commit data to database
                                arcpy.ArcSDESQLExecute(db_connection).commitTransaction()

                            except Exception as error_test_accountID_return:
                                print ("Status:  Failure to push Q2 update to record!")
                                print (error_test_accountID_return.args[0])

                        if error_catch == 0:

                            update_ToHost2GIS_AMI_SQL ='''
                            Update [UTIL].[ToHost2GIS_AMI]
                            set [SentToGIS_Status] = 'Complete'
                            , [SentToGIS_Date] = SYSDATETIME()
                            , [SentToGIS_Confirmed] = 'Yes'
                            where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                            FacilityID, XMTSerialNum, XMTInstDate)

                            arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)
                            ei_count += 1

                        print ("\n\nCompleted {0} of {1} remainnig updates of {2} total updates.\n\n".format(ei_count, remaining_updates, pending_update))
                else:

                    update_ToHost2GIS_AMI_SQL ='''
                    Update [UTIL].[ToHost2GIS_AMI]
                    set [SentToGIS_Status] = 'Failure'
                    , [SentToGIS_Date] = SYSDATETIME()
                    , [SentToGIS_Confirmed] = 'Pending'
                    where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                    FacilityID, XMTSerialNum, XMTInstDate)

                    arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)


    except Exception as error_pull_update_return:
        print ("Status:  Failure to pull update return!")
        print (error_pull_update_return.args[0])
        update_ToHost2GIS_AMI_SQL ='''
        Update [UTIL].[ToHost2GIS_AMI]
        set [SentToGIS_Status] = 'Failure'
        , [SentToGIS_Date] = SYSDATETIME()
        , [SentToGIS_Confirmed] = 'Pending'
        where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
        FacilityID, XMTSerialNum, XMTInstDate)

        arcpy.ArcSDESQLExecute(source_db_connection).execute(update_ToHost2GIS_AMI_SQL)

    return (ei_count)

def checkupdated():

    print ("Entering Check Update Installations---->\n")

    global checked_updates
    global checked_updates_fail
    checked_updates = 0
    checked_updates_fail = 0

    check_mc_sql = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'MC' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] = 'Complete' and [SentToGIS_Confirmed] = 'Yes' '''

    check_mc_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(check_mc_sql)

    if check_mc_return == True:
        mc_update = 0
    elif check_mc_return == None:
        mc_update = 0
    else:
        mc_update = [row for row in check_mc_return]
        mc_update = len(mc_update)

    check_mc_sql = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'MC' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] <> 'Complete' and [SentToGIS_Confirmed] <> 'Yes' '''

    check_mc_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(check_mc_sql)

    if check_mc_return == True:
        mc_update_fail = 0
    elif check_mc_return == None:
        mc_update_fail = 0
    else:
        mc_update_fail = [row for row in check_mc_return]
        mc_update_fail = len(mc_update_fail)

    check_me_sql = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'ME' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] = 'Complete' and [SentToGIS_Confirmed] = 'Yes' '''

    check_me_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(check_me_sql)

    if check_me_return == True:
        me_update = 0
    elif check_me_return == None:
        me_update = 0
    else:
        me_update = [row for row in check_me_return]
        me_update = len(me_update)

    check_me_sql = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'ME' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] <> 'Complete' and [SentToGIS_Confirmed] <> 'Yes' '''

    check_me_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(check_me_sql)

    if check_me_return == True:
        me_update_fail = 0
    elif check_me_return == None:
        me_update_fail = 0
    else:
        me_update_fail = [row for row in check_me_return]
        me_update_fail = len(me_update_fail)


    check_ei_sql = '''select
    [ProvidedPremiseNumber]
    ,[XMTSerialNum]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'EI' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] = 'Complete' and [SentToGIS_Confirmed] = 'Yes' '''

    check_ei_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(check_ei_sql)

    if check_ei_return == True:
        ei_update = 0
    elif check_ei_return == None:
        ei_update = 0
    else:
        ei_update = [row for row in check_ei_return]
        ei_update = len(ei_update)

    check_ei_sql = '''select
    [ProvidedPremiseNumber]
    ,[XMTSerialNum]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'EI' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] <> 'Complete' and [SentToGIS_Confirmed] <> 'Yes' '''

    check_ei_return = arcpy.ArcSDESQLExecute(source_db_connection).execute(check_ei_sql)

    if check_ei_return == True:
        ei_update_fail = 0
    elif check_ei_return == None:
        ei_update_fail = 0
    else:
        ei_update_fail = [row for row in check_ei_return]
        ei_update_fail = len(ei_update_fail)

    checked_updates = mc_update + me_update + ei_update
    checked_updates_fail = mc_update_fail + me_update_fail + ei_update_fail

    print ("Found {0} completed installations for today.".format(checked_updates))
    print ("Found {0} failed installations for today.".format(checked_updates_fail))

    print ("Leaving Check Update Installations----< \n\n")

    return (checked_updates, checked_updates_fail)

def sendcompletetioninfo(pending_update, email_target, mail_server, mail_from, checked_updates, checked_updates_fail, update_attempt_count):

    missedupdate = checked_updates_fail

    if checked_updates == pending_update and pending_update > 0:
        mail_priority = '5'
        mail_subject = 'Success:  New AMI installations captured successfully'
        mail_msg = ('{} out of {} meter updates were successfully completed.\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(checked_updates, pending_update))

    elif missedupdate == pending_update and pending_update > 0:
        mail_priority = '1'
        mail_subject = 'Failure:  New AMI installs were not captured successfully'
        mail_msg = ('There was a failure to update {} meters.  Please check the logs and scripts prior to attempting again.\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(missedupdate))

    else:
        mail_priority = '3'
        mail_subject = 'Warning:  New AMI Meters were partially added successfully'

        if missedupdate == 1:
            mail_msg = ('{} out of {} meter captures were successfully completed. {} was unsuccessful.\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(checked_updates, pending_update, missedupdate))
        else:
            mail_msg = ('{} out of {} meter captures were successfully completed. {} were unsuccessful.\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(checked_updates, pending_update, missedupdate))

    # Set SMTP Server and configuration of message.
    server = smtplib.SMTP(mail_server)
    email_target = email_target
    mail_priority = mail_priority
    mail_subject =  mail_subject
    mail_msg =  mail_msg

    send_mail = 'To: {0}\nFrom: {1}\nX-Priority: {2}\nSubject: {3}\n\n{4}'.format(email_target, mail_from, mail_priority, mail_subject, mail_msg)
    # Double commented out code hides how to send a BCC as well.
    ##send_mail = 'To: {0}\nFrom: {1}\nBCC: {2}\nX-Priority: {3}\nSubject: {4}\n\n{5}'.format(email_target, mail_from, mail_bcc, mail_priority, mail_subject, mail_msg)

    server.sendmail(mail_from, email_target, send_mail)
    # Double commented out code hides how to send a BCC as well.
    ##server.sendmail(mail_from, [email_target, mail_bcc], send_mail)

    server.quit()

    return

def sendcompletetion_noUpdates(email_target, mail_server, mail_from):
    mail_priority = '5'
    mail_subject = 'Success:  Process ran, but no updates required.'
    mail_msg = 'The process successfully ran, but no meter updates were required.\n\n[SYSTEM AUTO GENERATED MESSAGE]'

    # Set SMTP Server and configuration of message.
    server = smtplib.SMTP(mail_server)
    email_target = email_target
    mail_priority = mail_priority
    mail_subject =  mail_subject
    mail_msg =  mail_msg

    send_mail = 'To: {0}\nFrom: {1}\nX-Priority: {2}\nSubject: {3}\n\n{4}'.format(email_target, mail_from, mail_priority, mail_subject, mail_msg)
    # Double commented out code hides how to send a BCC as well.
    ##send_mail = 'To: {0}\nFrom: {1}\nBCC: {2}\nX-Priority: {3}\nSubject: {4}\n\n{5}'.format(email_target, mail_from, mail_bcc, mail_priority, mail_subject, mail_msg)

    server.sendmail(mail_from, email_target, send_mail)
    # Double commented out code hides how to send a BCC as well.
    ##server.sendmail(mail_from, [email_target, mail_bcc], send_mail)

    server.quit()

    return

# ------ Main ------

arcpy.SignInToPortal('https://www.arcgis.com', 'gisdba_cobgis', 'WAw4hic=3uCHUsaP7guc')

correctInstallDates()
check4udpate ()
if pending_update == 0:
    sendcompletetion_noUpdates(email_target, mail_server, mail_from)
    arcpy.ClearWorkspaceCache_management(source_db_connection)
    quit()
else:
    mergeODS2GIS()
    checkupdated()
    sendcompletetioninfo(pending_update, email_target, mail_server, mail_from, checked_updates, checked_updates_fail, update_attempt_count)
    arcpy.ClearWorkspaceCache_management(db_connection)
    arcpy.ClearWorkspaceCache_management(source_db_connection)
    quit()
