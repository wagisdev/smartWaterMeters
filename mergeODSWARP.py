#-------------------------------------------------------------------------------
# Name:        Merge ODS Received into wServiceConnection (WARP Speed)
# Purpose:  This script merges data consumed from the ODS staging table and
#           merges the data into the wServiceConnection via a version within the
#           target database.  Some QAQC checks occur prior to moving the data to
#           ensure data integrity.
#
# Author:      John Spence
#
# Created:  16 April 2020
# Modified:  
# Modification Purpose:
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
UTIL_conn = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=;'
                      'Database=Utilities;'
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      #r'UID=;'  # Comment out if you are using AD authentication.
                      #r'PWD='     # Comment out if you are using AD authentication.
                      )


WebGIS_conn = ('Driver={ODBC Driver 17 for SQL Server};'  # This will require adjustment if you are using a different database.
                      r'Server=;'
                      'Database=WebGIS;'
                      'Trusted_Connection=yes;'  #Only if you are using a AD account.
                      #r'UID=;'  # Comment out if you are using AD authentication.
                      #r'PWD='     # Comment out if you are using AD authentication.
                      )

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
#import pandas as pd
import datetime
from arcpy import env
from datetime import datetime
import pyodbc

def check4udpate():
#-------------------------------------------------------------------------------
# Name:        Function - Check 4 Update
# Purpose:  This looks into WebGIS and identifies if there are awaiting installs.
#-------------------------------------------------------------------------------

    global pending_update
    global update_count

    print ("Entering Check For Updates---->\n")

    query_string = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where convert(date, [InstallDate]) <= convert (date, getdate()) and [SentToGIS_Date] is NULL'''

    query_conn = pyodbc.connect(WebGIS_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    check_update_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    pending_update = len(check_update_return)

    # Check record count
    update_count = pending_update
    print ("Meters available at target: {0}".format(update_count))
    print ("Meter records pending insertion:  {0}\n".format(pending_update))
    print ("Leaving Check For Updates----< \n\n")

    return (update_count, pending_update)

def correctInstallDates():

    print ("Entering Installation Date Correction---->\n")

    try:
        query_string = '''
        select [ObjectID], [WorkEndDatetime]
        from [UTIL].[ToHost2GIS_AMI]
        where [InstallDate] is NULL
        '''

        query_conn = pyodbc.connect(WebGIS_conn)
        query_cursor = query_conn.cursor()
        query_cursor.execute(query_string)
        located_nulls = query_cursor.fetchall()
        query_cursor.close()
        query_conn.close()

        for row in located_nulls:
            objectID = row[0]
            date_target = '{}'.format(int(row[1]))
            if len(date_target) == 13:
                date_target = '0' + date_target
            try:
                print (date_target)
                date_target = datetime.strptime(date_target, "%m%d%Y%H%M%S")
                print (date_target)
                date_target = date_target.strftime("%m-%d-%Y")

                try:
                    update_string = '''
                    update [UTIL].[ToHost2GIS_AMI]
                    set [InstallDate] = '{}',
                    [XMTInstDate] = '{}'
                    where [ObjectID] = '{}'
                    '''.format(date_target, date_target, objectID)

                    update_conn = pyodbc.connect(WebGIS_conn)
                    update_cursor = update_conn.cursor()
                    update_cursor.execute(update_string)
                    update_conn.commit()
                    update_cursor.close()
                    update_conn.close()

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

    query_string = '''select
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
        query_conn = pyodbc.connect(WebGIS_conn)
        query_cursor = query_conn.cursor()
        query_cursor.execute(query_string)
        pull_update_return = query_cursor.fetchall()
        query_cursor.close()
        query_conn.close()

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
                    query_string = '''select
                    [METERMANUFACTURER]
                    from [UTIL].[wServiceConnection]
                    where [FacilityID] = {0} '''.format(FacilityID)

                    query_conn = pyodbc.connect(UTIL_conn)
                    query_cursor = query_conn.cursor()
                    query_cursor.execute(query_string)
                    old_wServiceConnectionMeterManufacturer_return = query_cursor.fetchall()
                    query_cursor.close()
                    query_conn.close()

                    for response in old_wServiceConnectionMeterManufacturer_return:
                        if response[0] == None:
                            print ("     No current manufacturer found.")
                            PMeterManufacturer = 'NULL'
                        else:
                            PMeterManufacturer = response[0]
                            print ("     Current manufacturer found:  {0}".format(PMeterManufacturer))
                            PMeterManufacturer = "'{}'".format(PMeterManufacturer)

                except Exception as old_wServiceConnectionMeterManufacturer_return:
                    print ("Status:  Failure to pull original MeterManufacturer!")
                    print (old_wServiceConnectionMeterManufacturer_return.args[0])
            else:
                PMeterManufacturer = "'{0}'".format(PMeterManufacturer)

            try:
                query_string = '''
                select [FacilityID] from [UTIL].[wServiceConnection] where [FacilityID] = '{0}' '''.format(FacilityID)

                query_conn = pyodbc.connect(UTIL_conn)
                query_cursor = query_conn.cursor()
                query_cursor.execute(query_string)
                check_wServiceConnection_return = query_cursor.fetchall()
                query_cursor.close()
                query_conn.close()

                for response in check_wServiceConnection_return:
                    if response[0] == None:
                        error_catch = 1
                    else:
                        error_catch = 0

            except Exception as check_wServiceConnection_return:
                print ("Status:  Failure to pull original Service Type!")
                print (check_wServiceConnection_return.args[0])

                error_catch = 1

                update_string ='''
                Update [UTIL].[ToHost2GIS_AMI]
                set [SentToGIS_Status] = 'Failure'
                , [SentToGIS_Date] = SYSDATETIME()
                , [SentToGIS_Confirmed] = 'Pending'
                where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                FacilityID, MetSerialNum, MetInstDate)

                update_conn = pyodbc.connect(WebGIS_conn)
                update_cursor = update_conn.cursor()
                update_cursor.execute(update_string)
                update_conn.commit()
                update_cursor.close()
                update_conn.close()

                return

            if error_catch == 0:

                # ADDED 2019 Sept 13 - Addressed PMetSerial issue for bug 980.
                #                      Additionally, condensed 3 separate DB
                #                      calls into 1 call for 4 items.
                try:
                    query_string = '''select
                    [MetSerialNum]
                    , cast (convert (date, [MetInstDate]) as varchar(10)) as [MetInstDate]
                    , [MetModel]
                    , [ServiceType]
                    , [XMTSerialNum]
                    from [UTIL].[wServiceConnection]
                    where [FacilityID] = {0} '''.format(FacilityID)

                    query_conn = pyodbc.connect(UTIL_conn)
                    query_cursor = query_conn.cursor()
                    query_cursor.execute(query_string)
                    old_wServiceConnectionData_return = query_cursor.fetchall()
                    query_cursor.close()
                    query_conn.close()

                    if len(old_wServiceConnectionData_return) == 0:
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

                    # Set SQL statement for update based upon current record row.
                    update_string = '''
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

                    update_conn = pyodbc.connect(UTIL_conn)
                    update_cursor = update_conn.cursor()
                    update_cursor.execute(update_string)
                    update_conn.commit()
                    update_cursor.close()
                    update_conn.close()

                except Exception as error_test_accountID_return:
                    error_catch = 1
                    print ("Status:  Failure to push update to record!")
                    print (error_test_accountID_return.args[0])
                    update_string ='''
                    Update [UTIL].[ToHost2GIS_AMI]
                    set [SentToGIS_Status] = 'Failure'
                    , [SentToGIS_Date] = SYSDATETIME()
                    , [SentToGIS_Confirmed] = 'Pending'
                    where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                    FacilityID, MetSerialNum, MetInstDate)

                    update_conn = pyodbc.connect(WebGIS_conn)
                    update_cursor = update_conn.cursor()
                    update_cursor.execute(update_string)
                    update_conn.commit()
                    update_cursor.close()
                    update_conn.close()

                    return

                if AsLeftQ1 is None:
                    AsLeftQ1 = 'No (Default)'

                if (AsLeftQ1 != 'No' and AsLeftQ1 != 'No (Default)') and error_catch == 0:

                    try:
                        # Begin update of UTIL.wServiceConnection Box Installation Related.

                        # Set SQL statement for update based upon current record row.
                        update_string = '''
                        Update [UTIL].[wServiceConnection]
                        set [InstallDate] = '{0}'
                        , [BoxModel] = '{1}'
                        , [SysChangeDate] = SYSDATETIME()
                        , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                        where [FacilityID] = '{2}' '''.format(MetInstDate, AsLeftQ1, FacilityID)

                        print ("    MC Asleft Q1")

                        update_conn = pyodbc.connect(UTIL_conn)
                        update_cursor = update_conn.cursor()
                        update_cursor.execute(update_string)
                        update_conn.commit()
                        update_cursor.close()
                        update_conn.close()

                    except Exception as error_test_accountID_return:
                        print ("Status:  Failure to push Q1 update to record!")
                        print (error_test_accountID_return.args[0])

                if AsLeftQ2 is None:
                    AsLeftQ2 = 'No (Default)'

                if (AsLeftQ2 != 'No' and AsLeftQ2 != 'No (Default)') and error_catch == 0:

                    try:
                        # Begin update of UTIL.wServiceConnection Box Installation Related.

                        # Set SQL statement for update based upon current record row.
                        update_string = '''
                        Update [UTIL].[wServiceConnection]
                        set [BoxCover] = '{0}'
                        , [SysChangeDate] = SYSDATETIME()
                        , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                        where [FacilityID] = '{1}' '''.format(AsLeftQ2, FacilityID,)

                        print ("    MC Asleft Q2")

                        update_conn = pyodbc.connect(UTIL_conn)
                        update_cursor = update_conn.cursor()
                        update_cursor.execute(update_string)
                        update_conn.commit()
                        update_cursor.close()
                        update_conn.close()

                    except Exception as error_test_accountID_return:
                        print ("Status:  Failure to push Q2 update to record!")
                        print (error_test_accountID_return.args[0])

                if error_catch == 0:

                    update_string ='''
                    Update [UTIL].[ToHost2GIS_AMI]
                    set [SentToGIS_Status] = 'Complete'
                    , [SentToGIS_Date] = SYSDATETIME()
                    , [SentToGIS_Confirmed] = 'Yes'
                    where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                    FacilityID, MetSerialNum, MetInstDate)

                    update_conn = pyodbc.connect(WebGIS_conn)
                    update_cursor = update_conn.cursor()
                    update_cursor.execute(update_string)
                    update_conn.commit()
                    update_cursor.close()
                    update_conn.close()

                    mc_count += 1

                print ("\n\nCompleted {0} of {1} updates.\n\n".format(mc_count, pending_update))

            else:

                update_string ='''
                Update [UTIL].[ToHost2GIS_AMI]
                set [SentToGIS_Status] = 'Failure'
                , [SentToGIS_Date] = SYSDATETIME()
                , [SentToGIS_Confirmed] = 'Pending'
                where [WorkOrderNumber] = '{0}' and [NewMeterNumber] = '{1}' and [InstallDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                FacilityID, MetSerialNum, MetInstDate)

                update_conn = pyodbc.connect(WebGIS_conn)
                update_cursor = update_conn.cursor()
                update_cursor.execute(update_string)
                update_conn.commit()
                update_cursor.close()
                update_conn.close()

    except Exception as error_pull_update_return:
        print ("Status:  Failure to pull update return!")
        print (error_pull_update_return.args[0])

    return (mc_count)


def mergeXMITInstalls(mc_count):

    global ei_count
    ei_count = 0

    remaining_updates = pending_update - mc_count

    query_string = '''select
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
        query_conn = pyodbc.connect(WebGIS_conn)
        query_cursor = query_conn.cursor()
        query_cursor.execute(query_string)
        pull_update_return = query_cursor.fetchall()
        query_cursor.close()
        query_conn.close()

        if len(pull_update_return) == 0:
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
                    query_string = '''
                    select [FacilityID] from [UTIL].[wServiceConnection] where [FacilityID] = '{0}' '''.format(FacilityID)

                    query_conn = pyodbc.connect(UTIL_conn)
                    query_cursor = query_conn.cursor()
                    query_cursor.execute(query_string)
                    check_wServiceConnection_return = query_cursor.fetchall()
                    query_cursor.close()
                    query_conn.close()

                    if len(check_wServiceConnection_return) == 0:
                        error_catch = 1
                    else:
                        error_catch = 0


                except Exception as check_wServiceConnection_return:
                    print ("Status:  Failure to pull original Service Type!")
                    print (check_wServiceConnection_return.args[0])

                    error_catch = 1

                    update_string ='''
                    Update [UTIL].[ToHost2GIS_AMI]
                    set [SentToGIS_Status] = 'Failure'
                    , [SentToGIS_Date] = SYSDATETIME()
                    , [SentToGIS_Confirmed] = 'Pending'
                    where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                    FacilityID, XMTSerialNum, XMTInstDate)

                    update_conn = pyodbc.connect(WebGIS_conn)
                    update_cursor = update_conn.cursor()
                    update_cursor.execute(update_string)
                    update_conn.commit()
                    update_cursor.close()
                    update_conn.close()

                if error_catch == 0:

                    # ADDED 2019 Sept 13 - Addressed PMetSerial issue for bug 980.
                    #                      Additionally, condensed 3 separate DB
                    #                      calls into 1 call for 4 items.
                    try:
                        query_string = '''select
                        [XMTSerialNum]
                        from [UTIL].[wServiceConnection]
                        where [FacilityID] = {0} '''.format(FacilityID)

                        query_conn = pyodbc.connect(UTIL_conn)
                        query_cursor = query_conn.cursor()
                        query_cursor.execute(query_string)
                        old_wServiceConnectionData_return = query_cursor.fetchall()
                        query_cursor.close()
                        query_conn.close()

                        for response in old_wServiceConnectionData_return:
                            XMTPSerial = response[0]

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
                            # Set SQL statement for update based upon current record row.
                            update_string = '''
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

                            update_conn = pyodbc.connect(UTIL_conn)
                            update_cursor = update_conn.cursor()
                            update_cursor.execute(update_string)
                            update_conn.commit()
                            update_cursor.close()
                            update_conn.close()

                        except Exception as error_test_accountID_return:
                            print ("Status:  Failure to push update to record!")
                            print (error_test_accountID_return.args[0])

                        if AsLeftQ1 is None:
                            AsLeftQ1 = 'No (Default)'

                        if (AsLeftQ1 != 'No' and AsLeftQ1 != 'No (Default)') and error_catch == 0:

                            try:
                                # Set SQL statement for update based upon current record row.
                                update_string = '''
                                Update [UTIL].[wServiceConnection]
                                set [InstallDate] = '{0}'
                                , [BoxModel] = '{1}'
                                , [SysChangeDate] = SYSDATETIME()
                                , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                                where [FacilityID] = '{2}' '''.format(XMTInstDate, AsLeftQ1, FacilityID)

                                print ("    EI Asleft Q1")

                                update_conn = pyodbc.connect(UTIL_conn)
                                update_cursor = update_conn.cursor()
                                update_cursor.execute(update_string)
                                update_conn.commit()
                                update_cursor.close()
                                update_conn.close()


                            except Exception as error_test_accountID_return:
                                print ("Status:  Failure to push Q1 update to record!")
                                print (error_test_accountID_return.args[0])

                        if AsLeftQ2 is None:
                            AsLeftQ2 = 'No (Default)'

                        if (AsLeftQ2 != 'No' and AsLeftQ2 != 'No (Default)') and error_catch == 0:

                            try:
                                # Begin update of UTIL.wServiceConnection Box Installation Related.

                                # Set SQL statement for update based upon current record row.
                                update_string = '''
                                Update [UTIL].[wServiceConnection]
                                set [BoxCover] = '{0}'
                                , [SysChangeDate] = SYSDATETIME()
                                , [SysChangeUser] = REPLACE(system_user,'COBNT1\\','')
                                where [FacilityID] = '{1}' '''.format(AsLeftQ2, FacilityID)

                                print ("    EI Asleft Q2")

                                update_conn = pyodbc.connect(UTIL_conn)
                                update_cursor = update_conn.cursor()
                                update_cursor.execute(update_string)
                                update_conn.commit()
                                update_cursor.close()
                                update_conn.close()

                            except Exception as error_test_accountID_return:
                                print ("Status:  Failure to push Q2 update to record!")
                                print (error_test_accountID_return.args[0])

                        if error_catch == 0:

                            update_string ='''
                            Update [UTIL].[ToHost2GIS_AMI]
                            set [SentToGIS_Status] = 'Complete'
                            , [SentToGIS_Date] = SYSDATETIME()
                            , [SentToGIS_Confirmed] = 'Yes'
                            where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                            FacilityID, XMTSerialNum, XMTInstDate)

                            update_conn = pyodbc.connect(WebGIS_conn)
                            update_cursor = update_conn.cursor()
                            update_cursor.execute(update_string)
                            update_conn.commit()
                            update_cursor.close()
                            update_conn.close()

                            ei_count += 1

                        print ("\n\nCompleted {0} of {1} remainnig updates of {2} total updates.\n\n".format(ei_count, remaining_updates, pending_update))
                else:

                    update_string ='''
                    Update [UTIL].[ToHost2GIS_AMI]
                    set [SentToGIS_Status] = 'Failure'
                    , [SentToGIS_Date] = SYSDATETIME()
                    , [SentToGIS_Confirmed] = 'Pending'
                    where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
                    FacilityID, XMTSerialNum, XMTInstDate)

                    update_conn = pyodbc.connect(WebGIS_conn)
                    update_cursor = update_conn.cursor()
                    update_cursor.execute(update_string)
                    update_conn.commit()
                    update_cursor.close()
                    update_conn.close()


    except Exception as error_pull_update_return:
        print ("Status:  Failure to pull update return!")
        print (error_pull_update_return.args[0])
        update_string ='''
        Update [UTIL].[ToHost2GIS_AMI]
        set [SentToGIS_Status] = 'Failure'
        , [SentToGIS_Date] = SYSDATETIME()
        , [SentToGIS_Confirmed] = 'Pending'
        where [WorkOrderNumber] = '{0}' and [XMTSerialNum] = '{1}' and [XMTInstDate] = '{2}' and [SentToGIS_Status] is NULL'''.format(
        FacilityID, XMTSerialNum, XMTInstDate)

        update_conn = pyodbc.connect(WebGIS_conn)
        update_cursor = update_conn.cursor()
        update_cursor.execute(update_string)
        update_conn.commit()
        update_cursor.close()
        update_conn.close()

    return (ei_count)

def checkupdated():

    print ("Entering Check Update Installations---->\n")

    global checked_updates
    global checked_updates_fail
    checked_updates = 0
    checked_updates_fail = 0

    query_string = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'MC' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] = 'Complete' and [SentToGIS_Confirmed] = 'Yes' '''

    query_conn = pyodbc.connect(WebGIS_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    check_mc_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    mc_check = len(check_mc_return)

    if mc_check == 0:
        mc_update = 0
    else:
        mc_update = mc_check

    query_string = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'MC' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] <> 'Complete' and [SentToGIS_Confirmed] <> 'Yes' '''

    query_conn = pyodbc.connect(WebGIS_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    check_mc_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    mc_check = len(check_mc_return)

    if mc_check == 0:
        mc_update_fail = 0
    else:
        mc_update_fail = mc_check

    query_string = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'ME' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] = 'Complete' and [SentToGIS_Confirmed] = 'Yes' '''

    query_conn = pyodbc.connect(WebGIS_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    check_me_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    me_check = len(check_me_return)

    if me_check == 0:
        me_update = 0
    else:
        me_update = me_check

    query_string = '''select
    [ProvidedPremiseNumber]
    ,[NewMeterNumber]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'ME' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] <> 'Complete' and [SentToGIS_Confirmed] <> 'Yes' '''

    query_conn = pyodbc.connect(WebGIS_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    check_me_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    me_check = len(check_me_return)

    if me_check == 0:
        me_update_fail = 0
    else:
        me_update_fail = me_check


    query_string = '''select
    [ProvidedPremiseNumber]
    ,[XMTSerialNum]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'EI' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] = 'Complete' and [SentToGIS_Confirmed] = 'Yes' '''

    query_conn = pyodbc.connect(WebGIS_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    check_ei_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    ei_check = len(check_ei_return)

    if ei_check == 0:
        ei_update = 0
    else:
        ei_update = ei_check

    query_string = '''select
    [ProvidedPremiseNumber]
    ,[XMTSerialNum]
    from [UTIL].[TOHOST2GIS_AMI] where [CompletedWorkType] = 'EI' and convert(date, [SentToGIS_Date]) = convert (date, getdate()) and [SentToGIS_Status] <> 'Complete' and [SentToGIS_Confirmed] <> 'Yes' '''

    query_conn = pyodbc.connect(WebGIS_conn)
    query_cursor = query_conn.cursor()
    query_cursor.execute(query_string)
    check_ei_return = query_cursor.fetchall()
    query_cursor.close()
    query_conn.close()

    ei_check = len(check_ei_return)

    if ei_check == 0:
        ei_update_fail = 0
    else:
        ei_update_fail = ei_check

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
        mail_subject = 'Test Success:  New AMI installations captured successfully'
        mail_msg = ('{} out of {} meter updates were successfully completed.\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(checked_updates, pending_update))

    elif missedupdate == pending_update and pending_update > 0:
        mail_priority = '1'
        mail_subject = 'Test Failure:  New AMI installs were not captured successfully'
        mail_msg = ('There was a failure to update {} meters.  Please check the logs and scripts prior to attempting again.\n\n[SYSTEM AUTO GENERATED MESSAGE]'.format(missedupdate))

    else:
        mail_priority = '3'
        mail_subject = 'Test Warning:  New AMI Meters were partially added successfully'

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

correctInstallDates()
check4udpate ()
if pending_update == 0:
    sendcompletetion_noUpdates(email_target, mail_server, mail_from)
    quit()
else:
    mergeODS2GIS()
    checkupdated()
    sendcompletetioninfo(pending_update, email_target, mail_server, mail_from, checked_updates, checked_updates_fail, update_attempt_count)
    quit()
