/****************************************************************
// ----- Persistent Systems Pvt. Ltd., Pune ----------- 
// Copyright (c) 2005
// All rights reserved.
// Name          : PCNWriter_Group.cpp
// Description   : Group level interface class
// Author        : Sunita Patil 02-11-2005

  				   Sunita Patil  17-06-2005
				   Missing links between source qualifier 
				   and the netezza target table is supported

				   Sunita Patil  20-06-2005
				   Support for transaction and recovery

				   Sunita Patil  23-06-2005
				   Update strategy is added

				   Sunita Patil  27-06-2005
				   Localization support is added

*****************************************************************/

//PCNWriter Header Files
#include "pcnwriter_partition.hpp"
#include "pcnwriter_group.hpp"
 
/****************************************************************
 * Name		   : PCNWriter_Group
 * Description : Constructor
 * Returns	   : None
 ****************************************************************/

PCNWriter_Group::PCNWriter_Group(const ISession* m_pSess,
								 const ITargetInstance* 
								 pTargetInstance, IUINT32 tgtNum, 
								 IUINT32 grpNum, PCNWriter_Plugin* plugDrv, 
								 const ISessionExtension* pSessExtn,
								 const ILog*	pLogger,
								 const IUtilsServer* pUtils)
:	m_pSession(m_pSess),
	m_pTargetInstance(pTargetInstance), 
	m_nGrpNum(grpNum), m_nTgtNum(tgtNum), 
	m_pPluginDriver(plugDrv), m_pSessExtn(pSessExtn),
	m_pLogger(pLogger),
	m_pUtils(pUtils),
	m_numPartitionsInitialized(0)
{}

PCNWriter_Group::PCNWriter_Group(const ISession* m_pSess,
								 const ITargetInstance* 
								 pTargetInstance, IUINT32 tgtNum, 
								 IUINT32 grpNum, PCNWriter_Plugin* plugDrv, 
								 const ISessionExtension* pSessExtn,
								  SLogger*	sLogger,
								 const IUtilsServer* pUtils,MsgCatalog* msgCatalog,const ILog* pLogger,unsigned long int &pluginRequestedRows,unsigned long int &pluginAppliedRows,unsigned long int &pluginRejectedRows)
:	m_pSession(m_pSess),
	m_pTargetInstance(pTargetInstance), 
	m_nGrpNum(grpNum), m_nTgtNum(tgtNum), 
	m_pPluginDriver(plugDrv), m_pSessExtn(pSessExtn),
	m_sLogger(sLogger),
	m_pUtils(pUtils), m_msgCatalog(msgCatalog),m_pLogger(pLogger), m_numPartitionsInitialized(0)
{ 
	m_pluginRequestedRows=&pluginRequestedRows;
	m_pluginAppliedRows=&pluginAppliedRows;
	m_pluginRejectedRows=&pluginRejectedRows;

	//Since we are creating a single temp table across all partitions, this variable will be set once temp table is
	//created by a partition.
	m_b_tempTableCreated=IFALSE;

	//Setting no of partition have loaded data in to the temp table equal to the zero. Once this is equal to the totalNoOfPartitions we can load
	//data from the temp table to the target table. 
	m_noPartLoadedDataTemp=0;
	m_totalReqRows=0;
	m_bTruncateTable=IFALSE;

	m_partitionDoneMutex= new IThreadMutex();
	m_totalReqRowsMutex= new IThreadMutex();

	m_WrtGrp_dsn = PCN_Constants::PCN_EMPTY;
    m_WrtGrp_uid = PCN_Constants::PCN_EMPTY;
    m_WrtGrp_pwd = PCN_Constants::PCN_EMPTY;
	m_WrtGrp_databasename = PCN_Constants::PCN_EMPTY;
	m_WrtGrp_schemaname = PCN_Constants::PCN_EMPTY;
	m_WrtGrp_servername = PCN_Constants::PCN_EMPTY;
	m_WrtGrp_port = PCN_Constants::PCN_EMPTY;
	m_WrtGrp_drivername = PCN_Constants::PCN_EMPTY;
	m_WrtGrp_conStrAddtnlConf = PCN_Constants::PCN_EMPTY;
}

/******************************************************************
 * Name		   : ~PCNWriter_Group
 * Description : Virtual destructor 
 * Returns	   : None
 ******************************************************************/
      
PCNWriter_Group::~PCNWriter_Group()
{
	for(size_t i=0; i<m_vecPartDrivers.length();i++)
    {
        delete m_vecPartDrivers.at(i); 
		m_vecPartDrivers.at(i) = NULL;
    }
	m_vecPartDrivers.clear();

	delete m_partitionDoneMutex;
	delete m_totalReqRowsMutex;
}

/******************************************************************
 * Name		   : setConnectionDetails
 * Description : to set the details for connection to be used in preSQL and postSQL that are executed in init and deinit of group.
 ******************************************************************/

ISTATUS PCNWriter_Group::setConnectionDetails(const ISessionExtension* m_pSessExtn,IUINT32 m_partNum, const IUtilsServer* pUtils)
{
    //Getting the connection information
    IVector<const IConnectionRef*> vecConnRef; 
	IUString attrVal;
    if(m_pSessExtn->getConnections(vecConnRef,m_partNum) == IFAILURE)
    {
        if(m_itraceLevel >= SLogger::ETrcLvlTerse)
            m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_CONN_INFO_FAIL));
    }
    const IConnectionRef* pConnRef = vecConnRef.at(0);
    const IConnection*    pConn = NULL;
    if((pConn = (vecConnRef.at(0))->getConnection()) == NULL)
    {
        if(m_itraceLevel >= SLogger::ETrcLvlTerse)
            m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_CONN_INFO_FAIL));
		return IFAILURE;
    }   
	else
	{
		//Get connection extensions(user name and password)
		m_WrtGrp_uid     = pConn->getUserName();
		m_WrtGrp_pwd = pConn->getPassword(); 
		
		extractConnectionInformation(PLG_CONN_SCHEMANAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_WrtGrp_schemaname = attrVal;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_DATABASENAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_WrtGrp_databasename = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_SERVERNAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_WrtGrp_servername = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_PORT, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_WrtGrp_port = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_DRIVERNAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_WrtGrp_drivername = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_ADDITIONAL_CONFIGS, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_WrtGrp_conStrAddtnlConf = attrVal;
		}
	}
	return ISUCCESS;
	vecConnRef.clear(); 
}

void PCNWriter_Group::extractConnectionInformation(const ICHAR* attname, IUString& val,const IConnection* pConn, const IUtilsServer* pUtils)
{
	IUString printMsg=PCN_Constants::PCN_EMPTY;
	ICHAR* printAttrName = new ICHAR[1024];
	for ( int i = 0; i < strlen(attname); i++ )
		printAttrName[i] = attname[i];
	printAttrName[strlen(attname)] = '\0';
	if(IFAILURE == pConn->getAttribute(IUString(attname), val))
	{		
		printMsg.sprint(m_msgCatalog->getMessage(MSG_WRT_ERROR_READING_CONNATTR),printAttrName);
		m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,printMsg);
	}
	if(IFAILURE == pUtils->expandString(val, val, IUtilsServer::EVarParamSession, IUtilsServer::EVarExpandDefault))
	{
		printMsg.sprint(m_msgCatalog->getMessage(MSG_WRT_ERROR_READING_CONNATTR),printAttrName);
		m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,printMsg);
	}
}

ISTATUS PCNWriter_Group::allocEnvHandles(SQLHENV &m_WrtGrp_henv)
{
		SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_WrtGrp_henv);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			if(m_itraceLevel >= SLogger::ETrcLvlNormal)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(MSG_WRT_ALLOC_ENV_HANDLE_FAIL));
			}
			return IFAILURE;
		}
		
		rc = SQLSetEnvAttr(m_WrtGrp_henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, SQL_IS_INTEGER);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			if(m_itraceLevel >= SLogger::ETrcLvlNormal)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(MSG_WRT_SET_ENV_ATTR_FAIL));
			}
			return IFAILURE;
		}
		
		#ifndef WIN32
		rc = SQLSetEnvAttr(m_WrtGrp_henv, SQL_ATTR_APP_UNICODE_TYPE,(SQLPOINTER)SQL_DD_CP_UTF16, SQL_IS_INTEGER);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			if(m_itraceLevel >= SLogger::ETrcLvlNormal)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(MSG_CMN_SET_UNICODE_MODE_FAIL));
			}
			return IFAILURE;
		}
		#endif
		return ISUCCESS;
}

/********************************************************************
*Function       : openDatabase
*Description    : Allocate the environment and connection handles
and connect connect to the datasource using the
given username and password
*Input          : Pointer to the Output file where the result
is to be stored
********************************************************************/

ISTATUS PCNWriter_Group::openDatabase()
{
	ISTATUS iResult = ISUCCESS;
	SQLRETURN		rc;
	SQLWCHAR        sqlState[1024];
	SQLWCHAR        msgeTxt[1024];
	SQLINTEGER      ntveErr;
	SQLSMALLINT     txtLngth;
	SQLTCHAR * dsnSQLTChar;
	SQLTCHAR*  uidSQLTChar;
	SQLTCHAR*  pwdSQLTChar;
	IINT32 tempMsgCode;

	//loginTimeOutValue holds custom value of login timeout set by the Administrator.
	int loginTimeOutValue = GetConfigIntValue(LOGIN_TIMEOUT_FLAG);

	try
	{
		// Connection Info DSN and User
		IUString tempVariableChar;
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_CONN_INFO_DSN_USR_NAME),m_WrtGrp_dsn.getBuffer(),m_WrtGrp_uid.getBuffer());
		if(m_itraceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

		ISTATUS stat = allocEnvHandles(m_WrtGrp_henv);
		if(stat==IFAILURE)
		{
			throw IFAILURE;
		}
		else
		{
			rc = SQLAllocHandle(SQL_HANDLE_DBC,m_WrtGrp_henv, &m_WrtGrp_hdbc);
			if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			{
				tempMsgCode = MSG_WRT_ALLOC_DATABASE_CONN_FAIL;
				throw tempMsgCode;
			}
			else
			{
				//setting LOGIN_TIMEOUT connection attribute according to the loginTimeOutValue while connecting to the database
				if(loginTimeOutValue < 0)
				{
					//If the custom variable been set to a -ve value, then leaving the LOGIN_TIMEOUT to the backend(odbc.ini). 
				}
				else if(loginTimeOutValue == 0) 
				{
					//if custom variable is not defined, then LOGIN_TIMEOUT is to be set to preserve the earlier state.
					SQLSetConnectAttr(m_WrtGrp_hdbc, (SQLINTEGER)SQL_ATTR_LOGIN_TIMEOUT, (void*)30, 0);
				}
				else 
				{
					//Will make the LOGIN_TIMEOUT as the custom variable set by the Administrator if it is valid.
					SQLSetConnectAttr(m_WrtGrp_hdbc, (SQLINTEGER)SQL_ATTR_LOGIN_TIMEOUT, (void*)loginTimeOutValue, 0);
				}
				
				IUString printMsg=PCN_Constants::PCN_EMPTY;
				printMsg.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_CONNECTION_INFO),m_WrtGrp_drivername.getBuffer(),
					m_WrtGrp_uid.getBuffer(),
					m_WrtGrp_databasename.getBuffer(),
					m_WrtGrp_servername.getBuffer(),
					m_WrtGrp_port.getBuffer(),
					m_WrtGrp_conStrAddtnlConf.getBuffer());
				m_sLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,printMsg);

				IUString strConnectString=PCN_Constants::PCN_EMPTY;
				strConnectString.sprint("Driver=%s;Username=%s;Password=%s;\
					Database=%s;Servername=%s;Port=%s",
					m_WrtGrp_drivername.getBuffer(),
					m_WrtGrp_uid.getBuffer(),
					m_WrtGrp_pwd.getBuffer(),
					m_WrtGrp_databasename.getBuffer(),
					m_WrtGrp_servername.getBuffer(),
					m_WrtGrp_port.getBuffer());

				if ( !m_WrtGrp_schemaname.isEmpty() )
					strConnectString.append(IUString(";").getBuffer()).append(m_WrtGrp_schemaname.getBuffer());
				if ( !m_WrtGrp_conStrAddtnlConf.isEmpty() )
					strConnectString.append(IUString(";").getBuffer()).append(m_WrtGrp_conStrAddtnlConf.getBuffer());

                SQLTCHAR* sqlcharConnString = Utils::IUStringToSQLTCHAR(strConnectString);
				SQLTCHAR outConnString[1024];
				SQLSMALLINT cbConnStrOut;
				rc = SQLDriverConnect(m_WrtGrp_hdbc, NULL, sqlcharConnString, 
					strConnectString.getLength(), outConnString, sizeof(outConnString), 
					&cbConnStrOut, SQL_DRIVER_NOPROMPT);
                
                DELETEPTRARR <SQLTCHAR>(sqlcharConnString);

				if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				{
					SQLGetDiagRecW(SQL_HANDLE_DBC, m_WrtGrp_hdbc,1, (SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth);
					if( ntveErr == 108)
					{
						IUString nativeError;
						nativeError.sprint(m_msgCatalog->getMessage(MSG_CMN_NATIVE_ERR_CODE),(int)ntveErr);
						if(m_itraceLevel >= SLogger::ETrcLvlNormal)
							m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_CMN_INVLD_USRNAME_PASSWD));
					}
					else
					{
						if(txtLngth != 0)
						{
							tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_MSG),IUString(msgeTxt).getBuffer());
						}
						if(m_itraceLevel >= SLogger::ETrcLvlTerse)
							m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,IUString(msgeTxt));
					}
					if(m_itraceLevel >= SLogger::ETrcLvlTerse)
						m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_INTE_SER_NOT_CONN_NPC));

					throw IFAILURE;
				}
				else
				{
					//connection successfully established
					if(m_itraceLevel >= SLogger::ETrcLvlNormal)
						m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_CONN_SUCC_ESTABLISHED));
				}
			}
		}
	}
	catch(IINT32 ErrMsgCode)
	{
		if(m_itraceLevel >= SLogger::ETrcLvlNormal)
		{
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(ErrMsgCode));
		}
		iResult = IFAILURE;
	}
	catch (...)
	{
		iResult = IFAILURE;
	}
	return iResult;
}

void checkRet_WrtGrp(SQLRETURN rc,IUString query,SQLHSTMT hstmt,MsgCatalog* msgCatalog, SLogger *sLogger,IBOOLEAN& bCheckAnyfailedQuery, IUINT32 traceLevel)
{
	SQLWCHAR        sqlState[1024];
	SQLWCHAR        msgeTxt[1024];
	SQLINTEGER      ntveErr;
	SQLSMALLINT     txtLngth;
	IINT32          iStat;

	IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
	if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	{
		bCheckAnyfailedQuery=ITRUE;
		tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_EXEC_QRY_FAIL),query.getBuffer());
		if(traceLevel >= SLogger::ETrcLvlTerse)
			sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

		iStat = 1;
		SQLRETURN rc;
		while (1)
		{
			rc = SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt,iStat, (SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth);
			//EBF4308: the above call does not return SQL_NO_DATA but only SQL_ERROR after the error is read once
			if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				break;

			if(txtLngth != 0)
			{
				tempVariableChar.sprint(msgCatalog->getMessage(MSG_CMN_MSG),IUString(msgeTxt).getBuffer());
				if(traceLevel >= SLogger::ETrcLvlTerse)
					sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
			//printing the sqlState
			tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_SQL_STATE),IUString(sqlState).getBuffer());
			if(traceLevel >= SLogger::ETrcLvlNormal)
				sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
			iStat++;
		}
	}
	else
	{
		tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_EXEC_QRY),query.getBuffer());
		if(traceLevel >= SLogger::ETrcLvlNormal)
			sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
	}
}


/******************************************************************
 * Name		   : init
 * Description : Initialize the Group
 * Returns	   : init status
 ******************************************************************/

ISTATUS PCNWriter_Group::init(const IWrtGroupInit* )
{
	ISTATUS iResult = ISUCCESS;
    // For now, get the number of partitions from SSessionExtension
	try
	{
		m_itraceLevel = this->m_sLogger->getTrace();
		m_numPartitions = m_pSessExtn->getPartitionCount();
		IUINT32 extensionPluginId = m_pSessExtn->getSubType();
		if(extensionPluginId==PCN_Constants::OLD_NETEZZA_EXTENSION_SUBTYPE)
		{
			IINT32 tempMsgCode = MSG_CMN_OLD_EXTN;
			throw tempMsgCode;
		}

		IVector<const IMetaExtVal*> valList;
		IUINT32 nMetaExtn = 0;
		nMetaExtn = m_pTargetInstance->getTable()->getMetaExtValList(valList,
						3,
						PCN_Constants::VENDORID, PCN_Constants::MEDOMAINID);
		IUString sMMD_extn_value("");
		ILocale* pLocaleUTF8 = (IUtilsCommon::getInstance())->createLocale(106);
		for(IUINT32 i = 0; i < nMetaExtn; ++i)
		{
			const IMetaExtVal* pVal = valList.at(i);
			IUString extName = pVal->getName();
			if(extName == IUString(MMD_EXTN_WRITER_ATTR_LIST))
			{
				if(pVal->getValue(sMMD_extn_value) == ISUCCESS)
				{
					Utils::setSessionAttributesFromMMDExtn(m_pSessExtn, sMMD_extn_value, pLocaleUTF8, m_numPartitions);
				}
			}
		}
		
		IINT32      truncTab=0;
		if(m_pSessExtn->getAttributeInt(WRT_PROP_TRUN_TRG_TABLE_OPTION,truncTab) == IFAILURE)
		{
			truncTab= 0;
		}
		if(truncTab)
		{
			m_bTruncateTable=ITRUE;
		}

		//BEGIN New Feature for Table name and prefix override Ayan
		IUString tempErrMsg,tempTgtTable_toExpanded, tempTgtTable;
		const ISessionImpl* m_pISessionImpl = (ISessionImpl*)m_pSession;
		if( m_pSessExtn->getAttribute(WRT_PROP_TABLENAME,tempTgtTable_toExpanded) == IFAILURE)
		{
			m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,tempErrMsg);
			return IFAILURE;
		}

		if(!tempTgtTable_toExpanded.isEmpty())
		{
			IUString tempTgtTableExpanded = "";
			if(m_pUtils->expandString(tempTgtTable_toExpanded,tempTgtTableExpanded,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
			{
				tempErrMsg.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),tempTgtTable_toExpanded.getBuffer());
				if(m_sLogger->getTrace() >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempErrMsg).getBuffer());
				return IFAILURE;
			}
			tempErrMsg = "";
			if(m_pUtils->expandString(tempTgtTableExpanded,tempTgtTable,IUtilsServer::EVarParamMapping,IUtilsServer::EVarExpandDefault)==IFAILURE)
			{
				tempErrMsg.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),tempTgtTableExpanded.getBuffer());
				if(m_sLogger->getTrace() >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempErrMsg).getBuffer());
				return IFAILURE;
			}
		}

		//For Schema
		IUString tempTgtSchema, tempTgtSchema_toExpanded;
		tempErrMsg = PCN_Constants::PCN_EMPTY;
		if( m_pSessExtn->getAttribute(WRT_PROP_TABLEPREFIX,tempTgtSchema_toExpanded) == IFAILURE)
		{
			m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,tempErrMsg);
			return IFAILURE;
		}
		if(!tempTgtSchema_toExpanded.isEmpty())
		{
			IUString tempTgtSchemaExpanded = "";
			if(m_pUtils->expandString(tempTgtSchema_toExpanded,tempTgtSchemaExpanded,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
			{
				tempErrMsg.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),tempTgtSchema_toExpanded.getBuffer());
				if(m_sLogger->getTrace() >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempErrMsg).getBuffer());
				return IFAILURE;
			}
			tempErrMsg = "";
			if(m_pUtils->expandString(tempTgtSchemaExpanded,tempTgtSchema,IUtilsServer::EVarParamMapping,IUtilsServer::EVarExpandDefault)==IFAILURE)
			{
				tempErrMsg.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),tempTgtSchemaExpanded.getBuffer());
				if(m_sLogger->getTrace() >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempErrMsg).getBuffer());
				return IFAILURE;
			}
		}

		if(!tempTgtSchema.isEmpty())
		{
			tempTgtSchema.append(IUString("\".\""));
			m_tempTableName = tempTgtSchema;
			m_tempTableName.append(IUString("pcn_temp_"));
		}
		else
		{
			m_tempTableName = "pcn_temp_";
		}
		if ( !tempTgtTable.isEmpty() )
			m_tempTableName.append(tempTgtTable);
		else
			m_tempTableName.append(m_pTargetInstance->getName());		

		//END New Feature for Table name and prefix override Ayan
		IUString tempStr;
		tempStr.sprint("_%d_%lld",getpid(),time(NULL));
		m_tempTableName.append(tempStr);		

		for(size_t i=0; i<m_numPartitions; i++)
		{
			// Ideally you should be getting the SSessExtension for this partition 
			// and pass that to the partition constructor, which in turn will give the 
			// connection information
			PCNWriter_Partition* pPartDriver = new PCNWriter_Partition(m_pSession, m_pTargetInstance, m_nTgtNum, 
																	m_nGrpNum, i, m_pPluginDriver, m_sLogger,
																	m_pSessExtn,m_pUtils,m_msgCatalog,m_pLogger,m_pluginRequestedRows,m_pluginAppliedRows,m_pluginRejectedRows, this);
			assert(pPartDriver);
			// Connections to be provided to pmserver
			m_vecPartDrivers.insert(pPartDriver);
		}

		IINT32 prop_wrt_pre_sql;
		prop_wrt_pre_sql=WRT_PROP_PRE_SQL;
		IUString wrtPreSQL = PCN_Constants::PCN_EMPTY;
		IINT32 m_nPartNum = 0;

		if(m_pSessExtn->getAttribute(prop_wrt_pre_sql,wrtPreSQL,m_nPartNum) == IFAILURE)
		{
			wrtPreSQL = PCN_Constants::PCN_EMPTY;
		}
		if(!wrtPreSQL.isEmpty())
		{
			IUString msgPrint = PCN_Constants::PCN_EMPTY;
			msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING),IUString("PreSQL").getBuffer(), wrtPreSQL.getBuffer());
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,msgPrint.getBuffer());			

			IINT32 tempMsgCode;
			SQLRETURN   rc;
			SQLHSTMT  hstmt;		
			SQLTCHAR* iuStrToSQLChar;

			setConnectionDetails(m_pSessExtn, 0, m_pUtils);
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,this->m_msgCatalog->getMessage(MSG_WRT_EXEC_PRESQL_INFO));

			if(openDatabase() == IFAILURE)
				throw IFAILURE;

			iuStrToSQLChar = Utils::IUStringToSQLTCHAR(wrtPreSQL);
			SQLAllocStmt(m_WrtGrp_hdbc, &hstmt);
			rc = SQLExecDirect(hstmt,iuStrToSQLChar,SQL_NTS);
			IBOOLEAN dummyVar = IFALSE;
			checkRet_WrtGrp(rc,iuStrToSQLChar,hstmt,m_msgCatalog,m_sLogger,dummyVar,m_itraceLevel);

			DELETEPTRARR <SQLTCHAR>(iuStrToSQLChar);
		}
	}
	catch(IINT32 ErrMsgCode)
    {
		IUINT32 tmpTrace = m_sLogger->getTrace();
        if(tmpTrace >= SLogger::ETrcLvlTerse)
            m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(ErrMsgCode));

        iResult = IFAILURE;
    }  
	catch(...)
	{
		iResult = IFAILURE;
	}
    return iResult;
}

/*********************************************************************
 * Name		   : getPartitionDriver
 * Description : Get the Partition level handle with the given
				 Partition ID      
 * Returns	   : Returns the Partition level handle 
				 with the given Partition ID      
 *********************************************************************/   

PWrtPartitionDriver* PCNWriter_Group::getPartitionDriver(IUINT32 partIndex)
{
    if( partIndex >= m_vecPartDrivers.length())
    {
		if(this->m_itraceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_REQST_PART_DRIVER_EXCED_LIMIT));
		return NULL;                  
    }
    return m_vecPartDrivers.at(partIndex);
}
     
/********************************************************************
 * Name		   : deinit
 * Description : Deinit Method of the Group
 * Returns	   : deinit status
 ********************************************************************/

ISTATUS PCNWriter_Group::deinit(ISTATUS status)
{	
	IINT32 prop_wrt_post_sql;
	prop_wrt_post_sql=WRT_PROP_POST_SQL;
	IUString wrtPostSQL = PCN_Constants::PCN_EMPTY;
	IINT32 m_nPartNum = 0;

	if(m_pSessExtn->getAttribute(prop_wrt_post_sql,wrtPostSQL,m_nPartNum) == IFAILURE)
	{
		wrtPostSQL = PCN_Constants::PCN_EMPTY;
	}
	if(!wrtPostSQL.isEmpty())
	{
		IUString msgPrint = PCN_Constants::PCN_EMPTY;
		msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING),IUString("PostSQL").getBuffer(), wrtPostSQL.getBuffer());
		m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,msgPrint.getBuffer());

		IINT32 tempMsgCode;
		SQLRETURN   rc;
		SQLHSTMT  hstmt;	
		SQLTCHAR* iuStrToSQLChar;

		setConnectionDetails(m_pSessExtn, 0, m_pUtils);
		m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,this->m_msgCatalog->getMessage(MSG_WRT_EXEC_POSTSQL_INFO));
		
		if(openDatabase() == IFAILURE)
		 throw IFAILURE;

		iuStrToSQLChar = Utils::IUStringToSQLTCHAR(wrtPostSQL);
		SQLAllocStmt(m_WrtGrp_hdbc, &hstmt);
		rc = SQLExecDirect(hstmt,iuStrToSQLChar,SQL_NTS);
		IBOOLEAN dummyFlagVar = IFALSE;
		checkRet_WrtGrp(rc,iuStrToSQLChar,hstmt,m_msgCatalog,m_sLogger,dummyFlagVar,m_itraceLevel);

		DELETEPTRARR <SQLTCHAR>(iuStrToSQLChar);
	}
	return ISUCCESS;
}  

IBOOLEAN PCNWriter_Group::isTempTableCreated()
{
	return m_b_tempTableCreated;
}

void PCNWriter_Group::setIfTempTableCreated(IBOOLEAN b)
{
	m_b_tempTableCreated=b;
}

IINT32 PCNWriter_Group::getNoPartLoadedDataTemp()
{
	return m_noPartLoadedDataTemp;
}
void PCNWriter_Group::incrementNoPartLoadedDataTemp()
{
	m_noPartLoadedDataTemp++;
}

void PCNWriter_Group::lockPartDoneMutex()
{
	m_partitionDoneMutex->lock();
}
void PCNWriter_Group::unlockPartDoneMutex()
{
	m_partitionDoneMutex->unlock();
}

IINT32 PCNWriter_Group::getPartitionCount()
{
	return m_numPartitions;
}
IINT32 PCNWriter_Group::getPartitionsInitializedCount()
{
	return m_numPartitionsInitialized;
}
void PCNWriter_Group::incrementPartitionsInitializedCount()
{
	m_numPartitionsInitialized++;
}

IUINT32 PCNWriter_Group::getTotalReqRows()
{
	 return m_totalReqRows;
}

void PCNWriter_Group::incrementTotalReqRows(IUINT32 totalReqRows)
{
	m_totalReqRowsMutex->lock();
	m_totalReqRows+=totalReqRows;
	m_totalReqRowsMutex->unlock();
}

void PCNWriter_Group::setIfTruncateTable(IBOOLEAN b)
{
	m_bTruncateTable=b;
}
IBOOLEAN PCNWriter_Group::getTruncateTable()
{
	return m_bTruncateTable;
}

IUString PCNWriter_Group::getTempTableName()
{
	return m_tempTableName;
}