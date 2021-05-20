/*****************************************************************
* ----- Persistent Systems Pvt. Ltd., Pune ----------- 
* Copyright (c) 2006
* All rights reserved.
* Name        : PCNReader_TConnection.cpp
* Description : This is a user-defined class which is used to keep the Netezza connection
information. Connection to NPS is established and released using this class.
ODBC APIs are used for connection management.
* Author      : Rashmi Varshney 03-11-06                                 
*****************************************************************/

#include "pcnreader_tconnection.hpp"   

#ifndef WIN32
#ifndef SQL_ATTR_APP_UNICODE_TYPE
#define SQL_ATTR_APP_UNICODE_TYPE 1064
#endif
#ifndef SQL_DD_CP_UTF16
#define SQL_DD_CP_UTF16 1
#endif
#endif


/****************************************************************
Constructor
****************************************************************/
PCN_TConnection::PCN_TConnection(void)
{
    this->traceLevel = this->m_pLogger->getTrace();
    this->m_hDbc = NULL;
    this->m_hEnv = NULL;
}

PCN_TConnection::PCN_TConnection ( SLogger* l,MsgCatalog* pMsgCatalog) :
m_pLogger (l),
m_pMsgCatalog(pMsgCatalog)
{
    this->m_hDbc = NULL;
    this->m_hEnv = NULL;
    this->traceLevel = this->m_pLogger->getTrace();
}
/*********************************************************************
* Function       : setDsnUidPwd
* Description    : Set the DSN name , username and password
* Input          :         1. dsn -> Datasource Name
*                          2. username -> Username
*                          3. password -> Password
*********************************************************************/

ISTATUS PCN_TConnection::setConnectionDetails(const ISessionExtension* m_pSessExtn,IUINT32 m_partNum, const IUtilsServer* pUtils)
{
    //Getting the connection information
    IVector<const IConnectionRef*> vecConnRef; 
	IUString attrVal;
    if(m_pSessExtn->getConnections(vecConnRef,m_partNum) == IFAILURE)
    {
        if(this->traceLevel >= SLogger::ETrcLvlTerse)
            m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_CONN_INFO_FAIL));
    }
    const IConnectionRef* pConnRef = vecConnRef.at(0);
    const IConnection*    pConn = NULL;
    if((pConn = (vecConnRef.at(0))->getConnection()) == NULL)
    {
        if(this->traceLevel >= SLogger::ETrcLvlTerse)
            m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_CONN_INFO_FAIL));
		return IFAILURE;
    }   
	else
	{
		//Get connection extensions(user name and password)
		m_uid     = pConn->getUserName();
		m_pwd = pConn->getPassword(); 
		
		extractConnectionInformation(PLG_CONN_SCHEMANAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_schemaname = attrVal;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_DATABASENAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_databasename = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_SERVERNAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_servername = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_PORT, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_port = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_DRIVERNAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_drivername = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_ADDITIONAL_CONFIGS, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_connectStrAdditionalConfigurations = attrVal;
		}
	}
	return ISUCCESS;
	vecConnRef.clear(); 
}

void PCN_TConnection::extractConnectionInformation(const ICHAR* attname, IUString& val,const IConnection* pConn, const IUtilsServer* pUtils)
{
	IUString printMsg=PCN_Constants::PCN_EMPTY;
	ICHAR* printAttrName = new ICHAR[1024];
	for ( int i = 0; i < strlen(attname); i++ )
		printAttrName[i] = attname[i];
	printAttrName[strlen(attname)] = '\0';
	if(IFAILURE == pConn->getAttribute(IUString(attname), val))
	{		
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_ERROR_READING_CONNATTR),printAttrName);
		m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,printMsg);
	}
	if(IFAILURE == pUtils->expandString(val, val, IUtilsServer::EVarParamSession, IUtilsServer::EVarExpandDefault))
	{
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_ERROR_READING_CONNATTR),printAttrName);
		m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,printMsg);
	}
}


/****************************************************************
Allocate the environment and connection handles and establish
the connection with the Netezza database using the DSN information 
Pointer to the Output file where the result is to be stored
****************************************************************/
ISTATUS PCN_TConnection::connect()
{
    SQLRETURN		rc;
    SQLWCHAR        sqlState[1024];
    SQLWCHAR        msgeTxt[1024];
    SQLINTEGER      ntveErr;
    SQLSMALLINT     txtLngth;
	
	//loginTimeOutValue holds custom value of login timeout set by the Administrator.
	int loginTimeOutValue = GetConfigIntValue(LOGIN_TIMEOUT_FLAG); 

    rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_hEnv);
    if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
    {
        if(this->traceLevel >= SLogger::ETrcLvlTerse)
            m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_ENV_ALOC_FAIL));
        return IFAILURE;
    }
    else
    {
        rc = SQLSetEnvAttr(m_hEnv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, SQL_IS_INTEGER);
        if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        {
            if(this->traceLevel >= SLogger::ETrcLvlTerse)
                m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_SET_ENV_ATTR_FAIL));
            return IFAILURE;
        }
        else
        {
			#ifndef WIN32
			rc = SQLSetEnvAttr(m_hEnv, SQL_ATTR_APP_UNICODE_TYPE,(SQLPOINTER)SQL_DD_CP_UTF16, SQL_IS_INTEGER);
			if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			{
	            if(this->traceLevel >= SLogger::ETrcLvlTerse)
		            m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_CMN_SET_UNICODE_MODE_FAIL));
			    return IFAILURE;
			}
			#endif

            rc = SQLAllocHandle(SQL_HANDLE_DBC,m_hEnv, &m_hDbc);
            if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            {
                if(this->traceLevel >= SLogger::ETrcLvlTerse)
                    m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_DB_HANDLE_FAIL));
                return IFAILURE;
            }
            else
            {
                //setting LOGIN_TIMEOUT connection attribute according to the loginTimeOutValue while connecting to the database
				if(loginTimeOutValue < 0)
				{
					//If the custom variable been set to a -ve value, then leaving the LOGIN_TIMEOUT to the backend(odbc.ini)
				}
				else if(loginTimeOutValue == 0) 
				{
					//if custom variable is not defined, then LOGIN_TIMEOUT is to be set to preserve the earlier state.
					SQLSetConnectAttr(m_hDbc, (SQLINTEGER)SQL_ATTR_LOGIN_TIMEOUT, (void*)5, 0);
				}
				else 
				{
					//Will make the LOGIN_TIMEOUT as the custom variable set by the Administrator if it is valid.
					SQLSetConnectAttr(m_hDbc, (SQLINTEGER)SQL_ATTR_LOGIN_TIMEOUT, (void*)loginTimeOutValue, 0);
				}

				IUString printMsg=PCN_Constants::PCN_EMPTY;
				printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_CONNECTION_INFO),m_drivername.getBuffer(),
					m_uid.getBuffer(),
					m_databasename.getBuffer(),
					m_servername.getBuffer(),
					m_port.getBuffer(),
					m_connectStrAdditionalConfigurations.getBuffer());
				m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,printMsg);

				IUString strConnectString=PCN_Constants::PCN_EMPTY;
				strConnectString.sprint("Driver=%s;Username=%s;Password=%s;\
					Database=%s;Servername=%s;Port=%s",
					m_drivername.getBuffer(),
					m_uid.getBuffer(),
					m_pwd.getBuffer(),
					m_databasename.getBuffer(),
					m_servername.getBuffer(),
					m_port.getBuffer());

				if ( !m_schemaname.isEmpty() )
					strConnectString.append(IUString(";").getBuffer()).append(m_schemaname.getBuffer());
				if ( !m_connectStrAdditionalConfigurations.isEmpty() )
					strConnectString.append(IUString(";").getBuffer()).append(m_connectStrAdditionalConfigurations.getBuffer());

                SQLTCHAR* sqlcharConnString = Utils::IUStringToSQLTCHAR(strConnectString);
				SQLTCHAR outConnString[1024];
				SQLSMALLINT cbConnStrOut;
				rc = SQLDriverConnect(m_hDbc, NULL, sqlcharConnString, 
					strConnectString.getLength(), outConnString, sizeof(outConnString), 
					&cbConnStrOut, SQL_DRIVER_NOPROMPT);
                
                DELETEPTRARR <SQLTCHAR>(sqlcharConnString);

                if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
                {
                    SQLGetDiagRec(SQL_HANDLE_DBC, m_hDbc,1, (SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth);

                    m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlVerboseData,IUString(sqlState));

                    if(txtLngth != 0)
                    {   
                        //Included to make the error message more generic without exposing user name (Works for windows)
                        if( ntveErr == 108)
                        {
                            IUString nativeError;
							nativeError.sprint(m_pMsgCatalog->getMessage(MSG_CMN_NATIVE_ERR_CODE),(int)ntveErr);
                            if(this->traceLevel >= SLogger::ETrcLvlNormal)
                                    m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_CMN_INVLD_USRNAME_PASSWD));   
                        }
                        else
                        {
                            if(this->traceLevel >= SLogger::ETrcLvlTerse)
                                m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,IUString(msgeTxt));
                        }
                    }
                    else
                    {
                        if(this->traceLevel >= SLogger::ETrcLvlTerse)
                            m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_SQL_CONNECT_FAIL));
                    }
                    return IFAILURE;
                }
                else
                {

                    if(this->traceLevel >= SLogger::ETrcLvlNormal)
                        m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_CONN_ESTABLISHED));
                }
            }
        }
    }
    return ISUCCESS;
}


/****************************************************************
Destructor
****************************************************************/
PCN_TConnection::~PCN_TConnection(void)
{
   
}

/****************************************************************
Get the environment handle
****************************************************************/
const SQLHENV& PCN_TConnection::getEnvHandle(void)
{
    return m_hEnv;
}

/****************************************************************
Get the Connection handle
****************************************************************/
const SQLHDBC& PCN_TConnection::getODbcHandle(void)
{
    return m_hDbc;
}


void PCN_TConnection::freeHandle()
{
	SQLRETURN rc;

    if(this->m_hDbc != NULL)
    {   
        rc = SQLDisconnect(this->m_hDbc);
        if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        {
            if(this->traceLevel >= SLogger::ETrcLvlTerse)
                m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_SQL_DISCONNECT_FAIL));
        }
        rc =  SQLFreeHandle(SQL_HANDLE_DBC, this->m_hDbc); 
        if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        {
            if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_SQL_FREE_HANDLE_FAIL_DBC));
            // Error in executing SQL free stmt
        }
        this->m_hDbc = NULL;
    }

    if(this->m_hEnv != NULL)
    {
        rc = SQLFreeHandle(SQL_HANDLE_ENV,this->m_hEnv); 
        if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        {
            if(this->traceLevel >= SLogger::ETrcLvlTerse)
                m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_SQL_FREE_HANDLE_FAIL_ENV));
            // Error in executing SQL free stmt
        }
        this->m_hEnv = NULL;
    }
}
