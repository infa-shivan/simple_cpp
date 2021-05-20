/*****************************************************************
 * ----- Persistent Systems Pvt. Ltd., Pune ----------- 
 * Copyright (c) 2006
 * All rights reserved.
 * Name        : PCNReader_TConnection.hpp
 * Description : This is a user-defined class which is used to keep the Netezza connection
				 information. Connection to NPS is established and released using this class.
				 ODBC APIs are used for connection management.
 * Author      : Rashmi Varshney 03-11-06				 				 
 *****************************************************************/

#ifndef  __PCN_READER__TCONNECTION_
#define __PCN_READER__TCONNECTION_

// ODBC lib files

//#include "NetMsgDef.hpp"
#include "msgcatalog.hpp"
#include "slogger.hpp"
#include "utilsrv.hpp"

class PCN_TConnection
{
public:
    
	/****************************************************************
		Constructor
	****************************************************************/
		PCN_TConnection(void);
		PCN_TConnection(SLogger* pLogger, MsgCatalog* pMsgCatalog);
   
		
	/****************************************************************
		Allocate the environment and connection handles and establish
		the connection with the Netezza database using the DSN information 
		Pointer to the Output file where the result is to be stored
	****************************************************************/
    ISTATUS connect();

	/****************************************************************
		Destructor
	****************************************************************/
	~PCN_TConnection(void);

	/****************************************************************
		Get the environment handle
	****************************************************************/
    const SQLHENV& getEnvHandle(void);

	/****************************************************************
		Get the Connection handle
	****************************************************************/
    const SQLHDBC& getODbcHandle(void);

	/****************************************************************
		Set the DSN name , username and password
	****************************************************************/
	ISTATUS setConnectionDetails(const ISessionExtension*	m_pSessExtn,IUINT32	m_npartNum, const IUtilsServer* pUtils);

	void extractConnectionInformation(const ICHAR* attname, IUString& val,const IConnection* pConn, const IUtilsServer* pUtils);

	void freeHandle();

private:
    
    SQLHENV				m_hEnv;			// Environment Handle
    
    SQLHDBC				m_hDbc;			// Connection Handle
	   
	IUString			m_dsn ;		//dsn name

	IUString			m_uid ;		//user name		

	IUString			m_pwd ;		//password

	IUString			m_databasename;

	IUString			m_schemaname;

	IUString			m_servername;

	IUString			m_port;

	IUString			m_drivername;

	IUString			m_connectStrAdditionalConfigurations;

	MsgCatalog*         m_pMsgCatalog;

	SLogger*			m_pLogger;	// Used to log messages in the session log

	IUINT32				traceLevel;
};
#endif
