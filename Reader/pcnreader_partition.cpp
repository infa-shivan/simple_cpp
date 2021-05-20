#include "pcnreader_partition.hpp"
#include "sdksrvin/iutlsrvi.hpp"
#include <algorithm>
#include <map>

void checkRet(SQLRETURN rc,IUString query,SQLHSTMT hstmt,MsgCatalog*	msgCatalog, SLogger *sLogger,IUINT32 traceLevel)
{
	SQLWCHAR        sqlState[1024];
	SQLWCHAR        msgeTxt[1024];
	SQLINTEGER      ntveErr;
	SQLSMALLINT     txtLngth;
	IINT32          iStat;

	IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
	if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	{			
		if(traceLevel >= SLogger::ETrcLvlTerse)
		{
			tempVariableChar.sprint(msgCatalog->getMessage(MSG_RDR_QRY_EXEC_FAIL),query.getBuffer());
			if(traceLevel >= SLogger::ETrcLvlTerse)
				sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		}
		iStat = 1;
		while ((SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt,iStat, (SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth)) != SQL_NO_DATA)
		{
			if(txtLngth != 0)
			{

				tempVariableChar.sprint(msgCatalog->getMessage(MSG_CMN_MSG),IUString(msgeTxt).getBuffer());				
				if(traceLevel >= SLogger::ETrcLvlTerse)
					sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
			iStat++;
		}		
	}
	else
	{
		if(traceLevel >= SLogger::ETrcLvlNormal)
		{
			tempVariableChar.sprint(msgCatalog->getMessage(MSG_RDR_EXEC_QRY),query.getBuffer());					
			if(traceLevel >= SLogger::ETrcLvlNormal)
				sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
		}
	}
}
/*
C Function assigned to thread which lodes data into pipe in case of Windows ONLY
*/
void* runThreadQuery( LPVOID lpvParam )
{
	SQLRETURN   rc;
    ThreadArgs* threadArgs = (ThreadArgs*)lpvParam;    

	SLogger *sLogger=threadArgs->sLogger;
	MsgCatalog*	msgCatalog=threadArgs->msgCatalog;

	ISTATUS status=threadArgs->pUtils->initThreadStructs(threadArgs->threadName);	    

	if (!threadArgs->insertintoExtQuery.isEmpty())
	{
		SQLTCHAR* insertExtQuery = Utils::IUStringToSQLTCHAR(threadArgs->insertintoExtQuery);
		rc = SQLExecDirect(threadArgs->hstmt,insertExtQuery,SQL_NTS);               
		checkRet(rc,threadArgs->insertintoExtQuery,threadArgs->hstmt,msgCatalog,sLogger,threadArgs->traceLevel);	
		DELETEPTRARR <SQLTCHAR>(insertExtQuery);	
		if(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
			threadArgs->retInsertintoExtQuery=ISUCCESS;
		else
		{
			if(IFALSE==threadArgs->pcnReaderObj->isPipeOpened())
			{
				ICHAR * fullPath = Utils::IUStringToChar(threadArgs->pipeFullPath);
				IINT32 fd = open(fullPath, O_WRONLY);
				FILE *fp = (fd != -1 ? fdopen(fd, "wb") : NULL);
				if(fp!=NULL)
					fclose(fp);				
				DELETEPTRARR <ICHAR>(fullPath);
			}
		}
	}

	threadArgs->pUtils->deinitThreadStructs();
	return NULL;
}


// constructor for the structure that is used for passing arguments 
// to the thread function
ThreadArgs::ThreadArgs( IUString insertintoExtQuery,                                             
					   IUString threadName,
                       SQLHSTMT hstmt,
                       ISTATUS retInsertintoExtQuery,                       
					   const IUtilsServer *pUtils,
					   SLogger *sLogger,
					   MsgCatalog*	msgCatalog,
					   IUINT32 traceLevel)
{
	this->insertintoExtQuery = insertintoExtQuery;    
	this->threadName=threadName;
    this->hstmt = hstmt;
    this->retInsertintoExtQuery=retInsertintoExtQuery;
    this->pUtils=pUtils;
	this->sLogger=sLogger;
	this->msgCatalog=msgCatalog;
	this->traceLevel=traceLevel;
}


/****************************************************
* Name : PCNReader_Partition
* Description : Constructor 
****************************************************/
PCNReader_Partition::PCNReader_Partition(const ISession* s, 
                                         const IUtilsServer* u,
                                         SLogger* l,
                                         IUINT32 dsqNum, 
                                         const IAppSQInstance* dsq,
                                         IUINT32 partNum,
										 MsgCatalog* pMsgCatalog, PCNReader_DSQ *pDSQ,std::map<IINT32, IUString> arMetadataMap) :
m_pSession(s),
m_pUtils(u),
m_pLogger (l),
m_nDsqNum(dsqNum),
m_pDSQ (dsq),
m_nPartNum (partNum),
m_part_MetadataMap(arMetadataMap),
m_pMsgCatalog(pMsgCatalog)
{
    m_pMetaExtension        = new NetMetaExtension();       // Memory Deallocation >> this->~PCNReader_Partition
    m_pSessionAttributes    = new NetSessionAttributes();       // Memory Deallocation >> this->~PCNReader_Partition

    m_dateUtils             = m_pUtils->getUtilsCommon()->getUtilsDate();
    m_threadArgs            = new ThreadArgs(PCN_Constants::PCN_EMPTY, PCN_Constants::PCN_EMPTY,NULL, IFAILURE,NULL,NULL,NULL,0);
    this->m_fHandle         = NULL;
	m_pLocale_Latin9        = NULL;
	m_pLocale_UTF8          = NULL;
	m_pLocale_Latin1        = NULL;
    this->m_nDTMFlag        = 0 ;
	this->m_parentDSQDriver=pDSQ;
	this->bIsPipeOpen=IFALSE;
	this->m_threadArgs->pcnReaderObj=this;
	this->m_bIsRunMethodCalled=IFALSE;
	this->m_pISessionImpl=(ISessionImpl*)m_pSession;	
	this->m_pTempStr=NULL;
	this->m_bIsEscapeChar=IFALSE;
	this->m_pEscapeChar=NULL;
	this->m_colDoubleDimension=NULL;
	this->m_colmnAttr=NULL;
	this->m_pDataBuffer=NULL;
	//BEGIN FIX FOR INFA392252 Partition-ID resolution
	m_uiPartitionID = m_pSession->getExtension(eWIDGETTYPE_DSQ,m_nDsqNum)->getPartitionId(m_nPartNum);
	m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlTerse,IUString("Set the Partition-ID"));
	//END FIX FOR INFA392252 Partition-ID resolution
	m_overridenPipePath = PCN_Constants::PCN_EMPTY;
}

/****************************************************
* Name : ~PCNReader_Partition
* Description : Destructor 
****************************************************/
PCNReader_Partition::~PCNReader_Partition()
{
    try{
		DELETEPTR <NetMetaExtension>(m_pMetaExtension);
		DELETEPTR <NetSessionAttributes>(m_pSessionAttributes);
		DELETEPTR <PCN_TConnection>(m_connection);
		DELETEPTR <ThreadArgs>(m_threadArgs);

    }catch(...){

    }
}

/********************************************************************
* Name : init
* Description : Called by the SDK framework after this object is 
*               created to allow partition-specific initializations.
*               Connection is established with the Netezza Domino Server
*               and checking for form or view is done.
* Returns : ISUCCESS if initialized else IFAILURE 
*********************************************************************/
ISTATUS PCNReader_Partition::init()
{
    ISTATUS iResult = ISUCCESS;
    extern const ICHAR* VERSION;      
    IUString    l_srcSrv;               //source server name
    IUString    l_srcDb;                //source database name 
    IVector<IMetaExtVal*> l_metaExtList;
    ITable* l_srcTab;
    IUString name;
    IINT32 tempMsgCode;

    try 
	{
		m_pSessExtn = m_pSession->getSourceExtension(m_nDsqNum, 0);
		m_numPartitions=m_pSessExtn->getPartitionCount();
        this->m_nTraceLevel = this->m_pLogger->getTrace();

		//Creates Locale for Latin9 and UTF8 code page id.
		if(this->createLocale() == IFAILURE)
		{
			tempMsgCode = MSG_RDR_CREATE_LOCALE_FAIL;
			throw tempMsgCode; 
		}


		//BEGIN New Feature for Table name and prefix override Ayan
        if(m_pDSQ->getSourceList(m_l_srcList) == IFAILURE)
            throw IFAILURE;

        //Get Session Attributes
        if(getSessionAttributes()==IFAILURE)
            throw IFAILURE;

        l_srcTab = (ITable*)m_l_srcList.at(0)->getTable();

        name = l_srcTab->getName();

		if ( !m_l_overRiddenTable.at(0).isEmpty() )
		{
			this->m_srcTblName = m_l_overRiddenTable.at(0);
		}
		else
		{
			this->m_srcTblName = name;
		}
		//END New Feature for Table name and prefix override Ayan
        //clear source list
		//BEGIN New Feature for Table name and prefix override Ayan
		if ( m_l_srcList.length() != 0 )
		{
			for ( IUINT32 itr = 0; itr < m_l_srcList.length(); itr++ )
			{
				IUString name(m_l_srcList.at(itr)->getTable()->getName());
				IUString str("Table Name ");
				str.append(name);
				m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlTerse,str);
			}
		}
		
        //l_srcList.clear();
		//END New Feature for Table name and prefix override Ayan

        if( this->UpdateFieldDetail() == IFAILURE)
		{
           tempMsgCode = MSG_RDR_FLD_INF_FAIL;
           throw tempMsgCode;
        }

        if(this->GenerateFinalQuery() == IFAILURE)
		{
           tempMsgCode = MSG_RDR_QRY_GEN_FAIL;
           throw tempMsgCode;
        }

		
        this->m_connection = new PCN_TConnection(this->m_pLogger,this->m_pMsgCatalog);
		if ( this->m_connection->setConnectionDetails(m_pSessExtn,m_nPartNum,m_pUtils) == IFAILURE )
		{
			tempMsgCode = MSG_RDR_ERROR_READING_MANDATORY_CONN_ATTRS;
			throw tempMsgCode;
		}

        if(this->m_connection->connect() == IFAILURE)
		{
           tempMsgCode = MSG_RDR_SER_CONN_NPC_FAIL;
           throw tempMsgCode;           
        }

        //clear metadata extension vector
        l_metaExtList.clear();

        //Used in case Enable High Precision is set 
        m_pDecimal18Utils = this->m_pUtils->getUtilsCommon()->getUtilsDecimal18();
        m_pDecimal28Utils = this->m_pUtils->getUtilsCommon()->getUtilsDecimal28();
       

        		
			if(this->createPipe() == IFAILURE)
			{
				tempMsgCode = MSG_RDR_CREATE_PIPE_FAIL;
				throw tempMsgCode;
			}

			if( this->CreateExternalTable() == IFAILURE)
			{
				tempMsgCode = MSG_RDR_CREATE_EXT_TABLE_FAIL;
				throw tempMsgCode;
			}
			
			m_pNullValue = Utils::IUStringToChar(this->m_nullValue);

			calMaxRowLength();

		/** Creates a joinable thread, if bIsDetached is not specified or 
		  IFALSE. Handle is the returned thread handle.
			*/
			m_iThreadMgr.spawn(&runThreadQuery,m_hThread,m_threadArgs,IFALSE);	       			
	        
			#ifdef WIN32 //ined(DOS) || defined(OS2) || defined(NT) || defined(WIN32) || defined (MSDOS)
			{
				ConnectNamedPipe(m_hPipe, NULL);
			}
		   #endif

			if( this->openWRPipe() == IFAILURE)
				throw IFAILURE;

		
    }
    catch(IINT32 ErrMsgCode)
    {
        if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
		{
            m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse, 
            m_pMsgCatalog->getMessage(ErrMsgCode));
        }
        iResult = IFAILURE;
    }    
    catch (...)
    {
        iResult = IFAILURE;
    }

    if(IFAILURE == iResult)
	{
        if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
			m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_PARTITION_INIT_FAIL));
    } 
	return iResult;
}

/********************************************************************
* Name        : run
* Description : This is the main entry point into the plug-in for 
*               reading the data. It is called by the SDK framework 
*               and returns only after all the data has been read.
* Returns     : IFAILURE if an error occurs while reading data and 
*               ISUCCESS otherwise
********************************************************************/

ISTATUS PCNReader_Partition::run(IRdrBuffer* const* const rdbuf)
{   
	m_bIsRunMethodCalled=ITRUE;
	if(IFAILURE==initRunMethod(rdbuf))
	{
		return IFAILURE;
	}

	if(IFAILURE==readAndProcessData(rdbuf))
	{
		return IFAILURE;
	}
	return ISUCCESS;
}


/********************************************************************
* Name        : deinit
* Description : Called by the SDK to mark the end of the partition's
*               lifetime. The session run status until this point is
*               passed as an input. Connection to the Essbase Server 
*               is released.
* Returns     : ISUCCESS if de-initialized
********************************************************************/
ISTATUS PCNReader_Partition::deinit(ISTATUS runStatus)
{  
	DELETEPTRARR <ColumnAttr>(m_colmnAttr);

	if(this->m_colDoubleDimension)
	{
		for(IUINT32 cnt =0 ; cnt< m_nNumOfConnectedCols; cnt++)
		{
			DELETEPTRARR <ICHAR>(this->m_colDoubleDimension[cnt]);    
		}

		DELETEPTRARR <ICHAR*>(this->m_colDoubleDimension);    
	}

	DELETEPTRARR <ICHAR>(m_pDataBuffer);
	DELETEPTRARR <ICHAR>(m_pTempStr);
	DELETEPTRARR <ICHAR>(m_pEscapeChar);

		//end of fix for bug 1718,1719 and CR 176962
	m_DSQFields.clear();
    m_srcFields.clear();
    m_OutputLinkedFields.clear();
    m_InputLinkedFields.clear();
    m_InputTableList.clear();
    
    m_timeFormat.clearAndDestroy();

	if(runStatus==IFAILURE || m_bIsRunMethodCalled==IFALSE)
	{
        this->closePipe(); 
		m_iThreadMgr.join(m_hThread);
#ifdef UNIX
		this->removePipe();
#endif                    
        return IFAILURE;

	}

    if(m_pUtils->hasDTMRequestedStop() == ITRUE && this->m_nDTMFlag == 0)
    {
        if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
            m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_DTM_STOP_REQUEST));

        this->m_nDTMFlag = 1;
        this->destroyLocale();
        this->closePipe(); 
		m_iThreadMgr.join(m_hThread);
#ifdef UNIX
		this->removePipe();
#endif
                    
        return IFAILURE;
    }
	else if (m_pUtils->hasDTMRequestedStop() == ITRUE)
	{
		m_iThreadMgr.join(m_hThread);
#ifdef UNIX
		this->removePipe();
#endif

		return IFAILURE;
	}
    

	if(this->m_fHandle)
	{
		if(!feof(this->m_fHandle))
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_ERROR_READING_FILE)); 
			this->destroyLocale();

			if(this->m_nTraceLevel >= SLogger::ETrcLvlVerboseInit)
				m_pLogger->logMsg(ILog::EMsgDebug,SLogger::ETrcLvlVerboseData,m_pMsgCatalog->getMessage(MSG_RDR_NO_EOF_CANCELING_SELECT_QRY));
		}
	}

	m_iThreadMgr.join(m_hThread);
	
	this->closePipe();
#ifdef UNIX
	this->removePipe();
#endif

    this->destroyLocale();
    if(this->m_nTraceLevel >= SLogger::ETrcLvlVerboseInit)
        m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlVerboseInit,m_pMsgCatalog->getMessage(MSG_RDR_PARTITION_DEINIT));

	if(m_threadArgs->retInsertintoExtQuery==IFAILURE)
	{				
		return IFAILURE;
	}		
	this->m_connection->freeHandle();
    return ISUCCESS;
}


/****************************************************
* Name : CreateExternalTable
* Description : Generates the create External Table query to be fired based on 
*                   Session level parametrs set by user.
****************************************************/
ISTATUS PCNReader_Partition::CreateExternalTable()
{
    SQLRETURN   rc;
    IUString extTabQuery="";
    IUString dropExtTable="";
    IUString externalTable="";

    IUString pid,sErrVal;
    IUINT32 nVal=0;

	try
	{
		//Allocate the statement handle to execute the queries.
		rc = SQLAllocHandle(SQL_HANDLE_STMT, this->m_connection->getODbcHandle(), &m_hstmt);
		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			//To Do
			if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
				m_pLogger->logMsg(ILog::EMsgWarning,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_ALLOC_STMT_HNDL_FAIL));
			// Allocation of statement handle failed

			return IFAILURE;
		}

		m_threadArgs->hstmt = m_hstmt;

		if((m_pUtils -> getIntProperty(IUtilsServer::EErrorThreshold,nVal)) == IFAILURE) 
		{
			// Error Threshold Failed
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_CMN_RETRIV_ERR_THRESH_FAIL));

			return IFAILURE;
		}

		sErrVal.sprint(IUString("%d"),nVal);

		//Prepare the External table queries
		IUString           sqlState;
		IUString            msgeTxt;

		pid.sprint(IUString("%d"),getpid());

		externalTable = IUString("pcn");//changed from PCN to pcn
		externalTable.append(IUString("_ext_"));
		externalTable.append(pid);

		extTabQuery = IUString("CREATE EXTERNAL TABLE ");
		extTabQuery.append(IUString("'"));
		extTabQuery.append(m_fullPath);
		extTabQuery.append(IUString("'"));

		extTabQuery.append(IUString(" USING ( "));
		extTabQuery.append(IUString(" DELIMITER "));
		extTabQuery.append(this->m_sDelimiter);

		if(!m_socketBuffSize.isEmpty())
		{
			extTabQuery.append(IUString( " SOCKETBUFSIZE "));
			extTabQuery.append(IUString( m_socketBuffSize));
		}

		extTabQuery.append(IUString(" MAXERRORS "));
		extTabQuery.append(sErrVal);

		//Temporarily removed as faced some issue with unload for control chars
		extTabQuery.append(IUString(" FILLRECORD TRUE "));

		if(m_bTreatNoDataAsEmpty)
		{
			if(!m_nullValue.isEmpty())
			{
				extTabQuery.append(IUString(" NULLVALUE '"));
				extTabQuery.append(this->m_nullValue);
				extTabQuery.append(IUString("' "));
			}
			else
			{
				//the default behavior of Netezza
				m_nullValue = "NULL";
			}
		}
		else
		{
			extTabQuery.append(IUString(" NULLVALUE '"));
			extTabQuery.append(this->m_nullValue);
			extTabQuery.append(IUString("' "));
		}

		if(m_bIsEscapeChar)
		{
			extTabQuery.append(IUString(" ESCAPECHAR '"));
			extTabQuery.append(m_sEscapeChar);
			extTabQuery.append(IUString("' "));
		}
		extTabQuery.append(IUString(" REMOTESOURCE 'ODBC'  ENCODING 'internal' ) AS  "));
		extTabQuery.append(this->m_finalQuery);

		this->m_threadArgs->insertintoExtQuery = extTabQuery;
		this->m_finalQuery = extTabQuery;
		this->m_threadArgs->pUtils=m_pUtils;
		this->m_threadArgs->sLogger=m_pLogger;
		this->m_threadArgs->msgCatalog=m_pMsgCatalog;
		//BEGIN FIX FOR INFA392252 Partition-ID resolution
		this->m_threadArgs->threadName.sprint("%s_%d_*_%d",NETZZA_RDR_THREAD.getBuffer(),m_nDsqNum+1,m_uiPartitionID);
		//END FIX FOR INFA392252 Partition-ID resolution
		this->m_threadArgs->traceLevel=this->m_nTraceLevel;
		m_threadArgs->pipeFullPath=m_fullPath;


	}catch(...)
	{
		return IFAILURE;
	}
    return ISUCCESS;
}

/****************************************************
* Name : createPipe
* Description : Creates a pipe in Windows/UNIX 
*                   The Pipe is created in the /server/bin Directory in case of Unix
*                   and internal memory in case of Windows
****************************************************/
inline ISTATUS PCNReader_Partition::createPipe()
{
    IUString tempIUStr;
	try
	{
		//Fix for CR133218
		// To get the Pipe directory path from session attribute
		const ISessionExtension*  pISE  = m_pSession->getExtension(eWIDGETTYPE_DSQ, m_nDsqNum);
		IUString strPipePath=m_overridenPipePath;
		if(strPipePath.compare(PCN_Constants::PCN_EMPTY) == 0)
		{
			// Failed to get the session attribute
			if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
				m_pLogger->logMsg(ILog::EMsgWarning,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_CMN_GET_PIPE_DIR_PATH_FAIL));
		}


		IUString pipeID;
		pipeID.sprint(IUString("PCN_PipeRDR%d_%d_%d_"), getpid(), this->m_nDsqNum, this->m_nPartNum);
		//pipeID.append(this->m_srcTblName,this->m_srcTblName.getLength());
		
		// Fix for EBF10745 Adding timestamp as part of external table name
		IUString timestamp;
		timestamp.sprint(IUString("_%lld"),time(NULL));
		pipeID.append(IUString(timestamp));


		#ifndef WIN32
			//Fix for CR133218
		if(strPipePath.isEmpty())
		{
			//Create pipes in $PMTempDir dir by default
			strPipePath="$PMTempDir";
		}
		if(m_pUtils->expandString(strPipePath,m_fullPath,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDirFull)==IFAILURE)
		{
			IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
			tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),strPipePath.getBuffer());
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			return IFAILURE;
		}
		#endif

		#ifdef WIN32 //ined(DOS) || defined(OS2) || defined(NT) || defined(WIN32) || defined (MSDOS)
		{
			m_fullPath= IUString("\\\\.\\PIPE\\");
			//Fix for CR133218
			m_fullPath.append(strPipePath);
			//end fix
			m_fullPath.append(pipeID);

			LPWSTR lpszPipename = LPWSTR(this->m_fullPath.getBuffer());

			m_hPipe = CreateNamedPipe(
				lpszPipename,             // pipe name 
				PIPE_ACCESS_DUPLEX,       // read/write access 
				PIPE_TYPE_BYTE |       // message type pipe 
				PIPE_READMODE_BYTE |   // message-read mode 
				PIPE_WAIT,
				PIPE_UNLIMITED_INSTANCES, // max. instances  
				655350,                  // output buffer size 
				655350,
				5000,                     // client time-out 
				NULL);


			if (m_hPipe == INVALID_HANDLE_VALUE) 
			{
				if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
					m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_CREATE_PIPE_FAIL));
				return IFAILURE;
			}
			else
			{
				tempIUStr = m_pMsgCatalog->getMessage(MSG_RDR_CREATE_PIPE_SUCCESS);
				tempIUStr.append(m_fullPath);
				if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
					m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,tempIUStr);
			}
		}
		#else
		{
			IUString  pipeName = IUString("/Pipe");
			pipeName.append(pipeID);
			m_fullPath.append( pipeName);
	        
			ICHAR * fullPath = Utils::IUStringToChar(this->m_fullPath);
			IUINT32 ret = remove(fullPath);
			IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
			if(ret==0)
			{
				tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_REM_PIPE_SUCC),m_fullPath.getBuffer());
				if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
					m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
			else
			{				
				tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_PIPE_NOT_EXIST),m_fullPath.getBuffer());
				if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
					m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}	

			IINT32 fd = mknod(fullPath, 010777, 0);

			//Fix for CR133218
			if(fd ==-1)
			{
				if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
					m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_CREATE_PIPE_FAIL));				
				DELETEPTRARR <ICHAR>(fullPath);
				return IFAILURE;
			}
			//end fix

			if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
			{
				tempIUStr = m_pMsgCatalog->getMessage(MSG_RDR_CREATE_PIPE_SUCCESS);
				tempIUStr.append(m_fullPath);
				if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
					m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,tempIUStr);
			}

			DELETEPTRARR <ICHAR>(fullPath);
		}
		#endif
	}
	catch(...)
	{
		return IFAILURE;
	}
	return ISUCCESS;
}


/****************************************************
* Name : openWRPipe
* Description : Opens the pipe in Windows/UNIX 

****************************************************/
inline ISTATUS PCNReader_Partition::openWRPipe()
{
	ISTATUS iResult = ISUCCESS;
	IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
	try
	{		
		tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_PIPE_OPENING),m_fullPath.getBuffer());
		if(m_nTraceLevel >= SLogger::ETrcLvlNormal)
			m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
		#ifdef WIN32
			IUINT32    handle = _open_osfhandle((LONG)m_hPipe, _O_RDONLY );
			if( handle == -1)
				return IFAILURE;
			m_fHandle = fdopen(handle,"rb");
			if(m_fHandle==NULL)
			{
				if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
					m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_OPEN_PIPE_FAIL));
				return IFAILURE;
			}
			if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
				m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_OPEN_PIPE));

		#else
			ICHAR * fullPath = Utils::IUStringToChar(m_fullPath);
			IINT32 fd = open(fullPath, O_RDONLY);
			m_fHandle = (fd != -1 ? fdopen(fd, "rb") : NULL);
			DELETEPTRARR <ICHAR>(fullPath);
			if(m_fHandle==NULL)
			{ 
				if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
					m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_OPEN_PIPE_FAIL));
				return IFAILURE;
			}
			if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
				m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_OPEN_PIPE));
		#endif
			bIsPipeOpen=ITRUE;
	}catch(...)
	{
		iResult = IFAILURE; 
	}
    return iResult;
}

/****************************************************
* Name : openWRPipe
* Description : Closes the pipe in case of failure or in deinit 

****************************************************/
ISTATUS PCNReader_Partition::closePipe()
{
	ISTATUS iResult = ISUCCESS;
	if(m_fHandle != NULL)
	{
		// closing pipe handle
		if(fclose(m_fHandle)==0)
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
				m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_CMN_CLOSE_PIPE));				
		}
		else
		{
			//Error closing pipe
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_CMN_CLOSE_PIPE_FAIL));

			iResult=IFAILURE;
		}
	}
	return iResult;
}

/********************************************************************
*Function       : removePipe
*Description    : remove the pipe after child thread is finished
*Input          : None
********************************************************************/
inline ISTATUS PCNReader_Partition::removePipe()
{
	ICHAR *temp = Utils::IUStringToChar(m_fullPath) ;
	ISTATUS iResult=ISUCCESS;
	IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
	if(remove(temp) == 0)
	{
		tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_REM_PIPE_SUCC),m_fullPath.getBuffer());
		if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
			m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
	}
	else
	{
		iResult=IFAILURE;
		tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_REM_PIPE_FAIL),m_fullPath.getBuffer());
		if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
			m_pLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
	}	
	DELETEPTRARR <ICHAR>(temp);
	return iResult;
}


/****************************************************
* Name : getSessionAttributes
* Description : Gets the session Attribute values 

****************************************************/

ISTATUS PCNReader_Partition::getSessionAttributes() 
{
	ISTATUS iResult = ISUCCESS;
	try
	{
		std::map<IINT32, IUString> metadataMap = this->m_part_MetadataMap;
		if ( metadataMap.empty() )
			return IFAILURE;

		const ISessionExtension*  pISE  = m_pSession->getExtension(eWIDGETTYPE_DSQ, m_nDsqNum);
		IUString socketBuffSize=PCN_Constants::PCN_EMPTY;

		IUString errMsg;
		//BEGIN FIX FOR INFA392252 Partition-ID resolution
		if( m_pISessionImpl->getSessWidgetInstanceAttr(eWIDGETTYPE_DSQ,m_nDsqNum,m_uiPartitionID,IUString(SELECT_DISTINCT),m_pSessionAttributes->m_sSelectDistinct,errMsg) == IFAILURE)
		{
			m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,errMsg);
			return IFAILURE;
		}
		IUINT32 numSortPoprts;
		if( m_pISessionImpl->getSessWidgetInstanceAttrInt(eWIDGETTYPE_DSQ,m_nDsqNum,m_uiPartitionID,IUString(NUMBER_OF_SORTED_PORTS),numSortPoprts,errMsg) == IFAILURE)
		{
			m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,errMsg);
			return IFAILURE;
		}
		m_pSessionAttributes->m_nSortedPorts=numSortPoprts;
		if( m_pISessionImpl->getSessWidgetInstanceAttr(eWIDGETTYPE_DSQ,m_nDsqNum,m_uiPartitionID,IUString(USER_DEF_JOIN),m_pSessionAttributes->m_sJoinType,errMsg) == IFAILURE)
		{
			m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,errMsg);
			return IFAILURE;
		}
		if( m_pISessionImpl->getSessWidgetInstanceAttr(eWIDGETTYPE_DSQ,m_nDsqNum,m_uiPartitionID,IUString(SOURCE_FILTER),m_pSessionAttributes->m_sFilterString,errMsg) == IFAILURE)
		{
			m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,errMsg);
			return IFAILURE;
		}
		if( m_pISessionImpl->getSessWidgetInstanceAttr(eWIDGETTYPE_DSQ,m_nDsqNum,m_uiPartitionID,IUString(SQL_QUERY),m_pSessionAttributes->m_sCustomQuery,errMsg) == IFAILURE)
		{
			m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,errMsg);
			return IFAILURE;
		}

		IUString tempErrMsg;
		std::map<IINT32,IUString>::iterator it;
		if ( m_l_srcList.length() > 1 )
		{
			for ( IUINT32 itr = 0; itr < m_l_srcList.length(); itr++ )
			{
				m_l_overRiddenSchema.insert(PCN_Constants::PCN_EMPTY);
				m_l_overRiddenTable.insert(PCN_Constants::PCN_EMPTY);
			}
		}
		else
		{
			it = metadataMap.find(RDR_PROP_OWNER_NAME);
			if ( it != metadataMap.end() )
				m_l_overRiddenSchema.insert(it->second);
			else
				m_l_overRiddenSchema.insert(PCN_Constants::PCN_EMPTY);
			it = metadataMap.find(RDR_PROP_SOURCE_TABLE_NAME);
			if ( it != metadataMap.end() )
				m_l_overRiddenTable.insert(it->second);
			else
				m_l_overRiddenTable.insert(PCN_Constants::PCN_EMPTY);
		}

		it = metadataMap.find(RDR_PROP_SOCKET_BUFF_SIZE);
		if( it == metadataMap.end() || 0 == it->second.compare(PCN_Constants::PCN_EMPTY) )
			socketBuffSize = IUString("8388608");
		else
			socketBuffSize = it->second;

		if(m_pUtils->expandString(socketBuffSize,m_socketBuffSize,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
		{
			IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
			tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),socketBuffSize.getBuffer());
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			return IFAILURE;
		}

		it = metadataMap.find(RDR_PROP_DELIMITER);
		IUString delimiter = PCN_Constants::PCN_PIPE;
		if(it == metadataMap.end() || 0 == it->second.compare(PCN_Constants::PCN_EMPTY))
			delimiter = PCN_Constants::PCN_PIPE;
		else
			delimiter = it->second;

		if(m_pUtils->expandString(delimiter,m_sDelimiter,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
		{
			IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
			tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),delimiter.getBuffer());
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			return IFAILURE;
		}

		m_b81103Delimiter=PM_FALSE;
		GetConfigBoolValue(DELIMITER_FLAG,m_b81103Delimiter);
		ISTATUS retDilm;
		IUINT32 lenDelim;

		/*
		Netezza odbc driver (after 4.x) expects client to provide delimiter as byte value rather a symbol for extended 
		ascii characters. While implementing support for passing byte value of delimiter, Latin9 code page was used to 
		convert a symbol to byte value. The reason to opt this code page was that the same code page was used while 
		passing the data (char/varchar columns) as byte stream to Netezza server. 
		In the 81103 we never had any support for Unicode mode so the delimiter was passed to Netezza server as 
		plain byte value. Now the 81103 sessions which were using delimiters as those characters which are extended 
		ascii characters as per the Latin1 code page but not in the extended ascii character set of Latin9, 
		faced the issue after upgrade. The obvious workaround for these cases was to provide delimiter as a 
		byte value only rather a symbol.
		To provide the seamless upgrade from 81103 we have implemented an undocumented flag (delimiter81103Functionality) 
		which if set at integration level, PWX for Netezza will use Latin1 code page to covert the delimiter symbol 
		to a byte value.		
		*/
		if(m_b81103Delimiter)
			retDilm = Utils::_ConvertToMultiByte(m_pLocale_Latin1,m_sDelimiter.getBuffer(),&m_cdelimiter,&lenDelim,ITRUE);
		else
			retDilm = Utils::_ConvertToMultiByte(m_pLocale_Latin9,m_sDelimiter.getBuffer(),&m_cdelimiter,&lenDelim,ITRUE);

		if(m_sDelimiter.getLength()==1)
		{
			m_iDelimiter=(unsigned char)m_cdelimiter[0];			
		}
		else if(m_sDelimiter.getLength()==3 && isdigit(m_cdelimiter[0]) && isdigit(m_cdelimiter[1]) && isdigit(m_cdelimiter[2]))
		{
			m_iDelimiter=atoi(m_cdelimiter);
		}
		else
		{
			IUString tempVariableChar;
			tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_INVALID_DELIM),m_sDelimiter.getBuffer());				
			if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,tempVariableChar);

			throw IFAILURE;
		}

		m_sDelimiter.sprint("%d",m_iDelimiter);
		DELETEPTRARR <ICHAR>(m_cdelimiter);	
		/*
		Since we need only one byte value for the delimiter, we are allocating only 2 byte space, second one for null valuee.
		*/
		if((m_cdelimiter=new ICHAR[2])==NULL)
		{
			throw IFAILURE;
		}
		memset(m_cdelimiter,0,2);
		sprintf(m_cdelimiter,"%c",m_iDelimiter);		


		//Currently removed  Control char as faced some issues for bulk unloads

		// To get the nullvalue from session attribute

		IUString nullValue;
		it = metadataMap.find(RDR_PROP_NULL_VAL);
		if(it == metadataMap.end() || 0 == it->second.compare(PCN_Constants::PCN_EMPTY))
			nullValue = PCN_NULL;
		else
			nullValue = it->second;

		if(m_pUtils->expandString(nullValue,m_nullValue,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
		{
			IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
			tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),nullValue.getBuffer());
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			return IFAILURE;
		}

		m_bTreatNoDataAsEmpty=PM_FALSE;
		GetConfigBoolValue(TREAT_NODATA_EMPTY,m_bTreatNoDataAsEmpty);

		m_bSyncNetezzaNullWithPCNull = PM_FALSE;
		GetConfigBoolValue(SYNC_NETEZZA_NULL_WITH_PCNULL,m_bSyncNetezzaNullWithPCNull);

		char* envPathCCIHOME = getenv("CCI_HOME");
		char* cfgFilePath;
		//platform specific handling for opening pmrdtm.cfg file.
		#if defined WIN32
		cfgFilePath = "\\..\\bin\\rdtm\\pmrdtm.cfg";
		#else
		cfgFilePath = "/../bin/rdtm/pmrdtm.cfg";
		#endif
		//final reuslt should be finacfgFilePath = envPathCCIHOME + cfgFilePath;
		char* finacfgFilePath = strcat (envPathCCIHOME,cfgFilePath);
		
		std::ifstream infile(finacfgFilePath);
		string linebuffer;
		string str_sync = "syncNetezzaNullWithPCNull";
		string str_treat = "treatNoDataAsEmptyString";

		while (infile && getline(infile, linebuffer))
		{
			if (linebuffer.length() == 0)continue;

			std::size_t foundSync = linebuffer.find(str_sync); 
			std::size_t foundTreat = linebuffer.find(str_treat); 
			
			if (foundSync!=std::string::npos)
			{	
				std::size_t foundValue = linebuffer.find("true"); 
				if (foundValue!=std::string::npos)
					m_bSyncNetezzaNullWithPCNull = 1;
			}
			if (foundTreat!=std::string::npos)
			{	
				std::size_t foundValue = linebuffer.find("true"); 
				if (foundValue!=std::string::npos)
					m_bTreatNoDataAsEmpty = 1;
			}

		}//end while
		infile.close();
		
		// To get the escapeChar from session attribute
		IUString escapeChar = PCN_Constants::PCN_EMPTY;
		it = metadataMap.find(RDR_PROP_ESCAPE_CHAR);
		if(it == metadataMap.end() || 0 == it->second.compare(PCN_Constants::PCN_EMPTY))
			escapeChar = PCN_Constants::PCN_EMPTY;
		else
			escapeChar = it->second;


		if(m_pUtils->expandString(escapeChar,m_sEscapeChar,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
		{
			IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
			tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),escapeChar.getBuffer());
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			return IFAILURE;
		}

		//Fix for bug 1822
		if(m_sEscapeChar==IUString(m_cdelimiter))
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
			{
				IUString tempVariableChar;
				tempVariableChar.sprint(m_pMsgCatalog->getMessage(32248),IUString("\\").getBuffer(),IUString("|").getBuffer());
				m_pLogger->logMsg(ILog::EMsgWarning,SLogger::ETrcLvlTerse,tempVariableChar);
			}
			
			m_sEscapeChar = IUString("\\");
			DELETEPTRARR <ICHAR>(m_cdelimiter);	
			m_cdelimiter = Utils::IUStringToChar(PCN_Constants::PCN_PIPE);
			m_iDelimiter=(unsigned char)m_cdelimiter[0];
			m_sDelimiter.sprint("%d",m_iDelimiter);
		}

		if(!m_sEscapeChar.isEmpty())
		{
			m_bIsEscapeChar=ITRUE;
			this->m_pEscapeChar=Utils::IUStringToChar(this->m_sEscapeChar);
		}

		
		IUString sqlqueryOverride = PCN_Constants::PCN_EMPTY;
		it = metadataMap.find(RDR_PROP_SQL_QUERY_OVERRIDE);
		if(it == metadataMap.end() || 0 == it->second.compare(PCN_Constants::PCN_EMPTY))
			sqlqueryOverride = PCN_Constants::PCN_EMPTY;
		else
			sqlqueryOverride = it->second;

		if(m_pUtils->expandString(sqlqueryOverride,m_pSessionAttributes->m_sCustomQuery,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
		{
			IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
			tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),sqlqueryOverride.getBuffer());
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			return IFAILURE;
		}

		IUString pipePath = PCN_Constants::PCN_EMPTY;
		it = metadataMap.find(RDR_PROP_PIPE_DIR_PATH);
		if(it == metadataMap.end() || 0 == it->second.compare(PCN_Constants::PCN_EMPTY))
#ifndef WIN32
			pipePath = IUString("$PMTempDir");
#else
			pipePath = PCN_Constants::PCN_EMPTY;
#endif
		else
			pipePath = it->second;

		if(m_pUtils->expandString(pipePath,m_overridenPipePath,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
		{
			IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
			tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),pipePath.getBuffer());
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			return IFAILURE;
		}
		
	printSessionAttributes();		

	}catch(...)
	{
		iResult = IFAILURE;
	}
    return iResult ;
}

void PCNReader_Partition::printSessionAttributes()
{
	IUString printMsg;
	IUINT32 pullBytes;
	if ( !m_overridenPipePath.isEmpty() )
	{
		printMsg=PCN_Constants::PCN_EMPTY;
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("Pipe Directory Path").getBuffer(), m_overridenPipePath.getBuffer());
		m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);
	}
	if ( !m_sDelimiter.isEmpty() )
	{
		printMsg=PCN_Constants::PCN_EMPTY;
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("Delimiter").getBuffer(), m_sDelimiter.getBuffer());
		m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);
	}
	if ( !m_nullValue.isEmpty() )
	{
		printMsg=PCN_Constants::PCN_EMPTY;
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("Null Value").getBuffer(), m_nullValue.getBuffer());
		m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);
	}
	if ( !m_sEscapeChar.isEmpty() )
	{
		printMsg=PCN_Constants::PCN_EMPTY;
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("Escape Character").getBuffer(), m_sEscapeChar.getBuffer());
		m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);
	}
	if ( !m_socketBuffSize.isEmpty() )
	{
		printMsg=PCN_Constants::PCN_EMPTY;
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("Socket Buffer Size").getBuffer(), m_socketBuffSize.getBuffer());
		m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);

	}
	if ( !m_pSessionAttributes->m_sCustomQuery.isEmpty() )
	{
		printMsg=PCN_Constants::PCN_EMPTY;
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("SQL Query Override").getBuffer(), m_pSessionAttributes->m_sCustomQuery.getBuffer());
		m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);

	}
	if ( !m_l_overRiddenSchema.at(0).isEmpty() )
	{
		printMsg=PCN_Constants::PCN_EMPTY;
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("Owner Name").getBuffer(), m_l_overRiddenSchema.at(0).getBuffer());
		m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);
	}
	if ( !m_l_overRiddenTable.at(0).isEmpty() )
	{
		printMsg=PCN_Constants::PCN_EMPTY;
		printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("Source Table Name").getBuffer(), m_l_overRiddenTable.at(0).getBuffer());
		m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);
	}

}


/*********************************************************************
* Name                  : GenerateFinalQuery
* Description                   : Generates the Final Query which will be fired from reader
using the metadata extension values.
* Returns               : none
*********************************************************************/

//Note the getRefPKInfo part is pending because of no EBF
ISTATUS PCNReader_Partition::GenerateFinalQuery()
{
	ISTATUS iResult = ISUCCESS;
	IUString finalQuery=PCN_Constants::PCN_EMPTY;
	try
	{
		IUString sTableName;
		IUString sFieldName;
		//BEGIN New Feature for Table name and prefix override Ayan
		IUString sSchemaName = PCN_Constants::PCN_EMPTY;;
		//END New Feature for Table name and prefix override Ayan
		m_nNumOfConnectedCols=m_OutputLinkedFields.length();

		if(!this->m_pSessionAttributes->m_sCustomQuery.isEmpty())
			finalQuery=this->m_pSessionAttributes->m_sCustomQuery;
		else if(!this->m_pMetaExtension->m_sCustomQuery.isEmpty())
			finalQuery=this->m_pMetaExtension->m_sCustomQuery;
		else
		{
			finalQuery = IUString("Select ");

			if(m_pSessionAttributes->m_sSelectDistinct.isEmpty())
			{
				if(m_pMetaExtension->m_sSelectDistinct == PCN_YES)
					finalQuery.append(IUString("DISTINCT "));
			}
			else if(m_pSessionAttributes->m_sSelectDistinct == PCN_YES)
				finalQuery.append(IUString("DISTINCT "));

			for(IUINT16 outFieldCnt = 0; outFieldCnt < m_nNumOfConnectedCols; outFieldCnt++)
			{
				//Get Table Name
				const IField* pField =  m_OutputLinkedFields.at(outFieldCnt);
				sTableName = pField->getTable()->getName();
				sFieldName = pField->getName(); 
				//BEGIN New Feature for Table name and prefix override Ayan
				if ( ITRUE == SearchForOverriddenSchemaAndTableName(sSchemaName,sTableName) )
				{
					finalQuery.append(IUString("\""));            
					finalQuery.append(sSchemaName);
					finalQuery.append(IUString("\"."));
				}
				//END New Feature for Table name and prefix override Ayan
	        
				finalQuery.append(IUString("\""));            
				finalQuery.append(sTableName);
				finalQuery.append(IUString("\""));            
				finalQuery.append(IUString("."));
				finalQuery.append(IUString("\""));            
				finalQuery.append(sFieldName);
				finalQuery.append(IUString("\""));

				if(outFieldCnt < m_nNumOfConnectedCols - 1)
					finalQuery.append(IUString(","));
			}

			//Attaching Table List
			IUString firstTableName;
			finalQuery.append(IUString(" from "));
			for(IUINT16 inTableCnt = 0; inTableCnt < this->m_InputTableList.length() ; inTableCnt++)
			{
				const ITable* pTable = this->m_InputTableList.at(inTableCnt);
				

				//BEGIN New Feature for Table name and prefix override Ayan
				IUString TableName = PCN_Constants::PCN_EMPTY, SchemaName = PCN_Constants::PCN_EMPTY;;
				TableName = pTable->getName();
				if ( ITRUE == SearchForOverriddenSchemaAndTableName(SchemaName,TableName) )
				{           
					finalQuery.append(IUString("\"")); 
					finalQuery.append(SchemaName);
					finalQuery.append(IUString("\"."));
				}

				finalQuery.append(IUString("\"")); 
				if(inTableCnt==0)
					firstTableName=TableName;

				finalQuery.append(TableName);
				finalQuery.append(IUString("\""));     
				//END New Feature for Table name and prefix override Ayan

				if(inTableCnt < this->m_InputTableList.length()-1)
					finalQuery.append(IUString(" ,"));
			}

			/*
			A dataslice identifies the portion of the database stored on each SPU. At installation, the system is 
			divided into logical number of data slices. When the system creates a record, it assigns it a logical 
			data slice (and hence a physical SPU) based on its distribution key. Although the system dynamically 
			generates datasliceid values, it does not store them with each individual record. But we can make use 
			of datasliceid to identify which data slice (or SPU) the records are coming from.
            
			Hence in any PC session, we can use the datasliceid to group the data from logical data slices into 
			as many groups as there are partitions in the session. 
			*/

			/*
			In case user is using SQL query(In DSQ property page or Session extension), we will not append dataslice id logic in the query.
			Data slice id logic will not be appended in case user
			1. has provided input in SQL query attribute for any partition. 
			2. different inputs in the session extension filter query acorss partitions.
			3. different inputs in the session extension user define join query acorss partitions.

			*/

			IBOOLEAN b_addDataSliceID;

			if(m_numPartitions >1)
			{
				if(IFALSE==this->getParentDSQDriver()->getAddDataSliceId())
					b_addDataSliceID=IFALSE;
				else
					b_addDataSliceID=ITRUE;
			}
			else
				b_addDataSliceID=IFALSE;

			if(b_addDataSliceID)
			{
				finalQuery.append(IUString(" where "));
				IUString tempDataSlice=PCN_Constants::PCN_EMPTY;
				tempDataSlice.sprint("mod(\"%s\".datasliceid, %d)=%d",firstTableName.getBuffer(),m_numPartitions,m_nPartNum);
				finalQuery.append(IUString(tempDataSlice));
			}

			if(m_pSessionAttributes->m_sJoinType.isEmpty())
			{

				if(!m_pMetaExtension->m_sJoinType.isEmpty())
				{
					if(b_addDataSliceID)
						finalQuery.append(IUString(" and "));
					else
						finalQuery.append(IUString(" where "));
					finalQuery.append(m_pMetaExtension->m_sJoinType.getBuffer());
				}
			}
			else
			{
				if(b_addDataSliceID)
					finalQuery.append(IUString(" and "));
				else
					finalQuery.append(IUString(" where "));
				finalQuery.append(m_pSessionAttributes->m_sJoinType);
			}

			if(m_pSessionAttributes->m_sFilterString.isEmpty())
			{
				if(!m_pMetaExtension->m_sFilterString.isEmpty())
				{
					if((m_pMetaExtension->m_sJoinType.isEmpty()) && (m_pSessionAttributes->m_sJoinType.isEmpty()))
					{
						if(b_addDataSliceID)
							finalQuery.append(IUString(" and "));
						else
							finalQuery.append(IUString(" where "));
					}
					else
						finalQuery.append(IUString(" and "));

					finalQuery.append(m_pMetaExtension->m_sFilterString.getBuffer());
				}
			} 
			else 
			{
				if((m_pMetaExtension->m_sJoinType.isEmpty())  && (m_pSessionAttributes->m_sJoinType.isEmpty()))
				{
					if(b_addDataSliceID)
						finalQuery.append(IUString(" and "));
					else
						finalQuery.append(IUString(" where "));
				}
				else
					finalQuery.append(IUString(" and "));

				finalQuery.append(m_pSessionAttributes->m_sFilterString);
			}

			//Attaching primary key foreign key relationship
			IBOOLEAN bEnteredFlag = IFALSE;

			for(IUINT16 iPos=0;iPos<m_InputTableList.length();iPos++)
			{
				IVector<IField *> vFldList;
				((ISource*)m_InputTableList.at(iPos))->getFKFieldList(vFldList);
				IBOOLEAN bHasNoFKey=ITRUE;

				IUString sTFieldName,sTTableName,sTDbdName;
				for(IUINT16 iItrFld=0;iItrFld<vFldList.length();iItrFld++)
				{
					IUString sTFieldName,sTTableName,sTDbdName;

					((ISourceField*) vFldList.at(iItrFld))->getRefPKInfo(sTFieldName,sTTableName,sTDbdName);
					//Search the table name in Input Table List
					bHasNoFKey=ITRUE;

					sTTableName = vFldList.at(iItrFld)->getTable()->getName();
					for(IUINT16 iTblItr=0;iTblItr<m_InputTableList.length();iTblItr++)
					{
						if(m_InputTableList.at(iTblItr)->getName().compare(sTTableName)==0)
						{
							bHasNoFKey=IFALSE;
						}
					}

					if(bHasNoFKey == IFALSE)
					{
						if((m_pMetaExtension->m_sJoinType.isEmpty()) && (m_pSessionAttributes->m_sJoinType.isEmpty())  &&( m_pMetaExtension->m_sFilterString.isEmpty()) && (m_pSessionAttributes->m_sFilterString.isEmpty()) &&( bEnteredFlag==IFALSE))
						{
							if(b_addDataSliceID)
								finalQuery.append(IUString(" and "));
							else
								finalQuery.append(IUString(" where "));
						}
						else
							finalQuery.append(IUString(" and "));

						//BEGIN New Feature for Table name and prefix override Ayan
						IUString TempTableName = ((ISource*)m_InputTableList.at(iPos))->getName();
						IUString TempSchemaName = PCN_Constants::PCN_EMPTY;;
						if ( ITRUE == SearchForOverriddenSchemaAndTableName(TempSchemaName,TempTableName) )
						{
							finalQuery.append(IUString("\""));            
							finalQuery.append(TempSchemaName);
							finalQuery.append(IUString("\"."));
						}
						finalQuery.append(IUString("\""));  
						finalQuery.append(TempTableName);
						finalQuery.append(IUString("\".\""));  						
						finalQuery.append(vFldList.at(iItrFld)->getName());
						finalQuery.append(IUString("\" = "));  						

						if ( ITRUE == SearchForOverriddenSchemaAndTableName(TempSchemaName,sTTableName) )
						{
							finalQuery.append(IUString("\""));            
							finalQuery.append(TempSchemaName);
							finalQuery.append(IUString("\"."));
						}
						finalQuery.append(IUString("\""));    
						finalQuery.append(sTTableName);
						finalQuery.append(IUString("\".\""));						
						finalQuery.append(vFldList.at(iItrFld)->getName());
						finalQuery.append(IUString("\""));  
						//END New Feature for Table name and prefix override Ayan

						bEnteredFlag=ITRUE;                
					}
				}
			}

			if(m_pMetaExtension->m_nSortedPorts > m_nNumOfConnectedCols  && m_pSessionAttributes->m_nSortedPorts < 0 )
			{
				m_pLogger->logMsg(ILog::EMsgWarning,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_NO_OF_STORED_PORT_WARN));
				return IFAILURE;
			}

			if(m_pSessionAttributes->m_nSortedPorts < 0)
			{
				if(m_pMetaExtension->m_nSortedPorts > 0)
				{
					finalQuery.append(IUString(" ORDER BY "));

					for(IUINT16 noPortsCnt = 0; noPortsCnt < m_pMetaExtension->m_nSortedPorts ; noPortsCnt++)
					{
						const IField* pField =  m_OutputLinkedFields.at(noPortsCnt);
						sFieldName = pField->getName();
						finalQuery.append(IUString("\""));  
						finalQuery.append(sFieldName);
						finalQuery.append(IUString("\""));  
						if(noPortsCnt < m_pMetaExtension->m_nSortedPorts - 1)
							finalQuery.append(IUString(" ,"));
					}

				}
			}
			else if(( m_pSessionAttributes->m_nSortedPorts <= m_nNumOfConnectedCols ) && ( m_pSessionAttributes->m_nSortedPorts > 0) )
			{
				finalQuery.append(IUString(" ORDER BY "));

				for(IUINT16 noPortsCnt = 0; noPortsCnt < m_pSessionAttributes->m_nSortedPorts ; noPortsCnt++)
				{
					const IField* pField =  m_OutputLinkedFields.at(noPortsCnt);
					sFieldName = pField->getName();
					finalQuery.append(IUString("\""));  
					finalQuery.append(sFieldName);
					finalQuery.append(IUString("\""));  
					if(noPortsCnt < m_pSessionAttributes->m_nSortedPorts - 1)
						finalQuery.append(IUString(" ,"));
				}
			}
			else if ( m_pSessionAttributes->m_nSortedPorts > m_nNumOfConnectedCols)
			{
				m_pLogger->logMsg(ILog::EMsgWarning,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_NO_OF_STORED_PORT_WARN));
				return IFAILURE;
			}
		}
		IUString finalQuery_afterSessionExpand;
		if(m_pUtils->expandString(finalQuery,finalQuery_afterSessionExpand,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
		{
			m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_SQL_QRY_EXPAND_FAIL));
			return IFAILURE;
		}
		if(m_pUtils->expandString(finalQuery_afterSessionExpand,this->m_finalQuery,IUtilsServer::EVarParamMapping,IUtilsServer::EVarExpandDefault)==IFAILURE)
		{
			m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_SQL_QRY_EXPAND_FAIL));
			return IFAILURE;
		}
	}catch(...)
		{
			iResult = IFAILURE;
		}
    return iResult;
}

//BEGIN New Feature for Table name and prefix override Ayan
/*********************************************************************
* Name                  : SearchForOverriddenSchemaAndTableName
* Description           : Searches the Original Source List of table names to find the number and check corresponding list if values are overriden or not
* Returns               : ISTATUS(Returns ITRUE only if Schema is overriden)
*********************************************************************/
IBOOLEAN PCNReader_Partition::SearchForOverriddenSchemaAndTableName(IUString& strSchemaName, IUString& strTableName)
{
	strSchemaName = PCN_Constants::PCN_EMPTY;
	for ( IUINT32 itr = 0; itr < m_l_srcList.length(); itr++ )
	{
		if ( 0 == m_l_srcList.at(itr)->getTable()->getName().compare(strTableName) )
		{
			if ( !m_l_overRiddenTable.at(itr).isEmpty() )
			{
				strTableName = m_l_overRiddenTable.at(itr);
			}
			if ( !m_l_overRiddenSchema.at(itr).isEmpty() )
			{
				strSchemaName = m_l_overRiddenSchema.at(itr);
				return ITRUE;
			}
			else
			{
				return IFALSE;
			}
		}
	}
}
//END New Feature for Table name and prefix override Ayan

/*********************************************************************
* Name                  : UpdateFieldDetails
* Description                   : Gets the input,output links , source table list 
and fills them in  the respective data member variables
* Returns               : ISTATUS
*********************************************************************/
ISTATUS PCNReader_Partition::UpdateFieldDetail()   
{
    //Get DSQ Field List Detail
    ECDataType datatype;
    IUINT32    prec;
    IINT32     scale;
    IUString strAttrValue;

    m_pDSQ->getTable()->getFieldList(m_DSQFields);

    //Get source fields that are linked and update input/output table list
    IVector<IUINT16> vPos;

	m_nNumOfCols=m_DSQFields.length();
    m_colmnAttr  = new ColumnAttr[ m_nNumOfCols];

    for(IUINT16 itrDsqFld = 0; itrDsqFld <m_nNumOfCols ; itrDsqFld++)
    {
        const ILink* pLink = m_pDSQ->getInputLink(m_DSQFields.at(itrDsqFld));

        if (    m_DSQFields.at(itrDsqFld)->getCDataType(datatype,prec,scale) != ISUCCESS)
        {
            if(this->m_nTraceLevel >= SLogger::ETrcLvlVerboseInit)
                m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlVerboseInit,m_pMsgCatalog->getMessage(MSG_RDR_INVALID_DATA_TYPE));
            return IFAILURE;
        }
		
        this->m_colmnAttr[itrDsqFld].m_dataType = datatype;
        this->m_colmnAttr[itrDsqFld].m_nPrec = prec ;
        this->m_colmnAttr[itrDsqFld].m_nScale = scale;



        if(pLink != NULL)
		{
			m_InputLinkedFields.insert(pLink->getFromField());
			m_srcFields.insert(pLink->getFromField());

			const ITable* pTable = pLink->getFromField()->getTable();
			if( m_InputTableList.contains(pTable) ==  IFALSE)
			{
				m_InputTableList.insert(pTable);
			}

			//Check for Output Link
			IVector<const ILink*> vLink;

			m_pDSQ->getOutputLinkList(m_DSQFields.at(itrDsqFld),vLink);
			if(vLink.length () > 0)
			{
				this->m_colmnAttr[itrDsqFld].isLinked=ITRUE;
				m_OutputLinkedFields.insert(pLink->getFromField());
				//Fix for Bug 1719, 1718
				IUString sdatatype;
				pLink->getFromField()->getDataType(sdatatype,prec,scale);
				if(sdatatype == PCN_TIMESTAMP)
				{
					strAttrValue =(IUString)DATE_FMT_YMD_HMS;
					m_timeFormat.insert(Utils::IUStringToChar(strAttrValue));
					this->m_colmnAttr[itrDsqFld].m_nPrec = 26 ;
					this->m_colmnAttr[itrDsqFld].m_nScale = scale;
				}
				else if (sdatatype == PCN_TIME)
				{
					strAttrValue = (IUString)DATE_FMT_HMS;
					m_timeFormat.insert(Utils::IUStringToChar(strAttrValue));
					this->m_colmnAttr[itrDsqFld].m_nPrec = 15 ;
					this->m_colmnAttr[itrDsqFld].m_nScale = scale;
				}
				else if (sdatatype == PCN_DATE)
				{
					strAttrValue =(IUString)DATE_FMT_YMD;
					m_timeFormat.insert(Utils::IUStringToChar(strAttrValue));
					this->m_colmnAttr[itrDsqFld].m_nPrec = 10 ;
					this->m_colmnAttr[itrDsqFld].m_nScale = scale;
				}
				else if (sdatatype == PCN_TIMETZ)
				{
					m_timeFormat.insert(NULL);
				}

				//in case CDataType is unichar(IS is in unicode)...For nchar and nvarchar we are setting boolean to the true.
				if(datatype==eCTYPE_UNICHAR || datatype==eCTYPE_CHAR)
				{
					if(sdatatype==PCN_VARCHAR || 
						sdatatype==PCN_CHAR)
					{
						this->m_colmnAttr[itrDsqFld].isColNchar=IFALSE;
					}
					else
					{
						this->m_colmnAttr[itrDsqFld].isColNchar=ITRUE;
					}
				}

				//end of fix for Bug 1718, 1719
            }
			else
			{
				this->m_colmnAttr[itrDsqFld].isLinked=IFALSE;
			}
        }
        else
		{
            m_srcFields.insert(NULL);
			this->m_colmnAttr[itrDsqFld].isLinked=IFALSE;
		}

    }

	return ISUCCESS;
}

/*---------------------------------------------------------------------------------------------------------------
Unicode Conversion
----------------------------------------------------------------------------------------------------------------*/

/********************************************************************
* Name                          : _ConvertToUnicode
* Description           : Converts the MBCS data to Unicode using the Locale object created
szCharSet                : MBCS string to be converted 
pwzCharSet               : IUNICHAR string passed by reference where unicode data is stored
pulBytes                         : No of characters scanned 
* Returns                       : failure or success 
*********************************************************************/


ISTATUS PCNReader_Partition::_ConvertToUnicode(ILocale *m_pLocale, ICHAR* szCharSet,
                                               IUNICHAR** pwzCharSet, IUINT32 *pulBytes, BOOL bAllocMem)
{
    ISTATUS iResult     = ISUCCESS;
    IUINT32 ulCharSetW  = 0;
    IUINT32 nRetVal     = 0;
    size_t byteLen;

    *pulBytes = 0;
	try
	{
		//Get max size of wide string
		ulCharSetW = (IUINT32) strlen (szCharSet);
		byteLen = (sizeof (IUNICHAR) * (ulCharSetW + 1));
		//max bytes
		*pulBytes = (IUINT32) byteLen;
		if (ITRUE == bAllocMem)
		{
			*pwzCharSet = NULL;

			if ((*pwzCharSet = (IUNICHAR *)malloc(*pulBytes)) == NULL) 
			{
				iResult = IFAILURE;
				*pulBytes = 0;
				return iResult;
			}               

		}
		memset (*pwzCharSet, 0, byteLen);
		// M2U
		//returns number of IUNICHARs written to pwzCharSet, excluding the NULL terminator
		nRetVal = m_pLocale->M2U (szCharSet, *pwzCharSet);
	}catch(...)
	{
		iResult = IFAILURE;
	}
    return iResult;
}

/********************************************************************
* Name                          : createLocale
* Description           : Creates locale based on codepage of connection object
* Returns                       : None 
*********************************************************************/


ISTATUS PCNReader_Partition::createLocale()
{
    ISTATUS iResult     = ISUCCESS;

	try
	{
		IUtilsCommon *pUtils = IUtilsCommon::getInstance ();
		m_pLocale_Latin9 = pUtils->createLocale(LATIN9_CODEPAGE_ID);
		m_pLocale_UTF8= pUtils->createLocale(UTF8_CODEPAGE_ID);
		m_pLocale_Latin1= pUtils->createLocale(LATIN1_CODEPAGE_ID);

		if (m_pLocale_Latin9  == NULL || m_pLocale_UTF8 == NULL || m_pLocale_Latin1  == NULL)
			return IFAILURE;
	}catch(...)
	{
		iResult = IFAILURE;
	}
    return ISUCCESS;
}

/********************************************************************
* Name                          : destroyLocale
* Description           : Destroys the locale object created   at time of partition init
* Returns                       : None 
*********************************************************************/

ISTATUS PCNReader_Partition::destroyLocale()
{
    ISTATUS iResult     = ISUCCESS;
	try
	{
		IUtilsCommon *pUtils = IUtilsCommon::getInstance ();
		pUtils->destroyLocale(this->m_pLocale_Latin9);
		pUtils->destroyLocale(this->m_pLocale_UTF8);
		pUtils->destroyLocale(this->m_pLocale_Latin1);
	}catch(...)
	{
		iResult = IFAILURE;
	}
    return ISUCCESS;
}

inline IINT64 PCNReader_Partition::convertStringToInt64(ICHAR *str)
{
	char c;
	IBOOLEAN bNegative = IFALSE;
	IINT64 total = 0;  // current total 
	while (isspace(*str))
        ++str;

	c=*str++;
	 if (c == '-') 
    {
		bNegative = ITRUE;
        c = *str++;      // skip sign 
    }
    else if (c == '+')
        c = *str++;      // skip sign

	 while (c >= '0' && c <= '9') 
	 {
		 int value = c - '0';
		 if (bNegative)
		 {		
			 total = 10 * total - value;  
		 }
		 else
		 {		
			 total = 10 * total + value;   
		 }
		 c = *str++;    /* get next char */
	 }

	 return total;
}

inline PCNReader_DSQ *PCNReader_Partition::getParentDSQDriver()
{
	return this->m_parentDSQDriver;
}

IBOOLEAN PCNReader_Partition::isPipeOpened()
{
	return bIsPipeOpen;
}

void PCNReader_Partition::calMaxRowLength()
{
	m_rowMaxLength=0;
	IUINT32 oldLength=0;
	ECDataType	  dataType;
	IUINT32		prec;
	IBOOLEAN isColNchar;

	IINT32 nMaxMBCSBytesPerChar=m_pLocale_UTF8->getMaxMBCSBytesPerChar();

	this->m_colDoubleDimension = new ICHAR *[m_nNumOfConnectedCols];
	this->m_colHadEscapeChar = new IBOOLEAN[m_nNumOfConnectedCols];

	IUINT32 connColNum=0;
	for(IUINT32 colNum=0;colNum<m_nNumOfCols;colNum++)
	{

		if(IFALSE==this->m_colmnAttr[colNum].isLinked)
		{
			continue;
		}
		dataType=this->m_colmnAttr[colNum].m_dataType;
		prec=this->m_colmnAttr[colNum].m_nPrec;
		isColNchar=this->m_colmnAttr[colNum].isColNchar;

		prec=prec+1; //1 for null termination, +2 for extra buffer(extra secure that we dont cause memory corruption.
		//We need to deal each type of column seprately, and need to allocate buffer accordingly.
		if(dataType==eCTYPE_SHORT || dataType== eCTYPE_INT32
			||dataType== eCTYPE_LONG || dataType== eCTYPE_LONG64 )
		{
			m_rowMaxLength+=prec+1; //1 for negative sign
		}
		else if(dataType==eCTYPE_FLOAT || dataType==eCTYPE_DOUBLE)
		{
			m_rowMaxLength+=prec+1+1;  //1 for negative sign and 1 for decimal point.
			m_rowMaxLength+=1+1+1+3; //1 for leading point, 1 for Exp, 1 for exp sign, 3 for Exponent
		}
		else if(dataType==eCTYPE_CHAR)
		{
			if(prec==0)
			{
				//it could be a upgraded timetz.. so add timetz precision
				m_rowMaxLength+=21;
			}
			if(isColNchar == ITRUE)
			{
				//Even though powercenter is treating data in ascii mode, netezza driver can pass data in utf8 mode.
				m_rowMaxLength+=nMaxMBCSBytesPerChar*prec;
			}
			else
			{
				m_rowMaxLength+=prec;
			}

			if(m_bIsEscapeChar)
			{
				m_rowMaxLength+=prec; //escape character for each character. Also  Escape character would take atmost one byte.
			}
		}
		else if(dataType==eCTYPE_UNICHAR)
		{			
			if(isColNchar == ITRUE)
			{
				m_rowMaxLength+=nMaxMBCSBytesPerChar*prec;
			}
			else
			{
				m_rowMaxLength+=prec;
			}

			if(m_bIsEscapeChar)
			{
				m_rowMaxLength+=prec; //escape character for each character. Also  Escape character would take atmost one byte.
			}
		}
		else if(dataType==eCTYPE_TIME)
		{
			m_rowMaxLength+=prec;
		}
		else if(dataType==eCTYPE_DECIMAL18_FIXED)
		{
			m_rowMaxLength+=18+2;		//2 for minus sign
		}
		else if(dataType==eCTYPE_DECIMAL28_FIXED)
		{
			m_rowMaxLength+=28+2;		//2 for minus sign
		}
		else
		{
			m_rowMaxLength+=prec;
		}
		
		m_rowMaxLength+=5;  //default extra buffer
		this->m_colDoubleDimension[connColNum] = new ICHAR[m_rowMaxLength-oldLength];
		memset((void *)(this->m_colDoubleDimension[connColNum]),0,m_rowMaxLength-oldLength);
		connColNum++;
		oldLength=m_rowMaxLength;
	}

	//Add Collumn and Row delimiter
	m_rowMaxLength+=m_nNumOfConnectedCols+1; //m_nNumOfConnectedCols-1 collumn delimiter and 2 row delimiter.
	//This is just to make sure we dont cause memory corrution.	
}

ISTATUS PCNReader_Partition::initRunMethod(IRdrBuffer* const* const rdbuf)
{
	m_nCapacity=rdbuf[0]->getCapacity();
	if(this->m_nTraceLevel >= SLogger::ETrcLvlVerboseData)
		m_pLogger->logMsg(ILog::EMsgDebug,SLogger::ETrcLvlVerboseData,((IUString("").sprint(" Capacity [%ld]", m_nCapacity)).getBuffer()));

	m_nMaxBufferSize=m_nCapacity * m_rowMaxLength;
	m_pDataBuffer=new ICHAR[m_nMaxBufferSize];

	//27=precision for timestamp collumn(26) + Null termination
	m_pTempStr = new ICHAR[27];

	m_bEnableTestLoad=IFALSE;
	m_nTestLoadCount=0;

	//Flag is set in case test Load is enabled
	m_pUtils -> getIntProperty(IUtilsServer::ETestLoadCount,m_nTestLoadCount);
	if(m_nTestLoadCount > 0) 
	{
		m_bEnableTestLoad = ITRUE;
		if(this->m_nTraceLevel >= SLogger::ETrcLvlNormal)
			m_pLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_TEST_LOAD_ENABLE));
	}
	return ISUCCESS;
}

ISTATUS PCNReader_Partition::readAndProcessData(IRdrBuffer* const* const rdbuf)
{
	//Write it as if only one read using fread will do it all. when we add loop we need to check for eof in the start of loop
	m_nNumOfBytesRead=0;
	m_nNumOfBytesProcessed=0;
	m_bAllDataRead=IFALSE;
	m_nNumOfRowsRowsProcessed=0;
	m_nTimpFldCnt=0;
	IUINT32 bufferOffset=0;

	/*
	if AvailableBuffer>= MaxRowLen
	GetThisRow from buffer and assign PowerCenter buffer
	if AvailableBuffer < MaxRowLen and Feof is reached
	GetThisRow from buffer and assign PowerCenter buffer
	else
	get more buffer from pipe by using fread call

	if you every time make fread call for m_nMaxBufferSize buffer size then make sure processed buffer from m_pDataBuffer is copied some where.
	*/
	do
	{
		m_nNumOfBytesRead=fread(m_pDataBuffer+bufferOffset,1,m_nMaxBufferSize-bufferOffset,m_fHandle);
		m_nNumOfBytesRead+=bufferOffset;

		//this check will inform whether all data has been read?
		if(m_nNumOfBytesRead < m_nMaxBufferSize || feof(m_fHandle))
		{
			m_bAllDataRead=ITRUE;
		}
		//This check will let us know whether buffer can be parsed? we need to make sure we have alreast one row to parse in the buffer.
		m_nNumOfBytesProcessed=0;

		if(m_bEnableTestLoad==ITRUE)
		{
			while(m_nNumOfBytesRead - m_nNumOfBytesProcessed  >=  m_rowMaxLength)
			{
				if(IFAILURE==parseRowBuffer(rdbuf))
				{
					return IFAILURE;
				}
				m_nTestLoadCount--;
				if(m_nTestLoadCount==0)
				{
					rdbuf[0]->flush(m_nNumOfRowsRowsProcessed);
					return ISUCCESS;
				}
			}
		}
		else
		{
			while(m_nNumOfBytesRead - m_nNumOfBytesProcessed  >=  m_rowMaxLength)
			{
				if(IFAILURE==parseRowBuffer(rdbuf))
				{
					return IFAILURE;
				}
			}
		}
		/*		
		todo: we need to make sure we dont loose the remaining bytes by re-reading data in the same buffer.
		The remaining bytes: = m_nNumOfBytesRead - m_nNumOfBytesProcessed.. so the data from 
		m_pDataBuffer[m_nNumOfBytesProcessed] to m_pDataBuffer[m_nNumOfBytesRead] havent been processed.
		*/
		if(IFALSE==m_bAllDataRead)
		{
			bufferOffset=m_nNumOfBytesRead-m_nNumOfBytesProcessed;
			memcpy(m_pDataBuffer,m_pDataBuffer+m_nNumOfBytesProcessed,bufferOffset);
		}

	}while(m_bAllDataRead==IFALSE);

	//All the data has been read, but we still have some buffer to parse.

	if(m_bEnableTestLoad==ITRUE)
	{
		while(m_nNumOfBytesRead > m_nNumOfBytesProcessed )
		{
			if(IFAILURE==parseRowBuffer(rdbuf))
			{
				return IFAILURE;
			}
			m_nTestLoadCount--;
			if(m_nTestLoadCount==0)
			{
				rdbuf[0]->flush(m_nNumOfRowsRowsProcessed);
				return ISUCCESS;
			}
		}
	}
	else
	{
		while(m_nNumOfBytesRead > m_nNumOfBytesProcessed )
		{
			if(IFAILURE==parseRowBuffer(rdbuf))
			{
				return IFAILURE;
			}
		}
	}

	//If we havent reached maximum capacity in the last then we need to flush it out.
	if(m_nNumOfRowsRowsProcessed > 0)
	{
		if(rdbuf == NULL || rdbuf[0] == NULL) 
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_FLUSH_DATA_FAIL));
			return IFAILURE;
		}

		if(m_pUtils->hasDTMRequestedStop() == ITRUE && this->m_nDTMFlag == 0)
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse, m_pMsgCatalog->getMessage(MSG_RDR_DTM_STOP_REQUEST));

			this->m_nDTMFlag = 1;

			this->closePipe();
			return IFAILURE;
		}

		ISTATUS grpLessFlush = rdbuf[0]->flush(m_nNumOfRowsRowsProcessed);
		if( grpLessFlush != ISUCCESS )
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_FLUSH_DATA_FAIL));
			return IFAILURE;
		}
		//All the rows have been flushed so re-starting the count.
		m_nNumOfRowsRowsProcessed=0;
	}

	return ISUCCESS;
}

ISTATUS PCNReader_Partition::parseRowBuffer(IRdrBuffer* const* const rdbuf)
{
	IUINT32 colNum=0,index=0;
	this->m_colDoubleDimension[0][0] = '\0';
	colNum=0;
	index=0;
	m_nTimpFldCnt=0;
	if(IFALSE==m_bIsEscapeChar)
	{
		//SInce there will be only m_nNumOfConnectedCols delimiter in a row buffer.
		while(colNum < m_nNumOfConnectedCols)
		{
			if(m_pDataBuffer[m_nNumOfBytesProcessed] == m_cdelimiter[0])
			{
				this->m_colDoubleDimension[colNum][index]= '\0';
				index=0;
				colNum++;
				m_nNumOfBytesProcessed++;
			}
			else if(m_pDataBuffer[m_nNumOfBytesProcessed] == '\n')
			{
				this->m_colDoubleDimension[colNum][index]='\0';
				colNum++;
				m_nNumOfBytesProcessed++;
			}
			else
			{
				this->m_colDoubleDimension[colNum][index] = m_pDataBuffer[m_nNumOfBytesProcessed];
				index++;
				m_nNumOfBytesProcessed++;
			}
		}
	}
	else
	{
		for(int i=0; i<m_nNumOfConnectedCols; i++)
		{
			m_colHadEscapeChar[i] = IFALSE;
		}

		//Need to escape, escape character :-)
		while(colNum < m_nNumOfConnectedCols)
		{
			if(m_pDataBuffer[m_nNumOfBytesProcessed] == m_cdelimiter[0])
			{
				this->m_colDoubleDimension[colNum][index]= '\0';
				index=0;
				colNum++;
				m_nNumOfBytesProcessed++;
			}
			else if(m_pDataBuffer[m_nNumOfBytesProcessed] == '\n')
			{
				this->m_colDoubleDimension[colNum][index]='\0';
				colNum++;
				m_nNumOfBytesProcessed++;
			}
			else if(m_pDataBuffer[m_nNumOfBytesProcessed] == m_pEscapeChar[0])
			{
				//incase found escape character, we need to copy next character from the pipe to the buffer.
				this->m_colDoubleDimension[colNum][index] = m_pDataBuffer[++m_nNumOfBytesProcessed];
				index++;
				m_nNumOfBytesProcessed++;
				m_colHadEscapeChar[colNum] = ITRUE;
			}
			else
			{
				this->m_colDoubleDimension[colNum][index] = m_pDataBuffer[m_nNumOfBytesProcessed];
				index++;
				m_nNumOfBytesProcessed++;
			}
		}
	}

	if(IFAILURE==setPCBuffer(rdbuf))
	{
		return IFAILURE;
	}
	return ISUCCESS;
}


ISTATUS PCNReader_Partition::setPCBuffer(IRdrBuffer* const* const rdbuf)
{	
	ECDataType datatype;
	IUINT32    prec;
	IINT32     scale;
	void* data;  
	IINT16* ind; 
	IUINT32* len;
	IINT32* l_Intdata;
    ISFLOAT* l_Floatdata;
    ISDOUBLE* l_Doubledata;
    ICHAR* l_CharData;
	IINT64* l_Int64data;
	IINT32  month=0;
    IINT32  day=0;
    IINT32  year=0;
    IINT32  hr=0;
    IINT32  min=0;
    IINT32  sec=0;
	IDate* l_Date;
    IDate* l_DateData;
	BOOL allocmem = true;
	IUNICHAR*               pwzBuffer;      
	IUINT32 pulbyte;
	IUNICHAR* l_UnicharData;
	ICHAR *fmt; 
	IDecimal18Fixed* value18 ;
    IDecimal28Fixed* value28 ;
	IUINT32 buffLen;
	
	/*
	m_colDoubleDimension is based on connected dsq port.
	m_colmnAttr is based on all dsq fields.
	rdbuf[0] is  is based on all dsq fields.
	*/
	IUINT32 connColNum=0;
	for(IUINT32 colNum = 0; colNum < m_nNumOfCols; colNum++)
	{

		data   = rdbuf[0]->getDataDirect(colNum);
		ind  = rdbuf[0]->getIndicator(colNum);
		len = rdbuf[0]->getLength(colNum);

		if(IFALSE==this->m_colmnAttr[colNum].isLinked || NULL==data)
		{
			continue;
		}

		datatype = this->m_colmnAttr[colNum].m_dataType;
		prec = this->m_colmnAttr[colNum].m_nPrec;
		scale = this->m_colmnAttr[colNum].m_nScale;
		// default set indicator as invalid(kSDKDataNULL)
		ind[m_nNumOfRowsRowsProcessed] = kSDKDataNULL; //kSDKDataNull

		switch (datatype)
		{
		case eINVALID_CTYPE: // invalid data type
			assert(0);             

		case eCTYPE_SHORT:

			l_Intdata = (IINT32*) data;
			if(this->m_colDoubleDimension[connColNum][0]!= '\0')
			{

				IINT32 tempShort = atoi(this->m_colDoubleDimension[connColNum]);
				l_Intdata[m_nNumOfRowsRowsProcessed] = tempShort;
				ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;
			}
			break;

		case eCTYPE_LONG:

			l_Intdata = (IINT32*) data;
			if(this->m_colDoubleDimension[connColNum][0]!= '\0')
			{

				IINT32 tempLong = atol(this->m_colDoubleDimension[connColNum]);
				l_Intdata[m_nNumOfRowsRowsProcessed] = tempLong;
				ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;
			}
			break;

		case eCTYPE_FLOAT:

			l_Floatdata = (ISFLOAT*) data;
			if(this->m_colDoubleDimension[colNum][0]!= '\0')
			{
				ISFLOAT tempFloat = (ISFLOAT)atof(this->m_colDoubleDimension[connColNum]);
				l_Floatdata[m_nNumOfRowsRowsProcessed] = tempFloat;
				ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;           
			}
			break;

		case eCTYPE_DOUBLE:   //Numeric data Type
			l_Doubledata = (ISDOUBLE*) data;
			if(this->m_colDoubleDimension[connColNum][0]!= '\0')
			{
				ISDOUBLE tempDouble = atof(this->m_colDoubleDimension[connColNum]);
				l_Doubledata[m_nNumOfRowsRowsProcessed] = tempDouble;
				ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;               

			}
			break;

		case eCTYPE_CHAR:    // Text data Type

			if(prec==0)
			{
				ind[m_nNumOfRowsRowsProcessed] = kSDKDataNULL; 
				break;
			}

			if(m_bSyncNetezzaNullWithPCNull)
			{
				if(0 == strcmp(this->m_colDoubleDimension[connColNum] , m_pNullValue))
				{
					if(m_bIsEscapeChar)
					{
						if(IFALSE == m_colHadEscapeChar[connColNum])
						{
							ind[m_nNumOfRowsRowsProcessed] = kSDKDataNULL;
							break;
						}
					}
					else
					{
						ind[m_nNumOfRowsRowsProcessed] = kSDKDataNULL;
						break;
					}
				}
			}

			l_CharData = (ICHAR*) data;
			strcpy(l_CharData +((m_nNumOfRowsRowsProcessed)*(prec+1)),(ICHAR*)this->m_colDoubleDimension[connColNum]);
			len[m_nNumOfRowsRowsProcessed] = (IUINT32)strlen(this->m_colDoubleDimension[connColNum]);
			ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;

			break;

		case eCTYPE_TIME:

			month=0;day=0; year=0; hr=0; min=0;sec=0;

			l_DateData = (IDate*) data;
			l_Date = m_dateUtils->getDateFromArray(l_DateData, m_nNumOfRowsRowsProcessed);					

 			fmt = m_timeFormat.at(m_nTimpFldCnt);
			if(this->m_colDoubleDimension[connColNum][0]!= '\0' && fmt!=NULL)
			{												
				//Fix for Bug 1718, 1719 and CR 176962				

				//26 is returned from Netezza for timestamp
				if(26 == prec)
					strcpy(m_pTempStr, "0000-00-00 00:00:00.000000");
				//15 is returned from Netezza for time
				else if (15 == prec)
					strcpy(m_pTempStr, "00:00:00.000000");
				//10 is returned from Netezza for date
				else if (10 == prec)
					strcpy(m_pTempStr, "0000-00-00");

				strncpy(m_pTempStr,(const ICHAR*)this->m_colDoubleDimension[connColNum],strlen((const ICHAR*)this->m_colDoubleDimension[connColNum]));

				//fix for bug 1718,1719 
				ISTATUS status = IFAILURE;
				status = m_dateUtils->convertFrom(l_Date,(const ICHAR*)m_pTempStr ,fmt);
				if (ISUCCESS == status)
					ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;
				//end of fix for bug 1718,1719 
			}

			m_nTimpFldCnt ++;

			break;

		case eCTYPE_UNICHAR:

			if(this->m_colmnAttr[colNum].isColNchar==ITRUE)
				_ConvertToUnicode(this->m_pLocale_UTF8,this->m_colDoubleDimension[connColNum],&pwzBuffer,&pulbyte,allocmem);
			else
				_ConvertToUnicode(this->m_pLocale_Latin9,this->m_colDoubleDimension[connColNum],&pwzBuffer,&pulbyte,allocmem);

			if(m_bSyncNetezzaNullWithPCNull)
			{			
				if(0 == strcmp(this->m_colDoubleDimension[connColNum] , m_pNullValue))
				{
					if(m_bIsEscapeChar)
					{
						if(IFALSE == m_colHadEscapeChar[connColNum])
						{
							ind[m_nNumOfRowsRowsProcessed] = kSDKDataNULL;
							DELETEPTRARR <IUNICHAR>(pwzBuffer);
							break;
						}
					}
					else
					{
						ind[m_nNumOfRowsRowsProcessed] = kSDKDataNULL;
						DELETEPTRARR <IUNICHAR>(pwzBuffer);
						break;
					}
				}
			}

			l_UnicharData = (IUNICHAR*) data;
			buffLen = (IUINT32) IUStrlen(pwzBuffer) * sizeof(IUNICHAR);

			//fix for for CR 177422
			memset(l_UnicharData+ ((m_nNumOfRowsRowsProcessed)*(prec + (m_nNumOfRowsRowsProcessed > 0? 1 :0))), 0, buffLen+2); 
			//end of fix

			memcpy(l_UnicharData + ((m_nNumOfRowsRowsProcessed)*(prec + (m_nNumOfRowsRowsProcessed > 0? 1 :0))), pwzBuffer, buffLen);

			buffLen = (IUINT32)IUStrlen(pwzBuffer);                                                  
			len[m_nNumOfRowsRowsProcessed] = buffLen;
			ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;

			DELETEPTRARR <IUNICHAR>(pwzBuffer);

			break;

		case eCTYPE_DECIMAL18_FIXED: 

			if(this->m_colDoubleDimension[connColNum][0]!= '\0')
			{
				IDecimal18Fixed* l_Decimal18data;
				l_Decimal18data = (IDecimal18Fixed*) data;

				value18 = m_pDecimal18Utils->getDecimalFromArray( l_Decimal18data , m_nNumOfRowsRowsProcessed); 
				m_pDecimal18Utils->convertFrom(value18,(const ICHAR*)this->m_colDoubleDimension[connColNum] );
				ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;
			}
			break;

		case eCTYPE_DECIMAL28_FIXED:

			if(this->m_colDoubleDimension[connColNum][0]!= '\0')
			{
				IDecimal28Fixed* l_Decimal28data;
				l_Decimal28data = (IDecimal28Fixed*) data;

				value28 = m_pDecimal28Utils->getDecimalFromArray( l_Decimal28data , m_nNumOfRowsRowsProcessed); 
				m_pDecimal28Utils->convertFrom(value28,(const ICHAR*)this->m_colDoubleDimension[connColNum] );
				ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;
			}
			break;

		case eCTYPE_LONG64:
			//rjain bigint support
			l_Int64data = (IINT64*) data;						 
			if(this->m_colDoubleDimension[connColNum][0]!= '\0')
			{					
				IINT64 tempLong64 = convertStringToInt64(this->m_colDoubleDimension[connColNum]);
				l_Int64data[m_nNumOfRowsRowsProcessed] = tempLong64;
				ind[m_nNumOfRowsRowsProcessed] = kSDKDataValid;
			}
			break;

		default:
			ind[m_nNumOfRowsRowsProcessed] = kSDKDataNULL;    // kSDKDataNull
			break;

		} // end of switch statement
		connColNum++;
	}
	m_nNumOfRowsRowsProcessed++;
	if(m_nNumOfRowsRowsProcessed == m_nCapacity)
	{
		if(rdbuf == NULL || rdbuf[0] == NULL) 
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_FLUSH_DATA_FAIL));
			return IFAILURE;
		}

		if(m_pUtils->hasDTMRequestedStop() == ITRUE && this->m_nDTMFlag == 0)
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse, m_pMsgCatalog->getMessage(MSG_RDR_DTM_STOP_REQUEST));

			this->m_nDTMFlag = 1;

			this->closePipe();
			return IFAILURE;
		}

		ISTATUS grpLessFlush = rdbuf[0]->flush(m_nNumOfRowsRowsProcessed);
		if( grpLessFlush != ISUCCESS )
		{
			if(this->m_nTraceLevel >= SLogger::ETrcLvlTerse)
				m_pLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_FLUSH_DATA_FAIL));
			return IFAILURE;
		}
		//All the rows have been flushed so re-starting the count.
		m_nNumOfRowsRowsProcessed=0;
	}
	return ISUCCESS;
}


TRepMMDMetaExt* PCNReader_Partition::fetchMetaExtn(TSessionTask* pTask, PmUString sMetaExtNamePreFix, PM_UINT32 workflowId, PM_UINT32 sessionInstanceNameKey,
		PM_UINT32 sQInstanceNameKey, PM_UINT32 partitionId, PM_UINT32 attrId)
{
	TRepMMDMetaExt* tempMetaExt;
	PmUString sMetaExtName;
	sMetaExtName.makeEmpty();
	sMetaExtName.sprint("%s_%d_%u_%u_%d_%d", sMetaExtNamePreFix.buffer(), workflowId, sessionInstanceNameKey, sQInstanceNameKey, partitionId, attrId);
	tempMetaExt=pTask->getMetaExtVal(sMetaExtName,1);
	if(!tempMetaExt)
	{
		sMetaExtName.makeEmpty();
		sMetaExtName.sprint("%s_%u_%u_%d_%d", sMetaExtNamePreFix.buffer(), sessionInstanceNameKey, sQInstanceNameKey, partitionId, attrId);	
		tempMetaExt=pTask->getMetaExtVal(sMetaExtName,1);
	}
	return tempMetaExt;
}
