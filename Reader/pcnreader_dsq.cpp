
/****************************************************************** 
 * ----- Persistent Systems Pvt. Ltd., Pune -----------            
 * Copyright (c) 2005
 * All rights reserved.
 * Name        : PCNReader_DSQ.hpp
 * Description : Data Source Qualifier Driver Implementation.
 *               This class is instantiated for each Application 
 *               Source Qualifier connected to an Netezza Source.
 * Author      : Rashmi Varshney
 ********************************************************************/


#ifdef WIN32
	#include "stdafx.hpp" 
#endif

//PCNReader header files
#include "pcnreader_dsq.hpp"
#include "sdksrvin/iutlsrvi.hpp"



/*******************************************
* Name : PCNReader_DSQ
* Description : Constructor
*******************************************/
PCNReader_DSQ::PCNReader_DSQ(const ISession* pSession, const IUtilsServer* utilServer, 
                             SLogger* logger, IUINT32 dsqNum, 
                             const IAppSQInstance* dsq,
                             MsgCatalog* pMsgCatalog):
m_pSession (pSession),
m_pUtils (utilServer),
m_dsqNum (dsqNum),
m_pDSQ (dsq),
m_pMsgCatalog(pMsgCatalog)
{
    m_pLogger = new SLogger(*logger,m_dsqNum);
	m_bAddDataSliceId=IFALSE;
	this->m_pISessionImpl=(ISessionImpl*)m_pSession;
}

/*******************************************
* Name : ~PCNReader_DSQ
* Description : Destructor
*******************************************/
PCNReader_DSQ::~PCNReader_DSQ()
{
    DELETEPTR <SLogger>(m_pLogger);
}

/********************************************************************
* Name : init
* Description : Called by the SDK framework after this object is 
*               created to allow Source Qualifier-specific initializations.
* Returns : ISUCCESS if initialized else IFAILURE 
*******************************************************************/
ISTATUS PCNReader_DSQ::init()
{    
	//Initializing DSQ Driver
    m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,this->m_pMsgCatalog->getMessage(MSG_RDR_INIT_DSQ_DRIVER));
    //get the group count
    IUINT32 grpcnt = m_pDSQ->getTable()->getGroupCount();
    if (grpcnt > 0 )
    {
        m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,this->m_pMsgCatalog->getMessage(MSG_RDR_GRP_SRC_NOT_SUPPORTED));
        return IFAILURE;
    }

	const ISessionExtension*  pISE  = m_pSession->getExtension(eWIDGETTYPE_DSQ,m_dsqNum);

	IINT32 numPartitions=pISE->getPartitionCount();

	IUINT32 extensionPluginId= pISE->getSubType();	
	//If the extension is old one, that means used netezza dbtype is plugin based.
	if(extensionPluginId==PCN_Constants::OLD_NETEZZA_EXTENSION_SUBTYPE)
	{
        m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,this->m_pMsgCatalog->getMessage(MSG_CMN_OLD_EXTN));
        return IFAILURE;
	}

	IUString fltrString, sqlQry;
	IUString previousFltrString;
	/*
	In case of multiple partitions, if user 
	 1. has provided input in SQL query attribute for any partition. 
	 2. different inputs in the session extension filter query acorss partitions
	 3. different inputs in the session extension user define join query acorss partitions

	then we will set m_bAddDataSliceId as IFALSE.
	*/
	if(numPartitions > 1)
	{	
		m_bAddDataSliceId=ITRUE;

		const IUtilsServerImpl* pUtilSvrImpl = static_cast<const IUtilsServerImpl* >(this->m_pUtils);

		const SSession* pSSession =pUtilSvrImpl->getSSession();    

		TSessionTask* pTask=m_pISessionImpl->getSessionTask();

		PmUString sMetaExtName, sMetaExtFilterValue, sMetaExtPrevFilterValue;
		PmUString sSQInstanceName=(m_pDSQ->getName()).getBuffer();
		PmUString sSessionInstanceName=pSSession->getSessionName();
		PmUString sMetaExtNamePreFix=pmA2U("NetzOldSQLAttr");

		PM_UINT32 sessionInstanceNameKey=Utils::DJBHash(sSessionInstanceName);
		PM_UINT32 sQInstanceNameKey=Utils::DJBHash(sSQInstanceName);
		PM_UINT32 partitionId;
		PM_UINT32 attrId=3;				//Filter condition
		TRepMMDMetaExt* tempMetaExt;

		IINT32 tmpMsgCode;
		IUString errMsg;
		for(IINT32 i=0; i<numPartitions; i++)
		{
			//BEGIN FIX FOR INFA392252 Partition-ID resolution
			IUINT32 iPartitionID = pISE->getPartitionId(i);
			if( m_pISessionImpl->getSessWidgetInstanceAttr(eWIDGETTYPE_DSQ,m_dsqNum,iPartitionID,IUString(SQL_QUERY),sqlQry,errMsg) == IFAILURE)
			//END FIX FOR INFA392252 Partition-ID resolution
			{
				m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,errMsg);
				return IFAILURE;
			}

			if(!sqlQry.isEmpty())
			{
				m_bAddDataSliceId=IFALSE;
				break;
			}

			sMetaExtName.makeEmpty();
			const SSessionInfo* sessionInfo = ((SSession*)pSSession)->getSessInfo();
			PM_UINT32 workflowId = sessionInfo->getWorkflowId();
			//BEGIN FIX FOR INFA392252 Partition-ID resolution
			sMetaExtName.sprint("%s_%d_%u_%u_%d_%d",sMetaExtNamePreFix.buffer(),workflowId,sessionInstanceNameKey,sQInstanceNameKey,
				iPartitionID,attrId);			
			//END FIX FOR INFA392252 Partition-ID resolution

			tempMetaExt=pTask->getMetaExtVal(sMetaExtName,1);
			if(!tempMetaExt)
			{
				sMetaExtName.makeEmpty();
				sMetaExtName.sprint("%s_%u_%u_%d_%d",sMetaExtNamePreFix.buffer(),sessionInstanceNameKey,sQInstanceNameKey,
					partitionId,attrId);	
				
				tempMetaExt=pTask->getMetaExtVal(sMetaExtName,1);
			}
			if(tempMetaExt)
			{
				fltrString=(tempMetaExt->m_value).buffer();
			}
			else
			{
				//BEGIN FIX FOR INFA392252 Partition-ID resolution
				if( m_pISessionImpl->getSessWidgetInstanceAttr(eWIDGETTYPE_DSQ,m_dsqNum,iPartitionID,IUString(SOURCE_FILTER),fltrString,errMsg) == IFAILURE)
				//END FIX FOR INFA392252 Partition-ID resolution
				{
					m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,errMsg);
					return IFAILURE;
				}
			}
			
			if(i==0)
			{
				previousFltrString=fltrString;
			}
			else
			{
				if(fltrString != previousFltrString)
				{
					m_bAddDataSliceId = IFALSE;
					break;
				}
			}
		}
	}

    this->traceLevel = this->m_pLogger->getTrace();


	//prepost SQL
		std::map<IINT32, IUString> metadataMap;
		IVector<const IMetaExtVal*> valList;
		IVector<const ISourceInstance*> srcList;

		ILocale                     *pLocaleUTF8;
		IUtilsCommon *pUtils = IUtilsCommon::getInstance ();
		pLocaleUTF8 = pUtils->createLocale(UTF8_CODEPAGE_ID);

		m_pDSQ->getSourceList(srcList);
		const ISourceInstance* srcInst = srcList.at(0);
		IUINT32 nMetaExtn = 0;
		nMetaExtn = srcInst->getTable()->getMetaExtValList(valList,
							3,
							PCN_Constants::VENDORID, PCN_Constants::MEDOMAINID);
		IUString sMMD_extn_value("");
		for(IUINT32 i = 0; i < nMetaExtn; ++i)
		{
			const IMetaExtVal* pVal = valList.at(i);
			IUString extName = pVal->getName();
			if(extName == IUString(MMD_EXTN_READER_ATTR_LIST))
			{
				if(pVal->getValue(sMMD_extn_value) == ISUCCESS)
				{
					metadataMap = Utils::setSessionAttributesFromMMDExtn(sMMD_extn_value, pLocaleUTF8);
				}
			}
		}
		m_DSQMetadataMap = metadataMap;

		if ( m_DSQMetadataMap.empty() )
			return IFAILURE;

		IUString preSQL = PCN_Constants::PCN_EMPTY;
		std::map<IINT32,IUString>::iterator it = m_DSQMetadataMap.find(RDR_PROP_PRE_SQL);
		if(it == m_DSQMetadataMap.end() || 0 == it->second.compare(PCN_Constants::PCN_EMPTY))
			preSQL = PCN_Constants::PCN_EMPTY;
		else
			preSQL = it->second;

		if(!preSQL.isEmpty())
		{
			IUString printMsg = PCN_Constants::PCN_EMPTY;
			printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("PreSQL").getBuffer(), preSQL.getBuffer());
			m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);

			PCN_TConnection* temp_pcnRdrConn = new PCN_TConnection(m_pLogger,m_pMsgCatalog);
			IINT32 tempMsgCode;
			SQLRETURN   rc;
			SQLHSTMT  hstmt;		
			SQLTCHAR* uiStrToSQLChar;

			const ISessionExtension* pSessExtn = m_pSession->getSourceExtension(m_dsqNum, 0);
			if ( temp_pcnRdrConn->setConnectionDetails(pSessExtn,0,m_pUtils) == IFAILURE )
			{
				tempMsgCode = MSG_RDR_ERROR_READING_MANDATORY_CONN_ATTRS;
				throw tempMsgCode;
			}

			m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,this->m_pMsgCatalog->getMessage(MSG_RDR_EXEC_PRESQL_INFO));

			if(temp_pcnRdrConn->connect() == IFAILURE)
			{
				tempMsgCode = MSG_RDR_SER_CONN_NPC_FAIL;
				throw tempMsgCode;           
			}

			rc = SQLAllocHandle(SQL_HANDLE_STMT, temp_pcnRdrConn->getODbcHandle(), &hstmt);
			uiStrToSQLChar = Utils::IUStringToSQLTCHAR(preSQL);
			rc = SQLExecDirect(hstmt,uiStrToSQLChar,SQL_NTS);
			checkRet(rc,uiStrToSQLChar,hstmt,m_pMsgCatalog,m_pLogger,traceLevel);

			DELETEPTRARR <SQLTCHAR>(uiStrToSQLChar);
			DELETEPTR <PCN_TConnection>(temp_pcnRdrConn);
		}

	//end pre-post SQL
    return ISUCCESS;
}

/********************************************************************
* Name : createPartitionDriver
* Description : SDK framework calls this method once per 
*               Source Qualifier partition.partitionNum is the 
*               partition number.
* Returns : It returns an instance of PRdrPartitionDriver-derived 
*           (PCNReader_Partition) object.
*******************************************************************/
PRdrPartitionDriver* PCNReader_DSQ::createPartitionDriver(IUINT32 partitionNum)
{
    if(this->traceLevel >= SLogger::ETrcLvlNormal)
        m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,this->m_pMsgCatalog->getMessage(MSG_RDR_CRT_PART_DRIVER));

    return new PCNReader_Partition(m_pSession, m_pUtils, m_pLogger,
	m_dsqNum, m_pDSQ, partitionNum, m_pMsgCatalog,this,m_DSQMetadataMap);    
}

/********************************************************************
* Name : destroyPartitionDriver
* Description : SDK framework prompts the plug-in to destroy the 
*                    PRdrPartitionDriver-derived(PCNReader_Partition) object.
* Return : void
*******************************************************************/
void PCNReader_DSQ::destroyPartitionDriver(PRdrPartitionDriver* pDriver)
{
    if(this->traceLevel >= SLogger::ETrcLvlNormal)
        m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,this->m_pMsgCatalog->getMessage(MSG_RDR_DEL_PART_DRIVER));
    
    DELETEPTR <PRdrPartitionDriver>(pDriver);
}

/********************************************************************
* Name : deinit
* Description : Called by the SDK to mark the end of the Source 
*               Qualifier's lifetime. The session run status until 
*               this point is passed as an input
* Returns : ISUCCESS if de-initialized 
*******************************************************************/
ISTATUS PCNReader_DSQ::deinit(ISTATUS runStatus)
{

		IUString postSQL = PCN_Constants::PCN_EMPTY;
		std::map<IINT32,IUString>::iterator it = m_DSQMetadataMap.find(RDR_PROP_POST_SQL);
		if(it == m_DSQMetadataMap.end() || 0 == it->second.compare(PCN_Constants::PCN_EMPTY))
			postSQL = PCN_Constants::PCN_EMPTY;
		else
			postSQL = it->second;
	
		if(!postSQL.isEmpty())
		{
			IUString printMsg = PCN_Constants::PCN_EMPTY;
			printMsg.sprint(m_pMsgCatalog->getMessage(MSG_RDR_PRINT_SESSION_ATTRIBUTE),IUString("PostSQL").getBuffer(), postSQL.getBuffer());
			m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,printMsg);
			
			PCN_TConnection* temp_pcnRdrConn = new PCN_TConnection(m_pLogger,m_pMsgCatalog);
			IINT32 tempMsgCode;
			SQLRETURN   rc;
			SQLHSTMT  hstmt;		
			SQLTCHAR* uiStrToSQLChar;

			const ISessionExtension* pSessExtn = m_pSession->getSourceExtension(m_dsqNum, 0);
			if ( temp_pcnRdrConn->setConnectionDetails(pSessExtn,0,m_pUtils) == IFAILURE )
			{
				tempMsgCode = MSG_RDR_ERROR_READING_MANDATORY_CONN_ATTRS;
				throw tempMsgCode;
			}
			
			m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,this->m_pMsgCatalog->getMessage(MSG_RDR_EXEC_POSTSQL_INFO));

			if(temp_pcnRdrConn->connect() == IFAILURE)
			{
				tempMsgCode = MSG_RDR_SER_CONN_NPC_FAIL;
				throw tempMsgCode;           
			}

			rc = SQLAllocHandle(SQL_HANDLE_STMT, temp_pcnRdrConn->getODbcHandle(), &hstmt);
			uiStrToSQLChar = Utils::IUStringToSQLTCHAR(postSQL);
			rc = SQLExecDirect(hstmt,uiStrToSQLChar,SQL_NTS);
			checkRet(rc,uiStrToSQLChar,hstmt,m_pMsgCatalog,m_pLogger,traceLevel);

			DELETEPTRARR <SQLTCHAR>(uiStrToSQLChar);
			DELETEPTR <PCN_TConnection>(temp_pcnRdrConn);
		}

    if(this->traceLevel >= SLogger::ETrcLvlNormal)
        m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,this->m_pMsgCatalog->getMessage(MSG_RDR_DEINIT_DSQ_DRIVER));
    return ISUCCESS;
}

/********************************************************************
* Name : getAddDataSliceId
* Description : Return the boolean, which indicate whehter we need to
*               append dataslice id concept in the query
* Returns : m_bAddDataSliceId
*******************************************************************/
IBOOLEAN PCNReader_DSQ::getAddDataSliceId()
{
	return m_bAddDataSliceId;
}
