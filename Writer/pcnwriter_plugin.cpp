/*******************************************************************

* Name			: PCNWriter_Plugin.cpp
* Description	: Plugin Driver Implementation

//Revision History
//Date 27/11/2007
1]Remove hard coded messages from code and add it into messgae catlog
*********************************************************************/


//PCN header files
#include "pcnwriter_plugin.hpp"

/********************************************************************
 * Name        : PCNWriter_Plugin
 * Description : Constructor
 * Returns     : None
 ********************************************************************/

PCNWriter_Plugin::PCNWriter_Plugin()        
:m_pSession(NULL), m_pPlugin(NULL) 
{ 
	m_vecTgtInst.clear(); 				  

}

/*********************************************************************
 * Name        : PCNWriter_Plugin
 * Description : Destructor
 * Returns     : None
 *********************************************************************/ 
      
PCNWriter_Plugin::~ PCNWriter_Plugin() 
{
	for(size_t i=0; i<m_vecTgtInst.length(); i++)
	{
		delete m_vecTgtInst.at(i);
		m_vecTgtInst.at(i) = NULL;
	}
	m_vecTgtInst.clear(); 
	delete m_pMsgCatalog;
    delete m_sLogger;
	this->m_pMsgCatalog = NULL;
	this->m_sLogger = NULL;
}

/*********************************************************************
 * Name        : init
 * Description : Init the Plugin
 * Returns     : init status
 *********************************************************************/

ISTATUS PCNWriter_Plugin::init(const ISession* pSession, const IPlugin* plugin,
                               const IUtilsServer* pUtilsSrv) 
{
	IVector<const ITargetInstance*> l_vecTgtInst;
	m_pMsgCatalog = NULL;
	m_sLogger = NULL;
	m_pSession = pSession;
	assert(m_pSession);
	m_pPlugin = plugin;
	assert(m_pPlugin);         
	m_pServerUtils = pUtilsSrv;

	const ILog* pLog;   
    pLog = m_pServerUtils->getLog();
    if (NULL == pLog)
        return IFAILURE;

	 m_pLogger = m_pServerUtils->getLog();
    if (NULL == m_pLogger)
        return IFAILURE;

	try
	{
       	m_pMsgCatalog = new MsgCatalog(m_pServerUtils->getUtilsCommon(),IUString("pmnetezza_"),IUString("Net"));   // Memory Deallocation 
	}
	catch(...)
	{
		return IFAILURE;
	}

	//Logging can be done only after setting ILog instance
	try
	{
		m_sLogger = new SLogger(m_pServerUtils,pLog);
	}
	catch(...)
	{
		pLog->logMsg(m_pMsgCatalog->getMessage(MSG_WRT_NETZ_WRITER),ILog::EMsgFatalError,m_pMsgCatalog->getMessage(MSG_CMN_CRT_PLOGGER_FAIL));
		return IFAILURE;
	}

	
    if (! (pUtilsSrv->getRepository()->isOptionInstalled(eNetezza)) )
	{
		m_pLogger->logMsg(m_pMsgCatalog->getMessage(MSG_WRT_NETZ_WRITER),ILog::EMsgError,m_pMsgCatalog->getMessage(MSG_CMN_NETZ_LIC_NOT_WORKING));
		return IFAILURE;
	}
	

	if(m_pSession->getMapping()->getTargetInstances(m_vecTgtInst) != ISUCCESS)
	{
		m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,this->m_pMsgCatalog->getMessage(MSG_WRT_GET_NETX_TRG_INST_FAIL));
		assert(0);
		return IFAILURE;
	}
	// Create target drivers  
	size_t l_nTargets = m_vecTgtInst.length();
	const ISessionExtension*	l_pSessExtn;
	for(size_t i=0;i<l_nTargets;i++) 
	{
		l_pSessExtn = m_pSession->getExtension(eWIDGETTYPE_TARGET,i);
		PCNWriter_Target* l_pTgtDriver = new PCNWriter_Target(m_pSession,m_vecTgtInst.at(i), i,this,l_pSessExtn,m_sLogger,m_pServerUtils,this->m_pMsgCatalog,this->m_pLogger);
		assert(l_pTgtDriver);
		m_vecTgtDrivers.insert(l_pTgtDriver);
	}
	assert(m_vecTgtInst.length() == m_vecTgtDrivers.length());
	return ISUCCESS;
}

/********************************************************************
 * Name            : validate
 * Description     : Validation code at the session level 
 * Returns         : validation status
 ********************************************************************/

ISTATUS PCNWriter_Plugin::validate()
{
	return ISUCCESS;    
}

/*********************************************************************
 * Name            : getTargetDriver
 * Description     : Get the target level handle for the given target
 *				   : index
 * Returns         : Returns the target level handle for the given 
					 target index
 *********************************************************************/

PWrtTargetDriver* PCNWriter_Plugin::getTargetDriver(IUINT32 targIndex)
{
    if( targIndex >= m_vecTgtDrivers.length())
    {  
		IUString tempVariableChar;
		tempVariableChar.sprint(m_pMsgCatalog->getMessage(MSG_WRT_TRG_INDX_INVALID),targIndex);
		m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,tempVariableChar);
        return NULL;         
    }

    return m_vecTgtDrivers.at(targIndex);
}

/**********************************************************************
 * Name            : isTransactional
 * Description     : is the given target partition transactional     
 * Returns         : return True if Transactional else return False
 **********************************************************************/
      
IBOOLEAN  PCNWriter_Plugin::isTransactional(IUINT32 targetInsID,
											IUINT32 groupID, IUINT32 partID) const 
{ 
 	return ITRUE; 
}                                 
 
/********************************************************************
 * Name            : deinit
 * Description     : Deinit Method of the Plugin
 * Returns         : deinit status
 ********************************************************************/

ISTATUS PCNWriter_Plugin::deinit(ISTATUS status)
{
 
	m_vecTgtDrivers.clear();
    return ISUCCESS;
}  


PWrtConnection* PCNWriter_Plugin::createConnection(IUINT32 targetIndex,
												   IUINT32 groupIndex,
												   IUINT32 partIndex)
{
	const ISessionExtension* l_pSessExtn= m_pSession->getExtension(eWIDGETTYPE_TARGET,targetIndex);

	//Getting the connection information
	IVector<const IConnectionRef*> vecConnRef; 

	l_pSessExtn->getConnections(vecConnRef,partIndex);

	//We support only one connection par target instance. 
	const IConnection*    pConn = NULL;
	pConn = (vecConnRef.at(0))->getConnection();
	vecConnRef.clear(); 

	PCNWriter_Connection *pPCNWriterConnection=new PCNWriter_Connection(targetIndex,groupIndex,partIndex,pConn);
	return pPCNWriterConnection;
}
