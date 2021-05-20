/*****************************************************************
* ----- Persistent Systems Pvt. Ltd., Pune ----------- 
* Copyright (c) 2005
* All rights reserved.
* Name        : PCNReader_Plugin.hpp
* Description : Plugin Driver Declaration
* Author      : Rashmi Varshney

*****************************************************************/

//PCNReader header files
#include "pcnreader_plugin.hpp"


/******************************************
* Name : PCNReader_Plugin
* Description : Constructor
******************************************/
PCNReader_Plugin::PCNReader_Plugin() :
m_pSession(NULL),
m_pPlugin(NULL),
m_pUtils(NULL)
{
}

/******************************************
* Name : ~PCNReader_Plugin
* Description : Destructor
******************************************/
PCNReader_Plugin ::~PCNReader_Plugin ()
{
    DELETEPTR <MsgCatalog>(m_pMsgCatalog);
    DELETEPTR <SLogger>(m_pLogger);
}

/********************************************************************
* Name : init
* Description : Called by the SDK framework once per PowerCenter 
*               session. All the metadata is passed to the plug-in
*               at this level.
* Returns : ISUCCESS if initialized else IFAILURE 
********************************************************************/
ISTATUS PCNReader_Plugin::init(const ISession* pSession, const IPlugin* pPlugin,
                               const IUtilsServer* pUtilsServer)
{
    m_pSession  = pSession;
    m_pPlugin   = pPlugin;
    m_pUtils    = pUtilsServer;

    const ILog* pLog;   
    pLog = m_pUtils->getLog();
    if (NULL == pLog)
        return IFAILURE;

    try
    {        
        m_pMsgCatalog = new MsgCatalog(m_pUtils->getUtilsCommon(),IUString("pmnetezza_"),IUString("Net"));   // Memory Deallocation 
    }
    catch(...)
    {   
        return IFAILURE;
    }

    //Set ILog in SLogger.
    //Logging can be done only after setting ILog instance
    try
    {
        m_pLogger = new SLogger(m_pSession,pLog);     // Memory Deallocation >> this->~PCNReader_Plugin.
    }
    catch(...)
    {
		pLog->logMsg(m_pMsgCatalog->getMessage(MSG_RDR_NETZ_READER),ILog::EMsgFatalError,m_pMsgCatalog->getMessage(MSG_CMN_CRT_PLOGGER_FAIL));
        return IFAILURE;
    }
	

    if (! (pUtilsServer->getRepository()->isOptionInstalled(eNetezza)) )
    {
        m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_CMN_NETZ_LIC_NOT_WORKING)); 
        return IFAILURE;
    }

    //get Source Qualifier metadata object
    if (pSession->getMapping()->getSQInstances(m_DSQList) != ISUCCESS)
    {
        m_pLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_pMsgCatalog->getMessage(MSG_RDR_GET_DSQ_LIST_FAIL)); 
        return IFAILURE;
    }	    
    return ISUCCESS;
}

/**************************************************************
* Name : validate
* Description : Can be used to do any session validation.
* Returns : ISUCCESS is returned if session is validated 
**************************************************************/
ISTATUS PCNReader_Plugin::validate()
{
    return ISUCCESS;
}

/*******************************************************************
* Name : createSQDriver
* Description : SDK framework calls this method once per Source 
*               Qualifier instance and per session run when the 
*               Source Qualifier is about to be started. dsqNum 
*            is the Source Qualifier number. 
* Returns : It returns an instance of PRdrSQDriver-derived
*           (PCNReader_DSQ) object in success or NULL if an error 
*           occurs.
*******************************************************************/
PRdrSQDriver* PCNReader_Plugin::createSQDriver(IUINT32 dsqNum)
{
    m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_CRT_DSQ_DRIVER));
    
    return new PCNReader_DSQ(m_pSession, m_pUtils, m_pLogger, dsqNum, 
        m_DSQList.at(dsqNum), m_pMsgCatalog);
}

/********************************************************************
* Name : destroySQDriver
* Description : SDK framework prompts the plug-in to destroy the
*               PRdrSQDriver-derived (PCNReader_DSQ) object. It is 
*               called when the Source Qualifier and all its 
*               partitions have shutdown. 
********************************************************************/
void PCNReader_Plugin::destroySQDriver(PRdrSQDriver* pSQDriver)
{
    m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_DEL_DSQ_DRIVER));

    DELETEPTR <PRdrSQDriver>(pSQDriver);
}

/********************************************************************
* Name : isRealTimeSQ
* Description : Queries the plug-in to see if the Source Qualifier
*                               can produce real-time flush requests.
* Returns : It sets the result to IFALSE as the real time flush 
requests cannot be produced. It returns ISUCCESS. 
********************************************************************/
ISTATUS PCNReader_Plugin::isRealTimeSQ(IUINT32 /*dsqNum*/, IBOOLEAN& result)
{
    // This is not realtime data return FALSE
    result  = IFALSE;
    return ISUCCESS;
}

/********************************************************************
* Name : deinit
* Description : Called by the SDK to mark the end of the plugin's 
*               lifetime. The session run status until this point is 
*               passed as an input. 
* Returns : ISUCCESS if de-initialized.
*********************************************************************/
ISTATUS PCNReader_Plugin::deinit(ISTATUS runStatus)
{
    m_pLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,m_pMsgCatalog->getMessage(MSG_RDR_DEINIT_PLUGIN));
    return ISUCCESS;
}
