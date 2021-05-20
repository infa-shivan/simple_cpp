/****************************************************************
* File Name : Slogger.hpp
* Details   : Header file for SLogger.cpp.Contains definitions of function for logging
*****************************************************************/



#include "slogger.hpp"

SLogger::~SLogger(void)
{
}

void SLogger::logMsg(ILog::ELogMsgLevel eMsgLevel, ETraceLevel eTraceLevel, IUString sMessage)
{
	try
	{
		m_pLogger->logMsg(m_sPrefix,eMsgLevel,sMessage);
	}catch(...){}
}

void SLogger::logMsg(IINT32 nMsgCode,ILog::ELogMsgLevel eMsgLevel, ETraceLevel eTraceLevel, IUString sMessage)
{
	try
	{
		m_pLogger->logMsg(nMsgCode,m_sPrefix,eMsgLevel,sMessage);
	}catch(...){}
}

void SLogger::logMsg(ILog::ELogMsgLevel eMsgLevel, ETraceLevel eTraceLevel,IUINT32 msgID)
{
	try
	{
		m_pLogger->logMsg(m_pMsgCatalog,eMsgLevel, msgID );
	}catch(...){}
}

SLogger::SLogger(const IUtilsServer* pUtilsServer,
                 const ILog* pLogger,
                 const IMsgCatalog* pMsgCatalog)
:m_pUtilsServer(pUtilsServer),
 m_pLogger(pLogger),
 m_pMsgCatalog(pMsgCatalog)
{
    m_pSession = NULL;
    m_sPrefix=pMsgCatalog->getMsgPrefix(); 
    m_eTraceLevel=ETrcLvlTerse;
    IUINT32 nTrcLvl;
    m_pUtilsServer->getIntProperty(IUtilsServer::ETracingLevel, nTrcLvl);
    m_eTraceLevel = (ETraceLevel)nTrcLvl;
    if(m_eTraceLevel == ETrcLvlNone) //TODO: due to improper Tracing architecture(for target) by Informatica
        m_eTraceLevel = ETrcLvlNormal;
}

SLogger::SLogger(const IUtilsServer* pUtilsServer,
                 const ILog* pLogger)
:m_pUtilsServer(pUtilsServer),
 m_pLogger(pLogger)
{
    m_pSession = NULL;
    m_sPrefix="Net"; 
    m_eTraceLevel=ETrcLvlTerse;
    IUINT32 nTrcLvl;
    m_pUtilsServer->getIntProperty(IUtilsServer::ETracingLevel, nTrcLvl);
    m_eTraceLevel = (ETraceLevel)nTrcLvl;
    if(m_eTraceLevel == ETrcLvlNone) //TODO: due to improper Tracing architecture(for target) by Informatica
        m_eTraceLevel = ETrcLvlNormal;
}


SLogger::SLogger(SLogger& rSLogger, IUINT32 nDSQ)
{
    m_pSession=rSLogger.m_pSession;
    m_pLogger=rSLogger.m_pLogger ;
	m_sPrefix=rSLogger.m_sPrefix ;
	m_pMsgCatalog=rSLogger.m_pMsgCatalog ;
    SetTraceLevel(nDSQ);
}

SLogger::SLogger(const ISession* pSession, const ILog* pLogger)
{
    m_pSession=pSession;
	m_pLogger=pLogger;
    m_sPrefix="Net";
    m_eTraceLevel=ETrcLvlTerse;
}
SLogger::SLogger(const ISession* pSession,const ILog* pLogger, IUString sPrefix)
{
    m_pSession=pSession;
	m_pLogger=pLogger;
    m_eTraceLevel=ETrcLvlTerse;
    m_sPrefix=sPrefix;
    m_pMsgCatalog = NULL;
}



SLogger::SLogger(const ISession* pSession,const ILog* pLogger, const IMsgCatalog* pMsg, IUString sPrefix)
{
    m_pSession=pSession;
	m_pLogger=pLogger;
    m_eTraceLevel=ETrcLvlTerse;
    m_sPrefix=sPrefix;
    m_pMsgCatalog = pMsg;
}




SLogger::SLogger(const ISession* pSession,const ILog* pLogger, IUINT32 nDSQ)
{
    m_pSession=pSession;
    m_pLogger=pLogger;
    m_sPrefix="Net";
    m_eTraceLevel=ETrcLvlTerse;
    SetTraceLevel(nDSQ);
}
SLogger::SLogger(const ISession* pSession,const ILog* pLogger, IUINT32 nDSQ, IUString sPrefix)
{
    m_pSession=pSession;
    m_pLogger=pLogger;
    m_eTraceLevel=ETrcLvlTerse;    		
    SetTraceLevel(nDSQ);
    m_sPrefix=sPrefix;
}

IINT32 SLogger::getTrace()
{
	return this->m_eTraceLevel;
}


ISTATUS SLogger::SetTraceLevel(IUINT32 nDSQ)
{
    IUINT32 nTrcLvl;
    m_pSession->getTracingLevel(eWIDGETTYPE_DSQ,nDSQ, nTrcLvl);
    IUINT32 InitLevel=0, RunLevel=0, Stats=0, Code=0;
    if(nTrcLvl & ISession::ETraceInit1) InitLevel=1; 
    else if(nTrcLvl & ISession::ETraceInit2) InitLevel=2; 
    else if(nTrcLvl & ISession::ETraceInit3) InitLevel=3; 
    else if(nTrcLvl & ISession::ETraceInit4) InitLevel=4;

    if(nTrcLvl & ISession::ETraceRun1) RunLevel=1; 
    else if(nTrcLvl & ISession::ETraceRun2) RunLevel=2; 
    else if(nTrcLvl & ISession::ETraceRun3) RunLevel=3; 
    else if(nTrcLvl & ISession::ETraceRun4) RunLevel=4;

    if(nTrcLvl & ISession::ETraceStats) Stats=1;

    if(nTrcLvl & ISession::ETraceCode) Code=1;

    if(InitLevel==0 && RunLevel==0) m_eTraceLevel=ETrcLvlTerse;
    else if(InitLevel==0 && RunLevel==1) m_eTraceLevel=ETrcLvlNormal;
    else if(InitLevel==1 && RunLevel==1) m_eTraceLevel=ETrcLvlVerboseInit;
    else if(InitLevel==2 && RunLevel==2) m_eTraceLevel=ETrcLvlVerboseData;
	return ISUCCESS;
}
