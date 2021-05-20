/****************************************************************
* File Name : Slogger.hpp
* Details   : Header file for SLogger.cpp.Contains description of function for logging
*****************************************************************/

#ifndef __SLOGGER_INCLUDED__
#define __SLOGGER_INCLUDED__


#include "sdkcmn/iustring.hpp"
#include "sdksrv/ilog.hpp"
#include "sdkcmn/isession.hpp"
#include "sdksrv/iutilsrv.hpp"
#include "sdkcmn/imsgcata.hpp"

//SLogger provides support of different log levels.
//
class SLogger
{
private:
    const IUtilsServer* m_pUtilsServer;     //used to get session level tracing value
    const ISession*    m_pSession;        //ISession is used to get mapping level Trace value
	const ILog*        m_pLogger;             //ILog is used to log the messages.
    const IMsgCatalog* m_pMsgCatalog;
    IUString           m_sPrefix;
public:
	enum ETraceLevel
	{
		ETrcLvlNone=0,
        ETrcLvlTerse,
		ETrcLvlNormal,
		ETrcLvlVerboseInit,
		ETrcLvlVerboseData
	};
    ETraceLevel m_eTraceLevel;
public:
    //This consturctor set the ILog and ISession Varaibles but TraceLevel is set to Terse
    SLogger(const ISession* ,const ILog*);  
    SLogger(const ISession* ,const ILog*, IUString sPrefix);
	SLogger(const ISession* ,const ILog*, const IMsgCatalog*, IUString sPrefix);
    SLogger(const IUtilsServer* ,const ILog*,const IMsgCatalog*);
	SLogger(const IUtilsServer* ,const ILog*);

    //This consturctor takes another Logger object and defines the TraceLevel from the DSQ.
    SLogger(SLogger& rSLogger, IUINT32 nDSQ);
    
    //Define the Log and Trace Levels
    SLogger(const ISession* ,const ILog*, IUINT32 nDSQ);
    SLogger(const ISession* ,const ILog*, IUINT32 nDSQ, IUString sPrefix);
    
private:
    ISTATUS SetTraceLevel(IUINT32 nDSQ);
public:
    //Logs the message based on the tracing level.
   	void logMsg(ILog::ELogMsgLevel eMsgLevel, ETraceLevel eTraceLevel, IUString sMessage);
    void logMsg(ILog::ELogMsgLevel eMsgLevel, ETraceLevel eTraceLevel,IUINT32 msgID);
	void logMsg(IINT32 nMsgCode,ILog::ELogMsgLevel eMsgLevel, ETraceLevel eTraceLevel, IUString sMessage);
	int getTrace();
    ~SLogger(void);
};
#endif
