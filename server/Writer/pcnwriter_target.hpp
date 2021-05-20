/****************************************************************
* Name 	   : PCNWriter_Target.hpp
* Description : Target Driver Declaration

*****************************************************************/


#if !defined(_PCN_WRITER_TARGET_)
#define _PCN_WRITER_TARGET_

//C++ header files
#include "stdafx.hpp"
#include "assert.h"

#include "msgcatalog.hpp"

//localization header files

#include "slogger.hpp"


//forward declarations
class ITargetInstance;
class PCNWriter_Plugin;
class PCNWriter_Group;
class PWrtConnection;
class ISessionExtension;


//class declaration
class PCNWriter_Target : public PWrtTargetDriver
{
public:
	// Constructor
	PCNWriter_Target(const ISession*	m_pSession,
					 const ITargetInstance* pTargetInstance, 
					 IUINT32 targNum,
					 PCNWriter_Plugin* plugDrv,
					 const ISessionExtension* pSessExtn,
					 const ILog* pLogger,
					 const IUtilsServer* pUtils);

	PCNWriter_Target(const ISession*	m_pSession,
					 const ITargetInstance* pTargetInstance, 
					 IUINT32 targNum,
					 PCNWriter_Plugin* plugDrv,
					 const ISessionExtension* pSessExtn,
					 SLogger* sLogger,
					 const IUtilsServer* pUtils,MsgCatalog *msgCatalog,const ILog* pLogger);


	// Virtual destructor	   
	~ PCNWriter_Target();
	
	// Init the target, exactly once per session per process	  
	ISTATUS init(const IWrtTargetInit* );
	
	// Returns the parition level handle with the given partition ID	  
	PWrtGroupDriver* getGroupDriver(IUINT32 grpId);
	
	// It is guaranteed that all target partitions for 'this' group has 	  
	// already been deinit() and deleted.	   
	ISTATUS deinit(ISTATUS);
	unsigned long int m_pluginRequestedRows;
	unsigned long int m_pluginAppliedRows;
	unsigned long int m_pluginRejectedRows;


private:
	IVector <PCNWriter_Group*>		 m_vecGrpDrivers;	 
	const ITargetInstance*			 m_pTargetInstance;
	IUINT32 						 m_nTargNum;
	PCNWriter_Plugin*				 m_pPluginDriver;
	const ISessionExtension*		 m_pSessExtn;
	const ILog* 					 m_pLogger;
	 SLogger *					 m_sLogger;
	const IUtilsServer* 			 m_pUtils;
	MsgCatalog*				 m_msgCatalog;
	int						 m_itraceLevel;
	const ISession*	m_pSession;

};

#endif // !defined(__PCN_WRITER_TARGET__)
