/****************************************************************
* Name		   : PCNWriter_Plugin.hpp
* Description  : Plugin Driver Declaration

*****************************************************************/

#ifndef _PCN_WRITER_PLUGIN_
#define _PCN_WRITER_PLUGIN_

//PCNWriter Header File
#include "pcnwriter_connection.hpp"
#include "pcnwriter_target.hpp"

#ifndef WIN32
#include <unistd.h>
#endif

//Forward Declarations
class ITargetInstance;
class ISessionExtension;

//class declaration				
class PCNWriter_Plugin : public PWrtPluginDriver
{
public:
    // Constructor
    PCNWriter_Plugin();

    // Virtual destructor      
    ~ PCNWriter_Plugin();
    
    // Init the target, exactly once per session per process      
    ISTATUS init(const ISession* pSession, const IPlugin* plugin,
                 const IUtilsServer* utils);

    // Can be used to validate the session      
    ISTATUS validate();
    
    // Returns the target level handle with the given target ID      
    PWrtTargetDriver* getTargetDriver(IUINT32 targID);

    // Is the given target partition transactional     
    IBOOLEAN  isTransactional(IUINT32 targetInsID,
                              IUINT32 groupID, IUINT32 partID) const;

    // It is guaranteed that all targets for 'this' plugin have        
    // already been deinit() and deleted.      
    ISTATUS deinit(ISTATUS);	

	PWrtConnection* createConnection(IUINT32 /*targetIndex */,
		IUINT32 /*groupIndex */,
		IUINT32 /* partIndex*/);
	
	const IUtilsServer*		m_pServerUtils;
	
private:
	// Instances received from pmserver
	IVector<const ITargetInstance*> 		m_vecTgtInst;
	// Drivers to be provided to pmserver
	IVector<PCNWriter_Target*>				m_vecTgtDrivers;
	// Instances received from pmserver
	const ISession* 						m_pSession;   
	const IPlugin*							m_pPlugin;
	const ILog* 							m_pLogger;
	MsgCatalog*								m_pMsgCatalog;	// Message catalog
	SLogger*								m_sLogger;	// Used to log messages in the session log
};

#endif // !defined(___PCN_WRITER_PLUGIN_)
