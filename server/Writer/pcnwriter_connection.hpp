#ifndef  __PCN_WRITER__CONNECTION_
#define __PCN_WRITER__CONNECTION_

#include "stdafx.hpp"
#include "utils.hpp"
#include "msgcatalog.hpp"
#include "slogger.hpp"

#include "pmi18n/pmustring.hpp"
#include "utilsrv.hpp"

//Forward class declaration
class PCNWriter_Plugin;
class PCNWriter_Partition;
class IThreadMutex;

static const char *TCG_FLAG = "DisableNetezzaTCG";

//Class declaration
class PCNWriter_Connection : public PWrtConnection
{
public:
	// Constructor for the class.
	PCNWriter_Connection();
	PCNWriter_Connection(IUINT32 targetIndex,IUINT32 groupIndex,IUINT32 partIndex,const IConnection*);
	// Destructor for the class.
	~PCNWriter_Connection();
    // This function is called from framework and initiates the Netezza handles.
    ISTATUS init();
    // This function de-initiate the class.
    ISTATUS deinit();
	// This function commits to the Netezza.
	ISTATUS commit();
	//This function is called from plugin deinit. This function is created to make sure commit function gets called only once.
	ISTATUS commitFromDeinit();
    // Rollbacks are not supported in Netezza. So not implemented.
    ISTATUS rollback();
	// Returns weather the passed connections is same as this one.
    IBOOLEAN isEqual(const PWrtConnection* pConn);
	//Set db handles and set other envoirnment attributes.
	ISTATUS allocEnvHandles(SQLHENV &);
	//setting message handles
	void setMessageHandles(IINT32, SLogger *,MsgCatalog*);
	//Appending partition object in the vector, which is used to store all paritions under this connection
	void addPartitionObject(PCNWriter_Partition *);
	IBOOLEAN isCommitCalled();
	ISTATUS validateSerializationError();
	void lockCommitMutex();
	void unlockCommitMutex();
	void setCommitRetStatus(ISTATUS);
	ISTATUS getCommitRetStatus();


private:
	SQLHENV     m_henv ;
	IUINT32 m_targetIndex,m_groupIndex,m_partIndex;
	IBOOLEAN m_bIsDBHanleAllocated;
	MsgCatalog*	m_msgCatalog;
	SLogger *	m_sLogger;
	IINT32		traceLevel;
	IVector<PCNWriter_Partition *> m_partitionList;
	const IConnection *m_pConnection;
	IBOOLEAN m_bIsCommitCalled;
	IINT32 *m_tgtTableIndex;
	IVector<const PCNWriter_Partition*> m_partList_commitOrder;
	IThreadMutex *m_commitMutex;
	ISTATUS m_CommitRetStatus;
	PM_BOOLEAN m_bTCGOffFlag;
};
#endif // #ifndef __PCN_WRITER__CONNECTION_
