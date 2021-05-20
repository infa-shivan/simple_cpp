/****************************************************************** 
 * ----- Persistent Systems Pvt. Ltd., Pune -----------            
 * Copyright (c) 2005
 * All rights reserved.
 * Name        : PCNReader_DSQ.hpp
 * Description : Data Source Qualifier Driver Declaration
 * Author      : Rashmi Varshney
 ********************************************************************/

#ifndef _PCN_READER_DSQ_
#define _PCN_READER_DSQ_
 
//PCNReader header files
#include "pcnreader_partition.hpp"      //Partition Driver declaration

class ISession;
class IUtilsServer;
class ILog;
class IAppSQInstance;
class IField;

extern void removeEscapeCharFromSQLStmt(ICHAR *attrValue);

class PCNReader_DSQ : public PRdrSQDriver
{
public:
    //Constructor
    PCNReader_DSQ (const ISession*, const IUtilsServer*, SLogger*, IUINT32 dsqNum,
                   const IAppSQInstance*, MsgCatalog*);

    //Destructor
    ~PCNReader_DSQ();

    /****************************************************************
     * Called by the SDK framework after this object is created to 
     * allow Source Qualifier-specific initializations.
     ****************************************************************/
    ISTATUS init();

    /****************************************************************
     * SDK framework calls this method once per Source Qualifier 
     * partition. partitionNum is the partition number.It returns an 
     * instance of PRdrPartitionDriver-derived (PCNReader_Partition) 
     * object.
     ****************************************************************/
    PRdrPartitionDriver* createPartitionDriver(IUINT32 partitionNum);

    /****************************************************************
     * SDK framework prompts the plug-in to destroy the 
     * PRdrPartitionDriver-derived (PCNReader_Partition) object.
     ****************************************************************/
    void destroyPartitionDriver(PRdrPartitionDriver*);

     /****************************************************************
     * Called by the SDK to mark the end of the Source Qualifier's
     * lifetime. The session run status until this point is passed as 
     * an input
     ****************************************************************/
    ISTATUS deinit(ISTATUS runStatus);

	IBOOLEAN getAddDataSliceId();

private:
    const ISession*                 m_pSession;         //Contains all the session-level metadata.
	const ISessionImpl* 			m_pISessionImpl;
    const IUtilsServer*             m_pUtils;           //Contains the utils methods.
    MsgCatalog*                     m_pMsgCatalog;      // Message catalog
    SLogger*                        m_pLogger;          // Used to log messages in the session log
    IUINT32                         m_dsqNum;           //Source Qualifier number.
    const IAppSQInstance*           m_pDSQ;             //Contains the Source Qualifier-specific metadata.   
    
    IVector<const IField*>          m_pDSQFields;       //Source Qualifier fields collection            
    IUINT32                         traceLevel;
	/*
	This boolean checks , whether we need to append dataslice id concept in the query for creating external table. 
	*/
	IBOOLEAN m_bAddDataSliceId;
	std::map<IINT32, IUString> m_DSQMetadataMap;
};
#endif  //_PCN_READER_DSQ_