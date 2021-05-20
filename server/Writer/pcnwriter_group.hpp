/****************************************************************
* Name 			: PCNWriter_Group.hpp
* Description	: Group Driver Declaration

*****************************************************************/

#if !defined(__PCN_WRITER_GROUP__)
#define __PCN_WRITER_GROUP__


//Informatica Header Files
#include "sdksrv/writer/pwgpdrv.hpp"
#include "sdkcmneb/ithreadmgr.hpp"
#include "sdkcmneb/isync.hpp"
#include "pcn_constants.hpp"

//Forward Declarations
class PCNWriter_Partition;
class ITargetInstance;
class PCNWriter_Plugin;
class PCNWriter_Connection;
class PWrtConnection;
class ISessionExtension;
class IThreadMutex;

//class declaration
class PCNWriter_Group : public PWrtGroupDriver
{
public:
	// Constructor
	PCNWriter_Group(const ISession*	m_pSession,
					const ITargetInstance* 
					pTargetInstance, IUINT32 tgtNum, 
					IUINT32 grpNum, PCNWriter_Plugin* plugDrv, 
					const ISessionExtension* pSessExtn,const ILog* pLogger,
					const IUtilsServer* pUtils);

	PCNWriter_Group(const ISession*	m_pSession,
					const ITargetInstance* 
					pTargetInstance, IUINT32 tgtNum, 
					IUINT32 grpNum, PCNWriter_Plugin* plugDrv, 
					const ISessionExtension* pSessExtn,SLogger* sLogger,
					const IUtilsServer* pUtils,MsgCatalog* msgCatalog,const ILog* pLogger,unsigned long int &,unsigned long int &,unsigned long int &);


	// Virtual destructor	   
	~ PCNWriter_Group();

	// Init the target, exactly once per session per process	  
	ISTATUS init(const IWrtGroupInit* );
	
	// Returns the parition level handle with the given partition ID	  
	PWrtPartitionDriver* getPartitionDriver(IUINT32 partIndex);

	// Get the connection for the given partition	 
	// It is guaranteed that all target partitions for 'this' group has 	  
	// already been deinit() and deleted.	   
	ISTATUS deinit(ISTATUS);

	IBOOLEAN isTempTableCreated();
	void setIfTempTableCreated(IBOOLEAN b);
	IINT32 getNoPartLoadedDataTemp();
	void incrementNoPartLoadedDataTemp();
	void lockPartDoneMutex();
	void unlockPartDoneMutex();
	IINT32 getPartitionCount();
	IINT32 getPartitionsInitializedCount();
	void incrementPartitionsInitializedCount();
	IUINT32 getTotalReqRows();
	void incrementTotalReqRows(IUINT32);
	void setIfTruncateTable(IBOOLEAN);
	IBOOLEAN getTruncateTable();
	IUString getTempTableName();

private:
	IVector <PCNWriter_Partition*>	m_vecPartDrivers;  
	const ITargetInstance*			m_pTargetInstance;
	IUINT32 						m_nGrpNum;
	IUINT32 						m_nTgtNum;
	const ISessionExtension*		m_pSessExtn; 
	PCNWriter_Plugin*				m_pPluginDriver;
	const ILog* 					m_pLogger;
	const IUtilsServer* 			m_pUtils;
	MsgCatalog*				 m_msgCatalog;
	SLogger *					 m_sLogger;
	IINT32						 m_itraceLevel;
	IINT32 m_numPartitions;
	IINT32 m_numPartitionsInitialized;
	unsigned long int *m_pluginRequestedRows;
	unsigned long int *m_pluginAppliedRows;
	unsigned long int *m_pluginRejectedRows;
	const ISession*	m_pSession;

	IBOOLEAN m_b_tempTableCreated;
	IINT32 m_noPartLoadedDataTemp; //This variable will count the no of partition have loaded data in to the temp table from the respective external table.
	IThreadMutex *m_partitionDoneMutex;
	IUINT32 m_totalReqRows;		//This variable will hold total number of requested rows across entire partitions. 
	IThreadMutex *m_totalReqRowsMutex;
	IBOOLEAN m_bTruncateTable;  //To check whether we need to trucnate table, this will be set if any partition 
		//have selected truncate target table option.
	IUString m_tempTableName;

	    // SQL types - for connection.
        IUString	m_WrtGrp_dsn ;
        IUString	m_WrtGrp_uid ;
        IUString    m_WrtGrp_pwd ;
		IUString	m_WrtGrp_databasename;
		IUString	m_WrtGrp_schemaname;
		IUString	m_WrtGrp_servername;
		IUString	m_WrtGrp_port;
		IUString	m_WrtGrp_drivername;
		IUString	m_WrtGrp_conStrAddtnlConf;
		SQLHENV     m_WrtGrp_henv ;
		SQLHDBC		m_WrtGrp_hdbc;

	// to get Connection details.
	//Set the DSN name , username and password
    ISTATUS setConnectionDetails(const ISessionExtension* m_pSessExtn,IUINT32 m_npartNum, const IUtilsServer* pUtils);
	void extractConnectionInformation(const ICHAR* attname, IUString& val,const IConnection* pConn, const IUtilsServer* pUtils);

    //Allocate the environment and connection handles and connect
    //connect to the datasource using the given username and password
    //Pointer to the Output file where the result is to be stored
    ISTATUS openDatabase();
	//Set db handles and set other envoirnment attributes.
	ISTATUS allocEnvHandles(SQLHENV &);
};

#endif // !defined(__PCN_WRITER_GROUP__)
