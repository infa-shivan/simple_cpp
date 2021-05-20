/****************************************************************

* Name                  : PCNWriter_Partition.cpp
* Description           : Partition level interface class

*****************************************************************/

#if !defined(__PCN_WRITER_PARTITION__)
#define __PCN_WRITER_PARTITION__

#ifdef WIN32 //#if defined(DOS) || defined(OS2) || defined(NT) || defined(WIN32) || defined (MSDOS)
        #include <process.h>
        #include <io.h>
#else //#elif defined(UNIX)
		#include <pthread.h>
        #include <fcntl.h>
        #include <sys/stat.h>
        #include <unistd.h>
        #include <sys/wait.h>
        #include <sys/types.h>
        #include <signal.h>             /* define signal(), etc.*/
        #include <stdio.h>
        #include <sys/types.h>          /* various type definitions.*/
        #include <sys/ipc.h>            /* general SysV IPC structures */
        #include <sys/sem.h>            /* semaphore functions and structs */
		#include<strings.h>
		#include <stdlib.h>
        #include <sys/shm.h>

#endif

// Header Files
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <ctype.h>


#ifndef WIN32
	#include <poll.h>
	#include <fcntl.h>
	#include <signal.h>
	#include <locale.h>
#endif



//rjain since HANDLE definition had been commented in sqlunx.h due to some build errors, so defining HANDLE for unix.
#ifdef UNIX
  typedef void *HANDLE;
  typedef void *LPVOID;
#endif


//informatica header

//PCNWriter header files
#include "pcnwriter_plugin.hpp"

#ifndef WIN32
#ifndef SQL_ATTR_APP_UNICODE_TYPE
#define SQL_ATTR_APP_UNICODE_TYPE 1064
#endif
#ifndef SQL_DD_CP_UTF16
#define SQL_DD_CP_UTF16 1
#endif
#endif

//API's included for undocumentation flag
#include "pmi18n/pmustring.hpp"
#include "utilsrv.hpp"
#include "pcnwriter_group.hpp" 
#include "pmtl/pmtldefs.h"
#include "sdksrvin/isessi.hpp"

#define BUFSIZE 65535

const short LINK_NULL = -1585;

struct IRowsStatInfo;
class ITargetInstance;
class ISessionExtension;
class IUtilsDate;
struct IDateFormatCHAR;
class PCNWriter_Plugin;
class IField;
class PCNWriter_Group;

using namespace std;

void* RunThreadQuery(LPVOID lpvParam);
void checkRet(SQLRETURN rc,IUString query,SQLHSTMT hstmt,MsgCatalog*	msgCatalog,SLogger *sLogger,IBOOLEAN& b,IUINT32 traceLevel);

//global variables

//structure to pass arguments to the thread

struct ThreadArgs
{
	IUString    updateQuery;       //for update query, update as update, update else insert, update as update
	IUString insertForUpsertQuery; //for insert query in case of update else insert operation
	IUString deleteQuery;         //query to be executed in case of delete operaion
	IUString    insertQuery;     //query to be executed in case of insert operaion
	IUString rejectRowQuery;	 //this query is responsible for logging rejected rows into the bad file(if the
	//user has provided Bad file name in to Bad File Name session extension attribute.
	IUString loadTempTableQuery;  //load data in to the temp table from the external table
	IUString getAffMinusAppQry; //Query for getting Affected rows minus Applied rows count.
	IUString    dropTempTable; //Drop temp table query
	IUString    dropExtTab;		//Drop external table query	
	IUString threadName;    //Child thread name used for session logging
	SQLHSTMT    hstmt;	//Statement handle
	//Fix for CR 177374
	SQLLEN	rowsInserted; //no of rows inserted in to the target table
	SQLLEN	rowsDeleted;  //no of rows get deleted from the target table
	SQLLEN	rowsUpdated; //no of rows get updated in to the target table.
	//end of fix
	SQLINTEGER affMinusAppCount;	//Affected rows minus Applied count
	IUINT32 traceLevel; //trace level used for session logging
	const IUtilsServer *pUtils;       //IutilsServer pointer, so that thread can 
	//initiate thread struct to enable external thread session logging
	SLogger *sLogger;    
	MsgCatalog*	msgCatalog; 
	IBOOLEAN bCheckAnyfailedQuery; //Whether any query is failed executed by child thread.
	IUString pipeFullPath;
	IUString deleteForUpsertQuery; 
	IBOOLEAN b81103Upsert;	//Boolean to check whether undocumentation flag for providing 81103 UPSERT 
	//functionality has been set

	PCNWriter_Partition *pcnWriterObj; //this will hold the corresponding object of partition driver. 
	SQLHDBC			hdbc; //Database handle
	IBOOLEAN b_isLastPartition;  //Whether the perticular partition is the last partition
	IBOOLEAN b_isLoadInToTgt;


	ThreadArgs(	IUString    updateQuery,
		IUString insertForUpsertQuery,
		IUString deleteQuery,
		IUString    insertQuery,
		IUString rejectRowQuery,		
		IUString loadTempTableQuery,
		IUString getAffMinusAppQry,
		IUString    dropTempTable,
		IUString    dropExtTab,		
		IUString threadName,
		SQLHSTMT    hstmt,		
		SQLLEN	rowsInserted,
		SQLLEN	rowsDeleted,
		SQLLEN	rowsUpdated,
		SQLINTEGER affMinusAppCount,		
		IUINT32 traceLevel,
		const IUtilsServer *pUtils,
		SLogger *sLogger,
		MsgCatalog*	msgCatalog,
		IBOOLEAN bCheckAnyfailedQuery,
		IBOOLEAN b81103Upsert
		);
};

//class declaration
class PCNWriter_Partition : public PWrtPartitionDriver
{

public:
        // Constructor
		 PCNWriter_Partition(const ISession* m_pSession,
												const ITargetInstance*
                                                pTargetInstance, IUINT32 tgtNum, IUINT32 grpNum,
                                                IUINT32 partNum, PCNWriter_Plugin* plugDrv,
                                                 SLogger* sLogger,
                                                const ISessionExtension* pSessExtn,
                                                const IUtilsServer* pUtils,MsgCatalog* msgCatalog,const ILog* pLogger,unsigned long int *,unsigned long int *,unsigned long int *, PCNWriter_Group *);

        // destructor
        ~ PCNWriter_Partition();

        // Init the target, exactly once per session per process
        ISTATUS init(IWrtPartitionInit*);

        EExecuteStatus execute(const IWrtBuffer* dbuff,
                                                   IWrtPartitionExecIn* input, IWrtPartitionExecOut* output);

        // It is guaranteed that all target partitions for 'this' group has
        // already been deinit() and deleted.
        ISTATUS deinit(ISTATUS);

        // Operator necessary for RW compilation
		IINT32 operator==( const PCNWriter_Partition& ) {assert(0); return(1);};

        //IUString to char pointer conversion

        //Set the DSN name , username and password
        ISTATUS setConnectionDetails(const ISessionExtension*	m_pSessExtn,IUINT32	m_npartNum, const IUtilsServer* pUtils);
		void extractConnectionInformation(const ICHAR* attname, IUString& val,const IConnection* pConn, const IUtilsServer* pUtils);

        //Allocate the environment and connection handles and connect
        //connect to the datasource using the given username and password
        //Pointer to the Output file where the result is to be stored
        ISTATUS openDatabase();

        //flush pipe
        inline ISTATUS flushPipe();

        // create pipe depending on OS
        inline ISTATUS createPipe();

        // open pipe
        ISTATUS openWRPipe();

        // close pipe
        ISTATUS closeWRPipe();

		//removing pipe
		inline ISTATUS removePipe();

        // Creates external tables, insert, modify, drop table query
        ISTATUS createExtTab();
        ISTATUS commit();

        ISTATUS autoCommitOFF();

        // Controls the loading of data into pipe.
        EExecuteStatus loadDataIntoPipe(const IWrtBuffer* dbuff,
                                        IWrtPartitionExecIn* input,
                                        IWrtPartitionExecOut* output);


        ISTATUS truncateTable();

        void flushPipe(IUString msgKey, IUString externalTable);
	    ISTATUS CreateExtTabCol();

        ISTATUS CreateExternalTable();

        ISTATUS createLocale();
		ISTATUS destroyLocale();
		/*
		This function is respnsible for appending distribution keys in the temp table schema, if all the distribution keys defined in the target table
		have been projected then we will choose same distribution keys for temp table also, otherwise we will not define distribution keys for temp tabl
		hence netezza will automatically choose distribution key.

		The reason for this is that the distribution keys are taken as a whole to determine which SPU the record will land on. 
		If any of the distribution keys are missing in the staging table, therefore, we will no longer land on the same SPU. 
		To avoid overloading any one SPU, random distribution has been used used.
		*/
		void addDistributionKey(IUString *createTempTableQuery, IVector<const IUString>, IVector<const IUString>);

		/*
		We changed Update else insert behavior in 8.6 from "delete followed by insert" to "do update else insert"
		this function is created for providing backward functionality. If user has added undocumentation flag then 
		we will provide old behavior. 
		*/
		void makeQueriesFor81103Upsert(IUString *,IUString *,IUString,IUString);

		ISTATUS   handlingHasDTMStop();
		PCNWriter_Group* getParentGroupDriver();
		//This function will set maximum length possible for any row.
		void calMaxRowLength();
		IBOOLEAN isAnyQueryFailed();
		IBOOLEAN isNoDataCase();
		ISTATUS childThreadHandling();
		IBOOLEAN isLoadInToTgt();
		//This function would be responsible for dropping of temp and external table in some special cases.
		void manageDropExtTempTable();
		IBOOLEAN bIsPipeOpened();
		IUString getTargetTblName();
		IBOOLEAN isQueryHaveWhereClause();
		SQLHDBC getDBHandle();
		ISTATUS getSessionExtensionAttributes();
		void printSessionAttributes();

private:
        IUINT32		m_nPartNum;
        IUINT32		m_nGrpNum;
        IUINT32		m_nTgtNum;
		IUINT32		m_rowsAs;

        IVector<const IField*>		m_inputFieldList;
        const ITargetInstance*		m_pTargetInstance;
        IRowsStatInfo*				m_pInsInfo;
        PCNWriter_Plugin*			m_pPluginDriver;
        const ISessionExtension*	m_pSessExtn;
        const ILog*                 m_pLogger;
        const IUtilsDate*           m_pDateUtils;
        const IUtilsDecimal18*      m_pDecimal18Utils;
        const IUtilsDecimal28*      m_pDecimal28Utils;
        IDateFormatCHAR*            m_pDateFormatChar;
        const IUtilsServer*         m_pUtils;
		const ISession*				m_pSession;

        // SQL types
        IUString	m_dsn ;
        IUString	m_uid ;
        IUString    m_pwd ;
		IUString	m_databasename;
		IUString	m_schemaname;
		IUString	m_servername;
		IUString	m_port;
		IUString	m_drivername;
		IUString	m_connectStrAdditionalConfigurations;
		SQLHENV     m_henv ;
        SQLHSTMT    m_hstmtDatatype ;
        IUString    m_tgtTable;
		IUString    m_tgtInstanceName;
        IUString    m_fullPath;
		IUString	m_errorLogDir;
        fstream     m_fileStream;   //output file
		IINT32		m_updstratFlg;  //flag to store update strategy

        IVector<const IUString>		m_keyFldNames;
        IVector<const IUString>		m_nonKeyFldNames;
		IVector<const IUString>		m_allFldNames;
		IVector<const IUString>		m_allLinkedFldNames;
		
		//  Fix for CR # 176520
		IINT32      m_InsertEscapeCHAR;
		// Fix for CR # 176520
        IUString    m_delimiter;
		ICHAR *		m_cdelimiter;
		IINT32 m_iDelimiter;
        IUString    m_ctrlchars;
        IUString    m_crlnstring;
        IUString    m_escapeChar;
		//Fix for 180628 CR
		const ICHAR *m_cescapeChar;
		const ICHAR *m_cnullVal;
		IBOOLEAN  m_bNULLCase;
		//end of fix
        IUString    m_nullValue;
        IUString    m_quotedValue;
		ICHAR *		m_cquotedValue;
        IUString    m_quoted;
		//Fix for CR 176528, 107699
		IUString    m_duplicateRowMechanism;
		IUString    m_cduplicateRowMechanism;
		//end of fix
		IINT32		tempTableCreated;
		IINT32      m_no_data_in_file;
        IINT32      m_ignoreKeyConstraint;
        IINT32      m_flgLessData;

        IVector<IINT32>	m_keyNotNull;
        IINT32			m_IsStaged;

        IVector<const IUString>		m_distributionKeyFlds;
        //creating member for thread argument
        ThreadArgs		*m_threadArgs  ;

        IINT32          m_IsUpdate,m_IsDelete,m_IsInsert;
		IUString    l_updStrat;

        HANDLE          m_hPipe;
		IINT32          m_pipeCreation;
        FILE*           m_fHandleUpdateDeleteInsert;
        FILE*           m_fHandle; //Will be set to UPDATE DELETE INSERT in loaddataintopipe
		IINT32          m_NoSelection ;
        //will store the SAME AS clause
        IUString        m_strColInfoSameAs;
        //Will store the TGT table schema
        IUString        m_strTgtTblSchema;
		//BEGIN New Feature for Table name and prefix override Ayan
		IUString		m_OriginalTableName;
		IUString		m_OriginalSchemaName;
		//END New Feature for Table name and prefix override Ayan

		//Statement handle need to define separate for All three function You cannot use same hamdle
        //for creating 3 EXT table

        SQLHSTMT        m_hstmt;
        SQLHDBC			m_hdbc;

        struct colInfo {
                        const void *data;
                        const IUINT32 *length;
                        const short *indicator;
        } *myCollInfo;

        struct cdatatype
        {
                ECDataType		dataType;
                IUINT32         prec;
                IINT32          scale;

        } *cdatatypes;

		//This structure stores information from the backend. 
		struct sqlDataTypes
		{
			IUString dataType;
			SQLINTEGER prec;
			SQLSMALLINT scale;
			IBOOLEAN isNUllable;
			IBOOLEAN isColNchar;         //if the column is nchar or nvarchar
		} *sqldatatypes;
        
		//this array is used to store which column are connected in target.
		IBOOLEAN *bIsColProjected;
		IBOOLEAN *bLoadPrecScale;

        struct datefmt
        {
                IDateFormatCHAR*        fmt;
        }*datefmts;

        IUString	destString;
		ICHAR		m_cdestString[65535];
		IUString	UpdateStrategy;
		IINT32		ParentID;

		IINT32		shmid;
        void 		*voidshared;
		IUINT32		m_rowsMarkedAsInsert;
        IUINT32		m_rowsMarkedAsDelete;
		IUINT32		m_rowsMarkedAsUpdate;

		IBOOLEAN 	m_dtmStopHandled;
		IUString	tempVariableChar ;
		MsgCatalog*	m_msgCatalog;
		SLogger *	m_sLogger;
		IINT32		traceLevel;
		ILocale		*m_pLocale_Latin9;
		ILocale		*m_pLocale_UTF8;
		ILocale		*m_pLocale_Latin1;
		ISTATUS		freeHandle();

		IThreadHandle m_hThread;
		IThreadManager m_iThreadMgr;
		IUString m_badFileName;
		IBOOLEAN m_bCreateBadFile;
		unsigned long int *m_pluginRequestedRows;
		unsigned long int *m_pluginAppliedRows;
		unsigned long int *m_pluginRejectedRows;		
		PM_BOOLEAN m_b81103Upsert;
		PM_BOOLEAN m_b81103UpsertWithIgnoreKey;
		PM_BOOLEAN m_b81103Delimiter;
		PCNWriter_Group *m_parent_groupDriver;
		IBOOLEAN m_bLoadInToTarget;
		IUString m_tempTblName;
		IBOOLEAN m_bIsPipeCreated;
		IBOOLEAN m_bIsPipeOpened;
		IBOOLEAN m_bIsPipeClosed;
		IBOOLEAN m_bIsChildThreadSpawned;
		IINT32 m_rowMaxLength;
		IUString m_socketBuffSize;
		PCNWriter_Connection *m_pcnWriterConnection;
		IBOOLEAN m_bChildThreadHandlingCalled;
		PM_BOOLEAN m_bNoDataFlag;
		PM_BOOLEAN m_bTreatNoDataAsEmpty;
		PM_BOOLEAN m_bSyncNetezzaNullWithPCNull;
		IBOOLEAN m_bAddEscapeCharForNullChar;
		ICHAR*  unicharData; //EBF11378

};

#endif // !defined(___PCN_WRITER_PARTITION__)


