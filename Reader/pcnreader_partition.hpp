/****************************************************************
* ----- Persistent Systems Pvt. Ltd., Pune ----------- 
* Copyright (c) 2005
* All rights reserved.
* Name        : PCNReader_Partition.hpp
* Description : Partition Driver Declaration
* Author      : Rashmi Varshney
****************************************************************/

#ifndef  _PCN_READER_PARTITION_
#define  _PCN_READER_PARTITION_

#define NETEZZA_SOURCE_NAME    IUString( "SourceTable" )
#define ERR_BUF_SIZE 512

//C++ header files
#include <assert.h>

#ifdef UNIX
//rjain since HANDLE definition had been commented in sqlunx.h due to some build errors, so defining HANDLE for unix.
typedef void *HANDLE;
typedef void *LPVOID;
#endif

#ifdef WIN32 //ined(DOS) || defined(OS2) || defined(NT) || defined(WIN32) || defined (MSDOS)
    #include <process.h>
    #include <fcntl.h>
    #include <io.h>
    #include <stdlib.h>
#else
    #include <fcntl.h>
    #include <sys/stat.h>
    #include <unistd.h>
    #include <sys/wait.h>
    #include <sys/types.h>
    #include <signal.h>             /* define signal(), etc.*/
    #include <sys/types.h>          /* various type definitions.*/
    #include <sys/ipc.h>            /* general SysV IPC structures */
    #include <sys/sem.h>            /* semaphore functions and structs */
    #include <stdlib.h>
    #include <sys/shm.h>
	
#endif
#include <string>

//using std::string;


#include "pcnreader_tconnection.hpp"
#include "pcnreader_dsq.hpp"
#include "pcnreader_metaextension.hpp"
#include "pcnreader_sessionattributes.hpp"
#include "sdkcmneb/ithreadmgr.hpp"

//API's included for undocumentation flag
#include "pmi18n/pmustring.hpp"
#include "utilsrv.hpp"
#include "rts/sessioninfo.hpp"

#define BUFSIZE 65535

class ISession;
class ISessionImpl;
class IUtilsServer;
class IUtilsDate;
//class ILog;
class IAppSQInstance;
class PCNReader_DSQ;
class PCNReader_Partition;
class SSessionInfo;
class TRepMMDMetaExt;


//registration file.


/*...........................................................
Structure containing Column related attribute
*/
struct ColumnAttr
{
    ECDataType	m_dataType;
    IUINT32		m_nPrec;
    IUINT32		m_nScale;
	IBOOLEAN isColNchar;         //if the column is nchar or nvarchar
	IBOOLEAN isLinked;
};

/*.....................................................................*/

struct ThreadArgs
{
    IUString		insertintoExtQuery;    
	IUString threadName;
    SQLHSTMT		hstmt;
    ISTATUS			retInsertintoExtQuery;        
	const IUtilsServer *pUtils;
	SLogger *sLogger;
	MsgCatalog*	msgCatalog;
	IUINT32 traceLevel;
	IUString pipeFullPath;
	PCNReader_Partition *pcnReaderObj;

    ThreadArgs(	
				IUString insertintoExtQuery,	
				IUString threadName,
				SQLHSTMT hstmt, 
				ISTATUS retInsertintoExtQuery, 				
				const IUtilsServer *pUtils,
				SLogger *sLogger,
				MsgCatalog*	msgCatalog,
				IUINT32 traceLevel
				);
};

void* runThreadQuery( LPVOID lpvParam );
void checkRet(SQLRETURN rc,IUString query,SQLHSTMT hstmt,MsgCatalog*	msgCatalog,SLogger *sLogger,IUINT32 traceLevel);

class PCNReader_Partition : public PRdrPartitionDriver
{
public:

    //Constructor
    PCNReader_Partition(const ISession*, const IUtilsServer*, SLogger*,
				        IUINT32 m_ndsqNum, const IAppSQInstance*,
						IUINT32 m_npartNum, MsgCatalog*, PCNReader_DSQ *,std::map<IINT32, IUString> arMetadataMap);
    //Destructor
    ~PCNReader_Partition();
    /****************************************************************
    * Called by the SDK framework after this object is created to allow
    * partition-specific initializations. Connection is established with
    * the Lotus Server and database are set. Check for form or views 
    * existence is done
    *****************************************************************/
    ISTATUS init();

    /*****************************************************************
    * This is the main entry point into the plug-in for reading
    * the data. It is called by the SDK framework and returns only
    * after all the data has been read.
    ******************************************************************/
    ISTATUS run(IRdrBuffer* const* const);

    /****************************************************************
    * Called by the SDK to mark the end of the partition's lifetime.
    * The session run status until this point is passed as an input.
    * Connection to the Lotus server is released.
    ******************************************************************/
    ISTATUS deinit(ISTATUS runStatus);

    //Create a external table query
    ISTATUS CreateExternalTable();
    //Cretaes a Pipe
    inline ISTATUS createPipe();
    //Opens Pipe in Read-Write mode
    inline ISTATUS openWRPipe();
    //closes pipe in deint or in case of any failure
    ISTATUS closePipe();
	//remove the pipe
	inline ISTATUS removePipe();

    ISTATUS prepareUnloadQuery ();
    //Gets the session attribute values ( DISTINCT,FILTER etc) from repository
    ISTATUS getSessionAttributes();
    //Stores the field information in data structures for use at run time
    ISTATUS UpdateFieldDetail() ; 
    //Generates Final Query to be fired at Database based on session and metadata values 
    ISTATUS GenerateFinalQuery();
    
    //Creates Locales for code page id Latin9 and UTF8
    ISTATUS createLocale();
    //Destroys the created locale
    ISTATUS destroyLocale();

    //Converts a char * string to IUNICHAR* using locale object basmed on code page set in connection object
    ISTATUS _ConvertToUnicode(	ILocale *pLocale, ICHAR* szCharSet,IUNICHAR** pwzCharSet,
								IUINT32 *pulBytes, BOOL bAllocMem);

	//converts a char* string to IINT64 
	inline IINT64 convertStringToInt64(ICHAR *str);
	inline PCNReader_DSQ *getParentDSQDriver();
	IBOOLEAN isPipeOpened();
	void calMaxRowLength();
	ISTATUS initRunMethod(IRdrBuffer* const* const);
	ISTATUS readAndProcessData(IRdrBuffer* const* const);
	ISTATUS parseRowBuffer(IRdrBuffer* const* const);
	ISTATUS setPCBuffer(IRdrBuffer* const* const);
	PM_UINT32 DJBHash(const PmUString&);

	TRepMMDMetaExt* fetchMetaExtn(TSessionTask* pTask, PmUString sMetaExtNamePreFix, PM_UINT32 workflowId, PM_UINT32 sessionInstanceNameKey,
		PM_UINT32 sQInstanceNameKey, PM_UINT32 partitionId, PM_UINT32 attrId);

private:
	//BEGIN New Feature for Table name and prefix override Ayan
	IBOOLEAN SearchForOverriddenSchemaAndTableName(IUString& strSchemaName, IUString& strTableName);
	//END New Feature for Table name and prefix override Ayan
	void printSessionAttributes();

private:
    PCN_TConnection*			m_connection; //Connnection object
    const ISession*				m_pSession;       // Contains all the session-level metadata.
	const ISessionImpl* 		m_pISessionImpl;
    const IUtilsServer*			m_pUtils;     // Contains the utils methods.
    FILE					    *m_fp;
    MsgCatalog*					m_pMsgCatalog;  // Message catalog
    SLogger*					m_pLogger;      // Used to log messages in the session log
    IUINT32						m_nDsqNum;     // Source Qualifier number.
    const IAppSQInstance*		m_pDSQ;           // Contains the Source Qualifier-specific metadata.
    IUINT32						m_nPartNum;        // Partition number. 
    const IUtilsDate*			m_dateUtils;  //Contains PowerCenter date utility methods
    ThreadArgs					*m_threadArgs;  //Thread structure pointer to me passed as argument
    ColumnAttr					*m_colmnAttr; // Column attribute structure
    IUString					m_srcTblName; //source table name
    const ISessionExtension*    m_pSessExtn;
    IUString                    m_fullPath; //Full Path to where pipe is created 
    HANDLE                      m_hPipe;     //Pipe Handle
    FILE*                       m_fHandle; // File handle to pipe 
    SQLHSTMT                    m_hstmt;        //Statement handle
    ICHAR **                    m_colDoubleDimension;
	IBOOLEAN*					m_colHadEscapeChar;
    NetMetaExtension*           m_pMetaExtension;
    NetSessionAttributes*       m_pSessionAttributes;
    IVector<const IField*>      m_DSQFields;
    IVector<const IField*>      m_srcFields;
    IVector<const IField*>      m_OutputLinkedFields;
    IVector<const IField*>      m_InputLinkedFields;
    IVector<const ITable*>      m_InputTableList;
    IVector<ICHAR *>            m_timeFormat;
	const IUtilsDecimal18*      m_pDecimal18Utils;
    const IUtilsDecimal28*      m_pDecimal28Utils;
    IUString                    m_finalQuery;
    IUString                    m_sDelimiter;
	ICHAR *		m_cdelimiter;
	IINT32 m_iDelimiter;
    IUString                    m_sCtrlchars;
    IUString                    m_sCrlnstring;
    IUString                    m_nullValue;
	ICHAR						*m_pNullValue;
    IUString                    m_sEscapeChar;
    ILocale                     *m_pLocale_Latin9;
	ILocale                     *m_pLocale_UTF8;
	ILocale						*m_pLocale_Latin1;
    IUINT32                     m_nTraceLevel;
    IUINT32                     m_nPid;
    IUINT32                     m_nDTMFlag;
    IUINT32                     m_EOFFlag;
	//BEGIN New Feature for Table name and prefix override Ayan
	IVector<const ISourceInstance*> m_l_srcList;
	IVector<IUString>			m_l_overRiddenSchema;
	IVector<IUString>			m_l_overRiddenTable;
	//END New Feature for Table name and prefix override Ayan
	IThreadHandle m_hThread;
	IThreadManager m_iThreadMgr;
	IUINT32 m_numPartitions;
	PCNReader_DSQ *m_parentDSQDriver;
	IBOOLEAN bIsPipeOpen;
	IBOOLEAN m_bIsRunMethodCalled;
	IINT32 m_rowMaxLength;
	IUINT32 m_nCapacity;
	ICHAR *m_pDataBuffer;
	IUINT32 m_nMaxBufferSize;
	IUINT32 m_nNumOfBytesRead;
	IUINT32 m_nNumOfBytesProcessed;
	IUINT32 m_nNumOfRowsRowsProcessed;
	IUINT32 m_nNumOfConnectedCols;
	IUINT32 m_nNumOfCols;
	IBOOLEAN m_bAllDataRead;
	IUINT32 m_nTimpFldCnt;
	ICHAR  * m_pTempStr;
	IBOOLEAN m_bIsEscapeChar;
	ICHAR	*m_pEscapeChar;
	IBOOLEAN m_bEnableTestLoad;
	IUINT32 m_nTestLoadCount;
	IUString m_socketBuffSize;
	PM_BOOLEAN m_b81103Delimiter;
	PM_BOOLEAN m_bTreatNoDataAsEmpty;
	PM_BOOLEAN m_bSyncNetezzaNullWithPCNull;
	//BEGIN FIX FOR INFA392252 Partition-ID resolution
	IUINT32		m_uiPartitionID;
	//END FIX FOR INFA392252 Partition-ID resolution
	IUString m_overridenPipePath;
	//for preSQl-postSQL changes - m_part_MetadataMap will be populated by init of DSQ
	std::map<IINT32, IUString> m_part_MetadataMap;
};
#endif //  _PCN_READER_PARTITION_
