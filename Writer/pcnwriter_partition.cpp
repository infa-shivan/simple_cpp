/****************************************************************

// Name          : PCNWriter_Partition.cpp
// Description   : Partition level interface class
//Revision History
//Date 13/11/2007
//Following function and structure are remove or update to FIX CR177639 
1]loadTable function.
2]createSharedMemory function.
3]SharedFlag struc
4]comment all call of kill method.
5]Updtae init function.
6]update deinit function.

Implement pThreads instead of fork to FIX CR177639 

//Date 27/11/2007
1]Implement to enforce primary key at connector level
2]Remove all comented code.
3]Remove hard coded message from code and add it into message catlog
*****************************************************************/
//PCN headers

#include "pcnwriter_partition.hpp"
#include <math.h>

#define   STR_LEN           128+1
#define   UPDATE            1
#define   INSERT            2
#define   UPSERT            3
#define   NONE              0
// Fix for CR # 176520
#define DEFAULT_BUFFER	50
extern ICHAR* PCNwriterAttributeDefinitions[];
// Fix for CR # 176520

//array to hold pow(10,x) where x<=15.
//This saves the cost of 'pow'
const ISDOUBLE powersof10[16] = { 1, 10,
	100,
	1000,
	10000,
	100000,
	1000000,
	10000000,
	100000000,
	1000000000,
	10000000000,
	100000000000,
	1000000000000,
	10000000000000,
	100000000000000,
	1000000000000000
};
// forward declaration
class ILink;

//!Thread structure constructor
ThreadArgs::ThreadArgs(
					   IUString    updateQuery,
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
					   SQLINTEGER	affMinusAppCount,					   
					   IUINT32 traceLevel,
					   const IUtilsServer *pUtils,
					   SLogger *sLogger,
					   MsgCatalog*	msgCatalog,
					   IBOOLEAN bCheckAnyfailedQuery,
					   IBOOLEAN b81103Upsert
					   )
{
	this->updateQuery=updateQuery;
	this->insertForUpsertQuery=insertForUpsertQuery;
	this->deleteQuery=deleteQuery;
	this->insertQuery=insertQuery;
	this->rejectRowQuery=rejectRowQuery;	
	this->loadTempTableQuery=loadTempTableQuery;
	this->getAffMinusAppQry=getAffMinusAppQry;
	this->dropTempTable=dropTempTable;
	this->dropExtTab=dropExtTab;
	this->threadName=threadName;
	this->hstmt=hstmt;
	this->rowsInserted=rowsInserted;
	this->rowsDeleted=rowsDeleted;
	this->rowsUpdated=rowsUpdated;
	this->affMinusAppCount=affMinusAppCount;	
	this->traceLevel=traceLevel;
	this->pUtils=pUtils;
	this->sLogger=sLogger;
	this->msgCatalog=msgCatalog;
	this->bCheckAnyfailedQuery=bCheckAnyfailedQuery;
	this->b81103Upsert=b81103Upsert;
	this->b_isLastPartition=ITRUE;
	this->b_isLoadInToTgt=ITRUE;
};


/*******************************************************************
* Name        : PCNWriter_Partition
* Description : Constructor
* Returns     : None
*******************************************************************/
PCNWriter_Partition::PCNWriter_Partition(const ISession* m_pSess,
										 const ITargetInstance* 
										 pTargetInstance, IUINT32 tgtNum, 
										 IUINT32 grpNum, IUINT32 partNum, 
										 PCNWriter_Plugin* plugDrv, 
										 SLogger*    sLogger,
										 const ISessionExtension* pSessExtn,
										 const IUtilsServer* pUtils,MsgCatalog* msgCatalog,const ILog* pLogger,unsigned long int * pluginRequestedRows,unsigned long int * pluginAppliedRows,unsigned long int * pluginRejectedRows, PCNWriter_Group *parentGrp)
										 : m_pSession(m_pSess),m_nTgtNum(tgtNum), m_nGrpNum(grpNum),
										 m_nPartNum(partNum), m_pPluginDriver(plugDrv),
										 m_sLogger(sLogger),
										 m_pSessExtn(pSessExtn),m_pUtils(pUtils),m_msgCatalog(msgCatalog),m_pLogger(pLogger)
{
	m_pTargetInstance = pTargetInstance;
	m_pDateUtils = m_pPluginDriver->m_pServerUtils->getUtilsCommon()->getUtilsDate();
	m_pDecimal18Utils = m_pPluginDriver->m_pServerUtils->getUtilsCommon()->getUtilsDecimal18();
	m_pDecimal28Utils = m_pPluginDriver->m_pServerUtils->getUtilsCommon()->getUtilsDecimal28();
	m_updstratFlg = -1;
	m_no_data_in_file = 0;
		m_threadArgs = new ThreadArgs(PCN_Constants::PCN_EMPTY,PCN_Constants::PCN_EMPTY,PCN_Constants::PCN_EMPTY,PCN_Constants::PCN_EMPTY
		,PCN_Constants::PCN_EMPTY,PCN_Constants::PCN_EMPTY,PCN_Constants::PCN_EMPTY,PCN_Constants::PCN_EMPTY,PCN_Constants::PCN_EMPTY
		,PCN_Constants::PCN_EMPTY,NULL,0,0,0,0,0,NULL,NULL,NULL,IFALSE,IFALSE);
	m_fHandle = NULL;
	m_flgLessData = 0;
	m_IsStaged = 0;
	m_fullPath = PCN_Constants::PCN_EMPTY;
	m_IsUpdate = 0;
	m_IsDelete = 0;
	m_IsInsert = 0;
	m_pipeCreation = 0;
	m_fHandleUpdateDeleteInsert=NULL;
	m_hstmt=NULL;
	m_ignoreKeyConstraint =0;
	m_NoSelection = 0;  //Will be set if nothing is selected Added to check if the Pipe was opened
	m_rowsMarkedAsInsert=0;
	m_rowsMarkedAsDelete=0;
	m_rowsMarkedAsUpdate=0;
	m_dtmStopHandled = IFALSE;
	m_pluginRequestedRows=pluginRequestedRows;
	m_pluginAppliedRows=pluginAppliedRows;
	m_pluginRejectedRows=pluginRejectedRows;
	m_pLocale_Latin9 = NULL;
	m_pLocale_UTF8 = NULL;
	m_pLocale_Latin1 = NULL;
	m_pInsInfo=NULL;
	m_parent_groupDriver=parentGrp;
	m_bIsPipeCreated=IFALSE;
	m_bIsPipeOpened=IFALSE;
	m_bIsPipeClosed=IFALSE;
	m_bIsChildThreadSpawned=IFALSE;
	m_pcnWriterConnection=NULL;
	m_bChildThreadHandlingCalled=IFALSE;
	m_bTreatNoDataAsEmpty=PM_FALSE;
	m_bSyncNetezzaNullWithPCNull=PM_FALSE;
	m_bNULLCase=IFALSE;
	unicharData = NULL;
	m_dsn = PCN_Constants::PCN_EMPTY;
    m_uid = PCN_Constants::PCN_EMPTY;
    m_pwd = PCN_Constants::PCN_EMPTY;
	m_databasename = PCN_Constants::PCN_EMPTY;
	m_schemaname = PCN_Constants::PCN_EMPTY;
	m_servername = PCN_Constants::PCN_EMPTY;
	m_port = PCN_Constants::PCN_EMPTY;
	m_drivername = PCN_Constants::PCN_EMPTY;
	m_connectStrAdditionalConfigurations = PCN_Constants::PCN_EMPTY;
 }

/*********************************************************************
* Name        : ~PCNWriter_Partition
* Description : Virtual destructor
* Returns     : None
********************************************************************/

PCNWriter_Partition::~ PCNWriter_Partition()
{	
	DELETEPTR <ThreadArgs>(m_threadArgs);
	DELETEPTRARR <ICHAR>(m_cdelimiter);
	DELETEPTRARR <const ICHAR>(m_cescapeChar);
	DELETEPTRARR <const ICHAR>(m_cnullVal);
	DELETEPTRARR <colInfo>(myCollInfo);
	DELETEPTRARR <cdatatype>(cdatatypes);
	DELETEPTRARR <sqlDataTypes>(sqldatatypes);
	DELETEPTRARR <IBOOLEAN>(bIsColProjected);	
	DELETEPTRARR <datefmt>(datefmts);
	DELETEPTRARR <ICHAR>(m_cquotedValue);
	DELETEPTRARR <IBOOLEAN>(bLoadPrecScale);    
	FREEPTR <ICHAR>(unicharData);

	destroyLocale();

	if(this->traceLevel >= SLogger::ETrcLvlNormal)
		m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,this->m_msgCatalog->getMessage(MSG_WRT_PART_DEINIT_SUCC));
}

/*********************************************************************
* Name        : init
* Description : Init the Partition and connect to Netezza database
* Returns     : init status
*********************************************************************/

ISTATUS PCNWriter_Partition::init(IWrtPartitionInit* pInit)
{		
	ISTATUS iResult = ISUCCESS;
	IINT32 tempMsgCode;
	IUString    l_IURejectFileDir;
	IUString    l_IURejectFileName;
	SQLTCHAR * tempSQLTChar;
	
	try
	{
		this->traceLevel = this->m_sLogger->getTrace();
		const ITable* pTgtTable  = m_pTargetInstance->getTable();
		m_tgtInstanceName = m_pTargetInstance->getName();
		m_tgtTable = pTgtTable->getName();
		IUString tempTgtTable = "";
		const ISessionImpl* m_pISessionImpl = (ISessionImpl*)m_pSession;

		IUString tempErrMsg;
		if( m_pSessExtn->getAttribute(WRT_PROP_TABLENAME,tempTgtTable) == IFAILURE)
		{
			m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,tempErrMsg);
			return IFAILURE;
		}
		if(!tempTgtTable.isEmpty())
		{
			IUString tempTgtTableExpanded = "";
			if(m_pUtils->expandString(tempTgtTable,tempTgtTableExpanded,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
			{
				tempErrMsg.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),tempTgtTable.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempErrMsg).getBuffer());
				return IFAILURE;
			}
			tempErrMsg = "";
			if(m_pUtils->expandString(tempTgtTableExpanded,m_tgtTable,IUtilsServer::EVarParamMapping,IUtilsServer::EVarExpandDefault)==IFAILURE)
			{
				tempErrMsg.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),tempTgtTableExpanded.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempErrMsg).getBuffer());
				return IFAILURE;
			}
		}

		//BEGIN New Feature for Table name and prefix override Ayan
		//BEGIN Changes for CR: INFA387873 Ayan
		IUString temp_TgtSchema, TgtSchema;
		tempErrMsg = PCN_Constants::PCN_EMPTY;
		if( m_pSessExtn->getAttribute(WRT_PROP_TABLEPREFIX,temp_TgtSchema) == IFAILURE)
		{
			m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,tempErrMsg);
			return IFAILURE;
		}
		if(!temp_TgtSchema.isEmpty())
		{
			IUString tempTgtSchemaExpanded = "";
			if(m_pUtils->expandString(temp_TgtSchema,tempTgtSchemaExpanded,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
			{
				tempErrMsg.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),temp_TgtSchema.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempErrMsg).getBuffer());
				return IFAILURE;
			}
			tempErrMsg = "";
			if(m_pUtils->expandString(tempTgtSchemaExpanded,TgtSchema,IUtilsServer::EVarParamMapping,IUtilsServer::EVarExpandDefault)==IFAILURE)
			{
				tempErrMsg.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),tempTgtSchemaExpanded.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempErrMsg).getBuffer());
				return IFAILURE;
			}
		}
		//END Changes for CR: INFA387873 Ayan
		m_OriginalTableName = m_tgtTable;
		if (!TgtSchema.isEmpty())
		{
			IUString TableName(m_tgtTable);
			m_tgtTable.makeEmpty();
			m_tgtTable = TgtSchema;
			m_tgtTable.append(IUString("\".\""));
			m_tgtTable.append(TableName);
			m_OriginalSchemaName = TgtSchema;
		}
		setlocale (LC_ALL, "");
		tempVariableChar.sprint(this->m_msgCatalog->getMessage(MSG_WRT_INIT_TRG_GROUP_PART),m_nTgtNum,m_OriginalTableName.getBuffer(),m_nGrpNum,m_nPartNum);
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,tempVariableChar.getBuffer());

		//END New Feature for Table name and prefix override Ayan

		IWrtPartitionInit::EStatType statType = IWrtPartitionInit::EInsert;
		m_pInsInfo = pInit->getRowsInfo(statType);

		//Get no of group and field list
		IINT32 groupcount = m_pTargetInstance->getTable()->getGroupCount();
		IINT32 numTotalCols;

		//Plugin only support single group
		if(groupcount == 0)
		{
			IVector<const IField*>		fieldList;
			m_pTargetInstance->getTable()->getInputFieldList(fieldList);
			numTotalCols = fieldList.length();
			bIsColProjected=new IBOOLEAN[numTotalCols];
			for(IINT32 i=0;i<numTotalCols;i++)
			{
				const IField *fld=fieldList.at(i);
				const ILink *link=m_pTargetInstance->getInputLink(fld);
				if(link!=NULL)
				{
					m_inputFieldList.insert(fld);
					bIsColProjected[i]=ITRUE;
				}
				else
					bIsColProjected[i]=IFALSE;
			}
		}
		else
		{
			tempMsgCode = MSG_WRT_MORE_THN_GROUP;
			throw tempMsgCode;
		}
		IUINT32 l_numProjectedCols=m_inputFieldList.length();

		setConnectionDetails(m_pSessExtn, m_nPartNum, m_pUtils);
		m_pcnWriterConnection=(PCNWriter_Connection *)pInit->getConnection();

		if(openDatabase() == IFAILURE)
			throw IFAILURE;
		if(createLocale()==IFAILURE)
			throw IFAILURE;


		myCollInfo = new colInfo[l_numProjectedCols];
		cdatatypes = new cdatatype[l_numProjectedCols];
		sqldatatypes = new sqlDataTypes[l_numProjectedCols];
		bLoadPrecScale=new IBOOLEAN[l_numProjectedCols];
		datefmts = new datefmt[l_numProjectedCols];

		ITargetField* l_Fld = NULL;
		size_t i = 0;

		//to get the datatypes from Netezza database for target table
		/* Declare buffers for bytes available to return */
		SQLRETURN  rc;
		/*We didnt define boolean datatype in to the plugin xml file, butdid define bool(alias of boolean), so any mapping which had boolean,
		got imported as varchar(1). Since we are now getting metadata from the designer, so in case of boolean datatype, Writen doesnt come to know
		about boolean data type. This is a behavior change from the connector which used to take metadata from backend, so if we have a column as
		varchar(1) we are reimporting metadata from the beackend in order to confirm whether it was a boolean column or varchar(1).
		*/

		/*
		We are seeing a lot of behavior changes because of improting metadata from the Designer, so we are always getting metadata from the backend.
		*/		

		// Get data, lengths and indicator values
		for(i=0; i<l_numProjectedCols; i++)
		{
			//get only those flds which are linked
			//set the indicator and the data vector to NULL
			//for the columns not linked

			ITargetField* l_Fld = (ITargetField*)m_inputFieldList.at(i);
			IUString colName = l_Fld->getName();

			IBOOLEAN bKey;
			bKey = l_Fld->isPrimaryKey();
			if(bKey == ITRUE)
			{
				m_keyFldNames.insert(colName);				
			}
			else
			{
				m_nonKeyFldNames.insert(colName);
			}
			m_allLinkedFldNames.insert(colName);
			
			//get the c-type datatype
			m_inputFieldList.at(i)->getCDataType(cdatatypes[i].dataType, cdatatypes[i].prec, cdatatypes[i].scale);						
		}

		//importing metadata from backend..this is similar to old method used in 851.		
		//to get the datatypes from Netezza database for target table
		SQLLEN	cbTypeName,cbColumnSize,cbDecimalDigits,cbNullable,cbColName;
		SQLCHAR     szTypeName[STR_LEN];	
		SQLCHAR     szColName[STR_LEN];
		SQLSMALLINT Nullable;
		SQLINTEGER  ColumnSize;
		SQLSMALLINT DecimalDigits;

		size_t index=0;
		
		rc = SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc,&m_hstmtDatatype);
		
		//BEGIN New Feature for Table name and prefix override Ayan
		tempSQLTChar = Utils::IUStringToSQLTCHAR(m_OriginalTableName);
		if (!m_OriginalSchemaName.isEmpty())
		{
			SQLTCHAR * charSchemaName;
			charSchemaName = Utils::IUStringToSQLTCHAR(m_OriginalSchemaName);
			rc =SQLColumns(	m_hstmtDatatype,
				NULL, 0,              /* All catalogs */
				charSchemaName, SQL_NTS,              /* Only specific Schema    */
				tempSQLTChar, SQL_NTS, /* CUSTOMERS table */
				NULL, 0);             /* All columns    */
			DELETEPTRARR <SQLTCHAR>(charSchemaName);
		}
		else
		{
			rc =SQLColumns(	m_hstmtDatatype,
				NULL, 0,              /* All catalogs */
				NULL, 0,              /* All Schema    */
				tempSQLTChar, SQL_NTS, /* CUSTOMERS table */
				NULL, 0);             /* All columns    */
		}
		//END New Feature for Table name and prefix override Ayan
		DELETEPTRARR <SQLTCHAR>(tempSQLTChar);

		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			throw IFAILURE;
		}

		/* Bind column for fetching SQL data type,precision, scale and Not null from the backend */
		SQLBindCol(m_hstmtDatatype, 6, SQL_C_CHAR, szTypeName, STR_LEN, &cbTypeName);
		SQLBindCol(m_hstmtDatatype, 7, SQL_C_SLONG, &ColumnSize, 0, &cbColumnSize);  
		SQLBindCol(m_hstmtDatatype, 9, SQL_C_SSHORT, &DecimalDigits, 0, &cbDecimalDigits);		
		SQLBindCol(m_hstmtDatatype, 11,SQL_C_SHORT, &Nullable, 0, &cbNullable);
		SQLBindCol(m_hstmtDatatype, 4, SQL_C_CHAR, szColName, STR_LEN, &cbColName);

		//IINT32 indexCol=0;
		while(TRUE)
		{
			rc = SQLFetch(m_hstmtDatatype);
			if (rc == SQL_ERROR )
			{
				throw IFAILURE;
			}
			else if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
			{
				ICHAR* colNameFromResultSet = (ICHAR*)szColName;
				IUString iursColName = IUString(colNameFromResultSet);
				IUString iuColName = m_allLinkedFldNames.at(index);
				//restoring data type in the varialbe.
				if(iuColName.compare(iursColName) == 0)
				{
					sqldatatypes[index].dataType=(ICHAR*)szTypeName;
					sqldatatypes[index].dataType=sqldatatypes[index].dataType.makeLower();

					sqldatatypes[index].prec=ColumnSize;
					sqldatatypes[index].scale=DecimalDigits;					

					if(Nullable==SQL_NO_NULLS)
						sqldatatypes[index].isNUllable=IFALSE;
					else
						sqldatatypes[index].isNUllable=ITRUE;

					//in case fo double we are setting flag corresponding to whether we want to load pre or scale.
					if(cdatatypes[index].dataType==eCTYPE_DOUBLE)
					{
						if(sqldatatypes[index].dataType == PCN_NUMERIC || sqldatatypes[index].dataType == PCN_DECIMAL ||
							sqldatatypes[index].dataType == PCN_BIGINT || sqldatatypes[index].dataType == PCN_INTEGER ||
							sqldatatypes[index].dataType == PCN_SMALL_INT || sqldatatypes[index].dataType == PCN_BYTE_INT)
						{
							bLoadPrecScale[index]=ITRUE;
						}
						else
							bLoadPrecScale[index]=IFALSE;
					}

					/*
					In case integration service in running in unicode mode, storing information whether the corresponding column is nchar/nvarchar or
					char/varchar.
					*/
					if(cdatatypes[index].dataType==eCTYPE_UNICHAR || cdatatypes[index].dataType==eCTYPE_CHAR)
					{
						if(sqldatatypes[index].dataType == PCN_VARCHAR || 
							sqldatatypes[index].dataType == PCN_CHAR)
						{
							sqldatatypes[index].isColNchar=IFALSE;
						}
						else
						{
							sqldatatypes[index].isColNchar=ITRUE;
						}
					}

					//loading data format for corresponding date time.
					if(PCN_DATE==sqldatatypes[index].dataType)
						datefmts[index].fmt = m_pDateUtils->createFormat(DATE_FMT_YMD);
					else if(PCN_TIME==sqldatatypes[index].dataType)
						datefmts[index].fmt = m_pDateUtils->createFormat(DATE_FMT_HMS);
					else if(PCN_TIMESTAMP== sqldatatypes[index].dataType)
						datefmts[index].fmt = m_pDateUtils->createFormat(DATE_FMT_YMD_HMS);
					else
						datefmts[index].fmt= NULL;

					index++;
				}
			}
			else
				break;
		}				

		if(IFAILURE==getSessionExtensionAttributes())
		{
			throw IFAILURE;
		}


		m_b81103Delimiter=PM_FALSE;
		GetConfigBoolValue(DELIMITER_FLAG,m_b81103Delimiter);

		ISTATUS retDilm;
		IUINT32 lenDelim;
		/*
		Netezza odbc driver (after 4.x) expects client to provide delimiter as byte value rather a symbol for extended 
		ascii characters. While implementing support for passing byte value of delimiter, Latin9 code page was used to 
		convert a symbol to byte value. The reason to opt this code page was that the same code page was used while 
		passing the data (char/varchar columns) as byte stream to Netezza server. 
		In the 81103 we never had any support for Unicode mode so the delimiter was passed to Netezza server as 
		plain byte value. Now the 81103 sessions which were using delimiters as those characters which are extended 
		ascii characters as per the Latin1 code page but not in the extended ascii character set of Latin9, 
		faced the issue after upgrade. The obvious workaround for these cases was to provide delimiter as a 
		byte value only rather a symbol.
		To provide the seamless upgrade from 81103 we have implemented an undocumented flag (delimiter81103Functionality) 
		which if set at integration level, PWX for Netezza will use Latin1 code page to covert the delimiter symbol 
		to a byte value.		
		*/
		
		if(m_b81103Delimiter)
			retDilm=Utils::_ConvertToMultiByte(m_pLocale_Latin1, m_delimiter.getBuffer(), &m_cdelimiter, &lenDelim, ITRUE);
		else
            retDilm=Utils::_ConvertToMultiByte(m_pLocale_Latin9, m_delimiter.getBuffer(), &m_cdelimiter, &lenDelim, ITRUE);
		
		if(m_delimiter.getLength()==1)
		{
			m_iDelimiter=(unsigned char)m_cdelimiter[0];			
		}
		else if(m_delimiter.getLength()==3 && isdigit(m_cdelimiter[0]) && isdigit(m_cdelimiter[1]) && isdigit(m_cdelimiter[2]))
		{
			m_iDelimiter=atoi(m_cdelimiter);
		}
		else
		{
			IUString tempVariableChar;
			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_INVALID_DELIM),m_delimiter.getBuffer());				
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,tempVariableChar);

			throw IFAILURE;
		}		
		m_delimiter.sprint("%d",m_iDelimiter);
		DELETEPTRARR <ICHAR>(m_cdelimiter);	
		/*
		Since we need only one byte value for the delimiter, we are allocating only 2 byte space, second one for null value.
		*/
		if((m_cdelimiter=new ICHAR[2])==NULL)
		{
			throw IFAILURE;
		}
		memset(m_cdelimiter,0,2);
		sprintf(m_cdelimiter,"%c",m_iDelimiter);		

		//Fix for bug 1823
		if(m_nullValue.getLength() > 1)
		{
			m_nullValue = m_nullValue.mid(0,1);
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgWarning,SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_NULL_VAL_MAX_ONE_CHAR));
		}
		//end fix
		m_cnullVal  = Utils::IUStringToChar(m_nullValue);

		//Fix for bug 1822
		if( m_escapeChar == IUString(m_cdelimiter))
		{
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
			{
				IUString tempVariableChar;
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_DELIMIT_ESCP_CHAR_CANT_SAME),IUString("\\").getBuffer(),IUString("|").getBuffer());
				m_sLogger->logMsg(ILog::EMsgWarning,SLogger::ETrcLvlTerse,tempVariableChar);
			}

			m_escapeChar = IUString("\\");
			DELETEPTRARR <ICHAR>(m_cdelimiter);
			m_cdelimiter = Utils::IUStringToChar(PCN_Constants::PCN_PIPE);
			m_iDelimiter=(unsigned char)m_cdelimiter[0];
			m_delimiter.sprint("%d",m_iDelimiter);
		}
		//end fix

		if(m_quoted == PCN_DOUBLE)
			m_quotedValue = PCN_Constants::PCN_DOUBLE_QUOAT;
		else if(m_quoted == PCN_SINGLE)
			m_quotedValue = PCN_Constants::PCN_SINGLE_QUOAT;

		m_cquotedValue = Utils::IUStringToChar(m_quotedValue);

		printSessionAttributes();

		//Filling the distribution key field
		SQLCHAR		szdistCol[150];
		SQLLEN  cbdist;
		// Variables used for debug meassages from ODBC
		SQLWCHAR	sqlState[1024];
		SQLWCHAR    msgeTxt[1024];
		SQLINTEGER  ntveErr;
		SQLSMALLINT txtLngth;
		SQLRETURN   rc2;
		IINT32      iStat;
		SQLHSTMT    hstmt ;

		IUString distributionKey = IUString("SELECT ATTNAME FROM _v_table_dist_map WHERE TABLENAME='");
		distributionKey.append(IUString( m_tgtTable));
		distributionKey.append(IUString( "'"));
		rc = SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc,&hstmt);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			tempMsgCode = MSG_WRT_ALLOC_STMT_HANDLE_FAIL;
			throw tempMsgCode;
		}

		tempSQLTChar = Utils::IUStringToSQLTCHAR(distributionKey);

		rc = SQLExecDirect(hstmt,tempSQLTChar, SQL_NTS);

		DELETEPTRARR <SQLTCHAR>(tempSQLTChar);

		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			//for getting the error messages from odbc
			// Get the status records.
			iStat = 1;
			IUString tt;
			while (rc2 = SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt,iStat,(SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth))
			{
				//EBF4308: the above call does not return SQL_NO_DATA but only SQL_ERROR after the error is read once
				if(rc2 != SQL_SUCCESS && rc2 != SQL_SUCCESS_WITH_INFO)
					break;

				if(txtLngth != 0)
				{
					tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_MSG),IUString(msgeTxt).getBuffer());
					if(this->traceLevel >= SLogger::ETrcLvlTerse)
						m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
				}

				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(sqlState));

				iStat++;
			}

			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_EXEC_SQL_STMT_ERR),distributionKey.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

			throw IFAILURE;
		}

		SQLBindCol(hstmt, 1, SQL_C_CHAR, szdistCol, 150, &cbdist);

		while(TRUE)
		{
			rc = SQLFetch(hstmt);
			if(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
				m_distributionKeyFlds.insert(IUString((ICHAR*)szdistCol));
			else
				break;
		}

		rc = SQLFreeStmt (hstmt, SQL_UNBIND);
		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
           tempMsgCode = MSG_WRT_SQL_FREE_STMT_ERR;
           throw tempMsgCode;
		}

		rc = SQLFreeStmt (hstmt, SQL_CLOSE);
		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
           tempMsgCode = MSG_WRT_SQL_FREE_STMT_ERR;
           throw tempMsgCode;
		}

		//if Truncate table is set then truncate the table before loading
		if(this->getParentGroupDriver()->getTruncateTable())
		{
			if(truncateTable() == IFAILURE)
			{
				freeHandle();
				throw IFAILURE;
			}
			//We hyave already truncated the table, so rest partition will not truncate the table.
			this->getParentGroupDriver()->setIfTruncateTable(IFALSE);
		}

		//Setting the Variable if Insert and Delete is set
		IUString l_InsertAndDelete;
		if ( m_IsInsert && m_IsDelete)
		{
			l_InsertAndDelete = m_msgCatalog->getMessage(MSG_WRT_INSERT_AND_DELETE);
		}
		else if (m_IsInsert)
		{
			l_InsertAndDelete = m_msgCatalog->getMessage(MSG_WRT_PCN_INSERT);
		}
		else if (m_IsDelete )
		{
			l_InsertAndDelete = m_msgCatalog->getMessage(MSG_WRT_PCN_DELETE);
		}

		if ( m_IsInsert == 0 && m_IsDelete == 0 && m_IsUpdate == NONE)
		{
			m_NoSelection =1;

			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_NO_OPRN_SELECT));

			throw ISUCCESS;
		}

		((IUtilsServer*)m_pUtils)->getIntProperty(IUtilsServer::ETreatSourceRowsAs, m_rowsAs);

		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_TREAT_SRC_ROW_ID),m_rowsAs);
		if(this->traceLevel >= SLogger::ETrcLvlVerboseInit)
			m_sLogger->logMsg(ILog::EMsgDebug, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

		if(this->m_rowsAs == 0)
		{
			m_IsDelete = 0;
			m_IsUpdate = 0;
		}
		else if(this->m_rowsAs == 1)
		{
			m_IsInsert = 0;
			m_IsUpdate = 0;
		}
		else if(this->m_rowsAs == 2)
		{
			m_IsInsert = 0;
			m_IsDelete = 0;
		}

		if ( m_IsInsert == 0 && m_IsDelete == 0 && m_IsUpdate == NONE)
		{
			m_NoSelection =1;
			if (m_rowsAs ==0 && m_IsInsert == 0 )
			{
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,	m_msgCatalog->getMessage(MSG_WRT_INSRT_ROWID_NETZ_SESS_NO_OPRATION));
			}
			if (m_rowsAs ==1 && m_IsDelete == 0 )
			{
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_DEL_ROWID_NETZ_SESS_NO_OPRATION));
			}
			if (m_rowsAs ==2 && m_IsUpdate == NONE)
			{
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_UPDT_ROWID_NETZ_SESS_NO_OPRATION));
			}
			throw ISUCCESS;
		}

		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_OPRN_SEL_UPD),l_InsertAndDelete.getBuffer(),l_updStrat.getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

		this->ParentID=getpid();

		if(autoCommitOFF() == IFAILURE)
		{
			throw IFAILURE;
		}
		

		m_b81103Upsert=PM_FALSE;
		m_b81103UpsertWithIgnoreKey=PM_FALSE;
		GetConfigBoolValue(UPSERT_FLAG,m_b81103Upsert);

		GetConfigBoolValue(UPSERT_WITH_IGNORE_KEY_FLAG, m_b81103UpsertWithIgnoreKey);
		GetConfigBoolValue(TREAT_NODATA_EMPTY,m_bTreatNoDataAsEmpty);
		GetConfigBoolValue(SYNC_NETEZZA_NULL_WITH_PCNULL,m_bSyncNetezzaNullWithPCNull);

		char* envPathCCIHOME = getenv("CCI_HOME");
		char* cfgFilePath;
		//platform specific handling for opening pmrdtm.cfg file.
		#if defined WIN32
		cfgFilePath = "\\..\\bin\\rdtm\\pmrdtm.cfg";
		#else
		cfgFilePath = "/../bin/rdtm/pmrdtm.cfg";
		#endif
		//final reuslt should be finacfgFilePath = envPathCCIHOME + cfgFilePath;
		char* finacfgFilePath = strcat (envPathCCIHOME,cfgFilePath);
		
		std::ifstream infile(finacfgFilePath);
		string linebuffer;
		string str_sync = "syncNetezzaNullWithPCNull";
		string str_treat = "treatNoDataAsEmptyString";

		while (infile && getline(infile, linebuffer))
		{
			if (linebuffer.length() == 0)continue;

			std::size_t foundSync = linebuffer.find(str_sync); 
			std::size_t foundTreat = linebuffer.find(str_treat); 
			
			if (foundSync!=std::string::npos)
			{	
				std::size_t foundValue = linebuffer.find("true"); 
				if (foundValue!=std::string::npos)
					m_bSyncNetezzaNullWithPCNull = 1;
			}
			if (foundTreat!=std::string::npos)
			{	
				std::size_t foundValue = linebuffer.find("true"); 
				if (foundValue!=std::string::npos)
					m_bTreatNoDataAsEmpty = 1;
			}

		}//end while
		infile.close();
		
		//Creating the External Table parameter i.e setting m_strColInfoSameAs & m_strTgtTblSchema
		if ( CreateExtTabCol() == IFAILURE)
			throw IFAILURE;

		if(m_bSyncNetezzaNullWithPCNull)
		{
			if(m_bTreatNoDataAsEmpty)
			{
				if(this->m_nullValue.isEmpty())
				{
					m_bNULLCase = ITRUE;
				}
			}
		}

		calMaxRowLength();

		if ( createPipe() ==  IFAILURE)
			throw IFAILURE;


		if(CreateExternalTable()== IFAILURE)
			throw IFAILURE;

		//EBF11378
		bool isUnicodeMode = false;
		IUINT32 maxUnicharPrecision = 0;
		for(IUINT32 colNum=0; colNum<m_inputFieldList.length(); colNum++)
		{
			if(cdatatypes[colNum].dataType==eCTYPE_UNICHAR)
			{
				isUnicodeMode = true;
				if(sqldatatypes[colNum].prec > maxUnicharPrecision)
					maxUnicharPrecision = sqldatatypes[colNum].prec;
			}
		}

		if(isUnicodeMode)
		{
			if(m_InsertEscapeCHAR)
				maxUnicharPrecision *= 2;
			IINT32 nMaxMBCSBytesPerChar= m_pLocale_UTF8->getMaxMBCSBytesPerChar();
			IUINT32 maxUnicharSpace = maxUnicharPrecision * nMaxMBCSBytesPerChar;
			if((unicharData = (ICHAR *)malloc(maxUnicharSpace)) == NULL) 
			{
				throw IFAILURE;
			}
		}


		/*
		In the song mode, metadata does not not provide the 
		information about number of partitions participating in the session.
		So counting here.
		*/
		this->getParentGroupDriver()->incrementPartitionsInitializedCount();
		
		/* Creates a joinable thread, if bIsDetached is not specified or
		  IFALSE. Handle is the returned thread handle.
		*/

		m_iThreadMgr.spawn(&RunThreadQuery,m_hThread,m_threadArgs,IFALSE);
		m_bIsChildThreadSpawned=ITRUE;

		#ifdef WIN32
			//Parent thread is waiting till child thread is connected
				ConnectNamedPipe(m_hPipe, NULL);
		#endif

		if(m_pUtils->hasDTMRequestedStop()==ITRUE  && m_dtmStopHandled==IFALSE)
		{
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_INIT_DTM_STOP_REQST_KILL_CHILD));				
			m_dtmStopHandled=ITRUE;
			throw IFAILURE;
		}
		
		//Add this partition in the list, maintained to get all the partitions for corresponding connection.
		m_pcnWriterConnection->addPartitionObject(this);

	}
	catch(IINT32 ErrMsgCode)
    {
        if(this->traceLevel >= SLogger::ETrcLvlTerse)
		{
            m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,
            m_msgCatalog->getMessage(ErrMsgCode));
        }
        iResult = IFAILURE;
    }
	catch(ISTATUS iStatus)
	{
		iResult = iStatus;
	}
    catch (...)
    {
        iResult = IFAILURE;
    }

	return iResult;
}

/*******************************************************************
* Name        : execute
* Description : Controls the loading of data into Netezza
* Returns     : status of loading of the data
*******************************************************************/

PWrtPartitionDriver::EExecuteStatus PCNWriter_Partition::execute(
	const IWrtBuffer* dbuff,
	IWrtPartitionExecIn* input,
	IWrtPartitionExecOut* output)
{
	PWrtPartitionDriver::EExecuteStatus status = ESuccess;

	if (m_NoSelection)
		return status;

	if (this->handlingHasDTMStop() ==IFAILURE)
		return EFailure;

	if(m_pipeCreation == 0)
	{
		//this flag is set to check if the source file has data
		//if it has then the flag is set to 1 else it is 0
		m_no_data_in_file = 1;

		//open the pipe in write mode
		if(openWRPipe() == IFAILURE)
				return EFailure;
		//Fix for CR177639
	}
	//flg to count number of times execute is called
	m_flgLessData++;

 	status = loadDataIntoPipe(dbuff, input, output);

	return status;
}

/*******************************************************************
* Name        : Deinit
* Description : Deinit the Partition
* Returns     : Deinit status
*******************************************************************/

ISTATUS PCNWriter_Partition::deinit(ISTATUS retStat)
{		
	ISTATUS iResult = ISUCCESS;
	IUString part;
	IUString tgt;
	IUString rows;
	// Fix for CR # 174877
	IUINT32 applied=0 , rejected=0,requested=0,affected=0;

	if(retStat == ISUCCESS)
	{
		//We need to make sure all the partitions commit is done only then start processing deinit of the partition.
		//If commitFromDeinit is failed that means, commit has not been called=> we need to make sure we dont load any data
		//and drop temp/external table.
		m_pcnWriterConnection->lockCommitMutex();
		if(IFALSE==m_pcnWriterConnection->isCommitCalled())
		{
			if(IFAILURE==m_pcnWriterConnection->commitFromDeinit())
			{
				m_pcnWriterConnection->setCommitRetStatus(IFAILURE);
				retStat=IFAILURE;
			}
		}
		else
		{
			if(IFAILURE==m_pcnWriterConnection->getCommitRetStatus())
			{
				retStat=IFAILURE;
			}
		}
		m_pcnWriterConnection->unlockCommitMutex();
	}

	if (retStat != ISUCCESS || this->handlingHasDTMStop() ==IFAILURE)
	{
		requested = m_rowsMarkedAsInsert + m_rowsMarkedAsUpdate + m_rowsMarkedAsDelete ;
		if(m_pInsInfo)
			m_pInsInfo->nReq=requested;

		//In case of erros, we need to make sure whether the created temp table has been dropped. 

		childThreadHandling();
		manageDropExtTempTable();
		return IFAILURE;
	}	
	
	part.sprint(IUString("%d"),m_nPartNum);
	tgt.sprint(IUString("%d"),m_nTgtNum);

	tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_DEINIT_TRG_PART),tgt.getBuffer(),part.getBuffer());
	if(this->traceLevel >= SLogger::ETrcLvlNormal)
		m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

	//common for both Windows and UNIX
	//if reject is selected as update strategy then simply return
	//remove the pipe before returning

	if(m_NoSelection == 1)
	{
		return ISUCCESS;
	}

	if ( m_IsStaged == 0)  //Staging is Not set
	{
		//Logging loading related messages in to the session log, this will be done only for those thread which executes query on to the target.
		if(m_threadArgs->b_isLoadInToTgt)
		{
			rows.sprint(IUString("%u"),m_rowsMarkedAsInsert);
			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_NUM_ROWS_MARKED_INSRT),rows.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

			if( ! m_IsInsert && m_rowsMarkedAsInsert > 0 )
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_NUM_ROWS_INSRT_REJCT),rows.getBuffer(),rows.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
			rows.sprint(IUString("%u"),m_rowsMarkedAsDelete);

			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_NUM_ROWS_DEL),rows.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			if( ! m_IsDelete && m_rowsMarkedAsDelete > 0 )
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_NUM_ROWS_DEL_REJCT),rows.getBuffer(),rows.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
			rows.sprint(IUString("%u"),m_rowsMarkedAsUpdate);

			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_NUM_ROWS_UPDT),rows.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

			if( ! m_IsUpdate  && m_rowsMarkedAsUpdate > 0 )
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_NUM_ROWS_UPDT_REJCT),rows.getBuffer(),rows.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
		}



		IBOOLEAN 	bCheckAnyfailedQuery=m_threadArgs->bCheckAnyfailedQuery;

		if(retStat == ISUCCESS)
		{
			if (bCheckAnyfailedQuery ==IFALSE)//If all query succeed then Commit the trasaction
			{
				requested = m_rowsMarkedAsInsert + m_rowsMarkedAsUpdate + m_rowsMarkedAsDelete ;
				this->getParentGroupDriver()->incrementTotalReqRows(requested);

				if(this->handlingHasDTMStop() ==IFAILURE)
				{
					return IFAILURE;
				}

				//Commit will be executed for those partitions which executes query on the target.
				if(m_threadArgs->b_isLoadInToTgt)
				{

					affected = m_threadArgs->rowsInserted + m_threadArgs->rowsDeleted + m_threadArgs->rowsUpdated ;

					applied=affected-m_threadArgs->affMinusAppCount;
					/*
					In case data is loaded in to the target table from the temp table, only the last parition will
					execute query on to the target, so rest all partition will not have any statistic. Only the 
					last partition will show all the requested rows. 
					In this case user might think that we are not utilizing other partitions, we also log temp load 
					summary for each partition.This will show the number of rows logged in to the temp table by 
					each partition. 

					In case data is loaded from external table, all the partitions will have statistics, so all will
					have their own requested rows. 
					*/
					m_pInsInfo->nAff = affected;
					m_pInsInfo->nApp=applied;
					
					if(m_threadArgs->loadTempTableQuery.isEmpty())
					{
						rejected = requested -  applied;
						m_pInsInfo->nReq=requested;
					}
					else
					{
						rejected = this->getParentGroupDriver()->getTotalReqRows() -  applied;
						m_pInsInfo->nReq=this->getParentGroupDriver()->getTotalReqRows();
					}
					m_pInsInfo->nRej=rejected;
				}

				//Loggin temp load summary for each partition.
				if(!m_threadArgs->loadTempTableQuery.isEmpty())
				{
					tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_NETZ_LOAD_SUMM_TEMP),m_tempTblName.getBuffer(),m_nPartNum+1, m_tgtInstanceName.getBuffer(),requested);
					if(this->traceLevel >= SLogger::ETrcLvlNormal)
						m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
				}

			}
		}

		if (bCheckAnyfailedQuery)//some query failed
		{
			requested = m_rowsMarkedAsInsert + m_rowsMarkedAsUpdate + m_rowsMarkedAsDelete ;
			m_pInsInfo->nReq=requested;

			return IFAILURE;
		}
	}//If non staging(pipe) check ends here		
	return iResult;
}

/********************************************************************
*Function       : closeWRPipe
*Description    : closes the pipe after writing the data
*Input          : None
********************************************************************/

ISTATUS PCNWriter_Partition::closeWRPipe()
{
	ISTATUS iResult = ISUCCESS;
	if(m_fHandleUpdateDeleteInsert != NULL)
	{
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_CLOSING_PIPE),m_fullPath.getBuffer());

		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

#ifndef UNIX
		if(FlushFileBuffers(m_hPipe) == 0)
		{				
			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_FLUSH_DATA_PIPE_ERR),m_fullPath.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		}
#endif

		// closing pipe handle
		if(fclose(m_fHandleUpdateDeleteInsert)==0)
		{				
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_CMN_CLOSE_PIPE));
		}
		else
		{
			//Error closing pipe
			iResult = IFAILURE;
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_CMN_CLOSE_PIPE_FAIL));
		}
	}
	if(iResult==ISUCCESS)
		m_bIsPipeClosed=ITRUE;
	return iResult;
}

/********************************************************************
*Function       : removePipe
*Description    : remove the pipe after child thread is finished
*Input          : None
********************************************************************/
inline ISTATUS PCNWriter_Partition::removePipe()
{
	ICHAR *temp = Utils::IUStringToChar(m_fullPath) ;
	ISTATUS iResult=ISUCCESS;
	if(remove(temp) == 0)
	{
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_REM_PIPE_SUCC),m_fullPath.getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
	}
	else
	{
		iResult=IFAILURE;
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_REM_PIPE_FAIL),m_fullPath.getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlTerse)
			m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
	}	
	DELETEPTRARR <ICHAR>(temp);
	return iResult;
}

ISTATUS PCNWriter_Partition::setConnectionDetails(const ISessionExtension* m_pSessExtn,IUINT32 m_partNum, const IUtilsServer* pUtils)
{
    //Getting the connection information
    IVector<const IConnectionRef*> vecConnRef; 
	IUString attrVal;
    if(m_pSessExtn->getConnections(vecConnRef,m_partNum) == IFAILURE)
    {
        if(this->traceLevel >= SLogger::ETrcLvlTerse)
            m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_CONN_INFO_FAIL));
    }
    const IConnectionRef* pConnRef = vecConnRef.at(0);
    const IConnection*    pConn = NULL;
    if((pConn = (vecConnRef.at(0))->getConnection()) == NULL)
    {
        if(this->traceLevel >= SLogger::ETrcLvlTerse)
            m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_CONN_INFO_FAIL));
		return IFAILURE;
    }   
	else
	{
		//Get connection extensions(user name and password)
		m_uid     = pConn->getUserName();
		m_pwd = pConn->getPassword(); 
		
		extractConnectionInformation(PLG_CONN_SCHEMANAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_schemaname = attrVal;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_DATABASENAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_databasename = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_SERVERNAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_servername = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_PORT, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_port = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_DRIVERNAME, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_drivername = attrVal;
		}
		else
		{
			return IFAILURE;
		}
		attrVal.makeEmpty();
		extractConnectionInformation(PLG_CONN_ADDITIONAL_CONFIGS, attrVal, pConn, pUtils);
		if(!attrVal.isEmpty())
		{
			m_connectStrAdditionalConfigurations = attrVal;
		}
	}
	return ISUCCESS;
	vecConnRef.clear(); 
}

void PCNWriter_Partition::extractConnectionInformation(const ICHAR* attname, IUString& val,const IConnection* pConn, const IUtilsServer* pUtils)
{
	IUString printMsg=PCN_Constants::PCN_EMPTY;
	ICHAR* printAttrName = new ICHAR[1024];
	for ( int i = 0; i < strlen(attname); i++ )
		printAttrName[i] = attname[i];
	printAttrName[strlen(attname)] = '\0';
	if(IFAILURE == pConn->getAttribute(IUString(attname), val))
	{		
		printMsg.sprint(m_msgCatalog->getMessage(MSG_WRT_ERROR_READING_CONNATTR),printAttrName);
		m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,printMsg);
	}
	if(IFAILURE == pUtils->expandString(val, val, IUtilsServer::EVarParamSession, IUtilsServer::EVarExpandDefault))
	{
		printMsg.sprint(m_msgCatalog->getMessage(MSG_WRT_ERROR_READING_CONNATTR),printAttrName);
		m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlNormal,printMsg);
	}
}

/********************************************************************
*Function       : openDatabase
*Description    : Allocate the environment and connection handles
and connect connect to the datasource using the
given username and password
*Input          : Pointer to the Output file where the result
is to be stored
********************************************************************/

ISTATUS PCNWriter_Partition::openDatabase()
{
	ISTATUS iResult = ISUCCESS;
	SQLRETURN		rc;
	SQLWCHAR        sqlState[1024];
	SQLWCHAR        msgeTxt[1024];
	SQLINTEGER      ntveErr;
	SQLSMALLINT     txtLngth;
	SQLTCHAR * dsnSQLTChar;
	SQLTCHAR*  uidSQLTChar;
	SQLTCHAR*  pwdSQLTChar;
	IINT32 tempMsgCode;

	//loginTimeOutValue holds custom value of login timeout set by the Administrator.
	int loginTimeOutValue = GetConfigIntValue(LOGIN_TIMEOUT_FLAG);

	try
	{
		// Connection Info DSN and User
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_CONN_INFO_DSN_USR_NAME),m_dsn.getBuffer(),m_uid.getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

		m_pcnWriterConnection->setMessageHandles(this->traceLevel, m_sLogger, m_msgCatalog);
		ISTATUS stat = m_pcnWriterConnection->allocEnvHandles(m_henv);
		if(stat==IFAILURE)
		{
			throw IFAILURE;
		}
		else
		{
			rc = SQLAllocHandle(SQL_HANDLE_DBC,m_henv, &m_hdbc);
			if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			{
				tempMsgCode = MSG_WRT_ALLOC_DATABASE_CONN_FAIL;
				throw tempMsgCode;
			}
			else
			{
				//setting LOGIN_TIMEOUT connection attribute according to the loginTimeOutValue while connecting to the database
				if(loginTimeOutValue < 0)
				{
					//If the custom variable been set to a -ve value, then leaving the LOGIN_TIMEOUT to the backend(odbc.ini). 
				}
				else if(loginTimeOutValue == 0) 
				{
					//if custom variable is not defined, then LOGIN_TIMEOUT is to be set to preserve the earlier state.
					SQLSetConnectAttr(m_hdbc, (SQLINTEGER)SQL_ATTR_LOGIN_TIMEOUT, (void*)30, 0);
				}
				else 
				{
					//Will make the LOGIN_TIMEOUT as the custom variable set by the Administrator if it is valid.
					SQLSetConnectAttr(m_hdbc, (SQLINTEGER)SQL_ATTR_LOGIN_TIMEOUT, (void*)loginTimeOutValue, 0);
				}
				
				IUString printMsg=PCN_Constants::PCN_EMPTY;
				printMsg.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_CONNECTION_INFO),m_drivername.getBuffer(),
					m_uid.getBuffer(),
					m_databasename.getBuffer(),
					m_servername.getBuffer(),
					m_port.getBuffer(),
					m_connectStrAdditionalConfigurations.getBuffer());
				m_sLogger->logMsg(ILog::EMsgInfo,SLogger::ETrcLvlNormal,printMsg);

				IUString strConnectString=PCN_Constants::PCN_EMPTY;
				strConnectString.sprint("Driver=%s;Username=%s;Password=%s;\
					Database=%s;Servername=%s;Port=%s",
					m_drivername.getBuffer(),
					m_uid.getBuffer(),
					m_pwd.getBuffer(),
					m_databasename.getBuffer(),
					m_servername.getBuffer(),
					m_port.getBuffer());

				if ( !m_schemaname.isEmpty() )
					strConnectString.append(IUString(";").getBuffer()).append(m_schemaname.getBuffer());
				if ( !m_connectStrAdditionalConfigurations.isEmpty() )
					strConnectString.append(IUString(";").getBuffer()).append(m_connectStrAdditionalConfigurations.getBuffer());

                SQLTCHAR* sqlcharConnString = Utils::IUStringToSQLTCHAR(strConnectString);
				SQLTCHAR outConnString[1024];
				SQLSMALLINT cbConnStrOut;
				rc = SQLDriverConnect(m_hdbc, NULL, sqlcharConnString, 
					strConnectString.getLength(), outConnString, sizeof(outConnString), 
					&cbConnStrOut, SQL_DRIVER_NOPROMPT);
                
                DELETEPTRARR <SQLTCHAR>(sqlcharConnString);

				if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				{
					SQLGetDiagRecW(SQL_HANDLE_DBC, m_hdbc,1, (SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth);
					if( ntveErr == 108)
					{
						IUString nativeError;
						nativeError.sprint(m_msgCatalog->getMessage(MSG_CMN_NATIVE_ERR_CODE),(int)ntveErr);
						if(this->traceLevel >= SLogger::ETrcLvlNormal)
							m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_CMN_INVLD_USRNAME_PASSWD));
					}
					else
					{
						if(txtLngth != 0)
						{
							tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_MSG),IUString(msgeTxt).getBuffer());
						}
						if(this->traceLevel >= SLogger::ETrcLvlTerse)
							m_sLogger->logMsg(ILog::EMsgError,SLogger::ETrcLvlTerse,IUString(msgeTxt));
					}
					if(this->traceLevel >= SLogger::ETrcLvlTerse)
						m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_INTE_SER_NOT_CONN_NPC));

					throw IFAILURE;
				}
				else
				{
					//connection successfully established
					if(this->traceLevel >= SLogger::ETrcLvlNormal)
						m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_CONN_SUCC_ESTABLISHED));
				}
			}
		}
	}
	catch(IINT32 ErrMsgCode)
	{
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
		{
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(ErrMsgCode));
		}
		iResult = IFAILURE;
	}
	catch (...)
	{
		iResult = IFAILURE;
	}
	return iResult;
}

/********************************************************************
*Function       : truncateTable
*Description    : truncates the table before data load
*Input          : None
********************************************************************/

ISTATUS PCNWriter_Partition::truncateTable()
{
	ISTATUS iResult = ISUCCESS;
	IUString          query;
	SQLRETURN     rc;
	SQLTCHAR*		tempSQLTChar;
	//call truncate table statement
	query = "TRUNCATE TABLE  ";
	query.append(IUString( "\""));
	query.append(IUString( m_tgtTable));
	query.append(IUString( "\""));

	try
	{
		IBOOLEAN bTemp=IFALSE;
		//execute the drop external table
		rc = SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc,&m_hstmt);
		tempSQLTChar  = Utils::IUStringToSQLTCHAR(query);
		rc = SQLExecDirect(m_hstmt,tempSQLTChar, SQL_NTS);
		DELETEPTRARR <SQLTCHAR>(tempSQLTChar);

		//BEGIN MODIFY for EBF13104 Ayan
		SQLRETURN rcCheck = rc;
		checkRet(rc,query,m_hstmt,m_msgCatalog,m_sLogger,bTemp,this->traceLevel);
		rc = SQLFreeStmt (m_hstmt, SQL_CLOSE);
		if(rcCheck != SQL_SUCCESS && rcCheck != SQL_SUCCESS_WITH_INFO)
		{
			throw IFAILURE;
		}
		//END MODIFY for EBF13104 Ayan

		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			throw IFAILURE;
		}
	}
	catch(...)
	{
		iResult = IFAILURE;
	}

	return iResult;
}

/********************************************************************
*Function       : flushPipe
*Description    : flushes the pipe in case of failure
*Input          : None
********************************************************************/
inline ISTATUS PCNWriter_Partition::flushPipe()
{
	ISTATUS iResult = ISUCCESS;
	IUString sPipe;
	//this is done to read the pipe which waiting
	//for some process to read it in case of query failure
	//which reads the pipe
	try
	{
		//Fix For CR177639
		#ifndef WIN32 // defined (UNIX)
		{
			sPipe = "cat ";
			sPipe.append(IUString( m_fullPath));
			sPipe.append(IUString( " > pipeData.dat"));
			ICHAR *temp = Utils::IUStringToChar(sPipe) ;
			system(temp);
			DELETEPTRARR <ICHAR>(temp);
			system("rm -f pipeData.dat");
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_FLUSH_PIPE));
		}
		#endif
	}
	catch(...)
	{
		iResult = IFAILURE;
	}
	return iResult;
}

/*****************************************************************
*Function       : createPipe
*Description    : creates a file for windows and pipe for
unix platforms respectively
*Input          : None
******************************************************************/

inline ISTATUS PCNWriter_Partition::createPipe()
{
	//get system date and time
	ISTATUS iResult = ISUCCESS;
	time_t curTime;
	curTime = time(NULL);
	struct tm *curTimeStruct;
	IUString pipeID;
	try
	{
		curTimeStruct = localtime(&curTime);
		// support for multiple session running simutaneously
		//BEGIN New Feature for Table name and prefix override Ayan
		pipeID.sprint(IUString("WRT%d_%d_%d_%d%d%d_%d%d%d"), getpid(), m_nTgtNum, m_nPartNum, curTimeStruct->tm_mday, curTimeStruct->tm_mon, curTimeStruct->tm_year, curTimeStruct->tm_hour, curTimeStruct->tm_min, curTimeStruct->tm_sec);
		//END New Feature for Table name and prefix override Ayan

		#ifdef WIN32 // defined(DOS) || defined(OS2) || defined(NT) || defined(WIN32) || defined (MSDOS)
		{
			m_fullPath.append(pipeID);
			LPWSTR lpszPipename = LPWSTR(m_fullPath.getBuffer());
			m_hPipe = CreateNamedPipe(	lpszPipename,             // pipe name
										PIPE_ACCESS_DUPLEX,       // read/write access
										PIPE_TYPE_MESSAGE |       // message type pipe
										PIPE_READMODE_MESSAGE |   // message-read mode
										PIPE_WAIT,                // blocking mode
										PIPE_UNLIMITED_INSTANCES, // max. instances
										BUFSIZE,                  // output buffer size
										BUFSIZE,                  // input buffer size
										5000,                     // client time-out
										NULL);
			if (m_hPipe == INVALID_HANDLE_VALUE)
			{
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_CREATE_PIPE_FAIL));
				throw IFAILURE;
			}
			else
			{
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_CREATE_PIPE_SUCC));
			}
		}
		#else //if defined(UNIX)
		{
			IUString  pipeName = IUString("/Pipe");
			pipeName.append(IUString( pipeID));
			m_fullPath.append(pipeName);
			ICHAR * temp = Utils::IUStringToChar(m_fullPath) ;
			IINT32 ret = remove(temp);
			if(ret == 0)
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_REM_PIPE_SUCC),m_fullPath.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
			else
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_PIPE_NOT_EXIST),m_fullPath.getBuffer());

				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
			//temp = Utils::IUStringToChar(m_fullPath) ;

			IINT32 fd = mknod(temp, 010777, 0);

			//Fix for CR133218
			if(fd ==-1)
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_CREATE_PIPE_FAIL),m_fullPath.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,tempVariableChar.getBuffer());
				DELETEPTRARR <ICHAR>(temp);
				throw IFAILURE;
			}
			//end fix

			DELETEPTRARR <ICHAR>(temp);
			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_CREATE_THIS_PIPE_SUCC),m_fullPath.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,tempVariableChar.getBuffer());
		}
	#endif
	}
	catch(...)
	{
		iResult = IFAILURE;
	}
	
	if(iResult==ISUCCESS)
		m_bIsPipeCreated=ITRUE;

	return iResult;
}

/*****************************************************************
* Name        : loadDataIntoPipe
* Description : Controls the loading of data into pipe.
* Returns     : status of loading of the data
*******************************************************************/

PWrtPartitionDriver::EExecuteStatus PCNWriter_Partition::loadDataIntoPipe(
	const IWrtBuffer* dbuff,
	IWrtPartitionExecIn* input,
	IWrtPartitionExecOut* output)
{
	PWrtPartitionDriver::EExecuteStatus iResult = ESuccess;
	ICHAR * l_rowType ="";
    ICHAR * tempDecimal = new ICHAR[1024]; //Memory is initially allocated for storing the decimal,
	IUINT32 l_numProjectedCols = m_inputFieldList.length();
	const IINT16 linkNULL = LINK_NULL;
	IINT32   dataCntr  = -1;
	IBOOLEAN* noOpArray = input->getNoOpArray();
	IBOOLEAN bConvErrors = input->hasConvErrors();
	IINT32 len;

	// no. of rows available to load
	IUINT32 numRowsAvailable = dbuff->getNumRowsAvailable();
	IBOOLEAN rowHasConvErrors = IFALSE;
	try
	{
		ICHAR *m_pipeBuffer=new ICHAR[m_rowMaxLength*numRowsAvailable];
		ICHAR *pipeBuffer;
		//Setting all buffer memory to NULL
		/*memset(m_pipeBuffer,0,m_rowMaxLength*numRowsAvailable);*/
		pipeBuffer=m_pipeBuffer;

		// Get data, lengths and indicator values
		for(size_t i=0; i< l_numProjectedCols; i++)
		{
			if(dbuff->getDataIndirect(i) != NULL)
			{
				//getDataIndirect column num: An index on the connected columns.
				myCollInfo[i].data = dbuff->getDataIndirect(i);
				myCollInfo[i].length = dbuff->getLength(i);
				myCollInfo[i].indicator = dbuff->getIndicator(i);
			}
			else
			{
				myCollInfo[i].indicator = &linkNULL;
			}
		}
		size_t colNum;
		size_t length;
		IWrtBuffer::ERowType rowType ;

		IBOOLEAN b_isQuoted;
			if(m_quoted == PCN_NO)
				b_isQuoted=IFALSE;
			else
				b_isQuoted=ITRUE;


		for(size_t rowNum=0; rowNum < numRowsAvailable; rowNum++)
		{
			//To get the row type of column
			rowType= dbuff->getRowType(rowNum);

			switch(rowType)
			{
			case IWrtBuffer::EInsert:
				l_rowType ="2";
				m_rowsMarkedAsInsert++;
				break;
			case IWrtBuffer::EUpdate:
				l_rowType="1";
				m_rowsMarkedAsUpdate++;
				break;
			case IWrtBuffer::EDelete:
				l_rowType ="0";
				m_rowsMarkedAsDelete++;
				break;
			default :
				continue;
			}

			// check noOpArray
			if(noOpArray[rowNum])
				continue;

			// Check for conversion errors and link
			rowHasConvErrors = IFALSE;

			// If the entire block does not have any errors, we let go
			if(bConvErrors)
			{
				// Check if every column data is valid in this row
				for(size_t colNum=0; colNum< l_numProjectedCols; colNum++)
				{
					short indicator = *(const IINT16 *)myCollInfo[colNum].indicator+rowNum;
					if (indicator == kSDKDataTooLarge)
					{
						rowHasConvErrors = ITRUE;
						break;
					}
					else if(indicator == kSDKDataValid)
					{
						// Just to check if it is branching out correctly during bedug
						continue;
					}
				}
			}

			if(rowHasConvErrors)
				continue; // Just skip the row

			// For all the columns
			for(colNum=0; colNum< l_numProjectedCols; colNum++)
			{
				m_bAddEscapeCharForNullChar = ITRUE;
				const short *indicatorArray = myCollInfo[colNum].indicator;
				if(*indicatorArray == LINK_NULL)
				{
					if(colNum < l_numProjectedCols -1)
					{
						strncpy(pipeBuffer,m_cdelimiter,1);
						pipeBuffer+=1;
					}
					continue;
				}

				short indicator = indicatorArray[rowNum];
				if(indicator == kSDKDataNULL)
				{
					//write the NULL Character
					if(m_bSyncNetezzaNullWithPCNull)
					{
						/*If this flag is not set then for all the NULL indicator data 
						we write an empty string(which is two consecutive delimiters in the pipe
						otherwise we write exact NULL character in the pipe.
					*/
						if(m_bNULLCase)
					{
							strncpy(pipeBuffer, "NULL",4);
							pipeBuffer+=4;
							//TODO: this flag can be removed, as we anyway have continue statement in this block.
							m_bAddEscapeCharForNullChar = IFALSE;

						}
						else if(!m_nullValue.isEmpty())
						{
							strncpy(pipeBuffer, m_cnullVal,1);
							pipeBuffer+=1;
							m_bAddEscapeCharForNullChar = IFALSE;
						}
					}
					if(colNum < l_numProjectedCols -1)
					{
						strncpy(pipeBuffer,m_cdelimiter,1);
						pipeBuffer+=1;
					}
					continue;
				}
				

				switch(cdatatypes[colNum].dataType)
				{
				case eINVALID_CTYPE: // invalid data type
					assert(0);

				case eCTYPE_SHORT:
					{
						if(b_isQuoted==IFALSE)
							len = sprintf(pipeBuffer, "%d", *((const IUINT16*)myCollInfo[colNum].data + rowNum));
						else
							len = sprintf(pipeBuffer, "%s%d%s", m_cquotedValue,*((const IUINT16*)myCollInfo[colNum].data + rowNum),m_cquotedValue);

						pipeBuffer+=len;
						break;
					}
				case eCTYPE_LONG:
					{			
						
						if (b_isQuoted == ITRUE){
							strncpy(pipeBuffer, m_cquotedValue, 1);
							pipeBuffer = pipeBuffer + 1;
						}

						IINT32 intColValue = *((const IINT32*)(myCollInfo[colNum].data) + rowNum);
						int len = Utils::int32ToStr(intColValue, pipeBuffer);
						pipeBuffer = pipeBuffer + len;

						if (b_isQuoted == ITRUE){
							strncpy(pipeBuffer, m_cquotedValue, 1);
							pipeBuffer = pipeBuffer + 1;
						}
						break;
					}

				case eCTYPE_INT32:
					{
						if(b_isQuoted==IFALSE)
							len = sprintf(pipeBuffer, "%d", *((const IUINT32*)(myCollInfo[colNum].data) + rowNum));
						else
							len = sprintf(pipeBuffer, "%s%d%s", m_cquotedValue,*((const IUINT32*)(myCollInfo[colNum].data) + rowNum),m_cquotedValue);

						pipeBuffer+=len;
						break;
					}

				case eCTYPE_LONG64:
					{
						if (b_isQuoted == ITRUE){
							strncpy(pipeBuffer, m_cquotedValue, 1);
							pipeBuffer = pipeBuffer + 1;
						}						
						
						IINT64 longtColValue = *((const IINT64*)(myCollInfo[colNum].data) + rowNum);
						int len = Utils::int64ToStr(longtColValue, pipeBuffer);
						pipeBuffer = pipeBuffer + len;
						
						if (b_isQuoted == ITRUE){
							strncpy(pipeBuffer, m_cquotedValue, 1);
							pipeBuffer = pipeBuffer + 1 ;
						}						
						break;
					}

				case eCTYPE_CHAR:
					{
							/*  Fix for CR # 176520
							parse the value string to see special characters.if special charaters present prefix them Escape charater
							1. The list of characters that need to be escaped are
							- NUL (0)
							- CR
							- LF
							- NULLVALUE character
							- ESCAPECHAR characte
							- DELIM character
								*/


						if (m_InsertEscapeCHAR)
						{
							const ICHAR *value = *((const ICHAR**)(myCollInfo[colNum].data)+rowNum);
							IINT32 len = strlen(value);
							//Here we are assuming that all the character in the col could be special one.
							ICHAR * temp = new ICHAR[len + sqldatatypes[colNum].prec];   
							IINT32 j=0;

							if(m_bNULLCase)
							{
								//this is the only case where data in the source can actually contain NULL string which needs to be escaped.
								if(m_bAddEscapeCharForNullChar)
								{
									//there is sufficient data to hold NULL
									if(0 == strcmp(value,"NULL"))
									{
										temp[j] = m_cescapeChar[0];
										j++;
									}
								}
							}

							for (IINT32 i =0 ; i <len ; i++)
							{
								//Fix for CR 180628
								/*
								In this case since char will be of one byte, we can compare it with char value of delimter. 
								*/
								if((IFALSE == m_bNULLCase) && (!m_nullValue.isEmpty()))
								{
									if(m_bAddEscapeCharForNullChar)
									{
										//there is sufficient data to hold NULL
										if(value[i]== m_cnullVal[0])
										{
											temp[j] = m_cescapeChar[0];
											j++;
										}
									}
								}

								if (value[i]=='\r'|| value[i]=='\n'||value[i]=='\0' ||value[i] == m_cescapeChar[0] || value[i] == m_cdelimiter[0])
								{
									temp[j] = m_cescapeChar[0];
								//end of fix for CR 180628
									j++;
								}

							temp[j] = value[i];
							j++;
							}
							temp[j] = '\0';
							len  = strlen (temp);
							if(b_isQuoted==IFALSE)
							{
								memcpy(pipeBuffer,temp,len);
								pipeBuffer+=len;
							}
							else
							{

								strncpy(pipeBuffer,m_cquotedValue,1);
								pipeBuffer+=1;
								memcpy(pipeBuffer,temp,len);
								pipeBuffer+=len;
								strncpy(pipeBuffer,m_cquotedValue,1);
								pipeBuffer+=1;
							}
							DELETEPTRARR <ICHAR>(temp);
						}
						else
						{
							IINT32 len=*((const IUINT32*)(myCollInfo[colNum].length) + rowNum);
							if(b_isQuoted==IFALSE)
							{
								memcpy(pipeBuffer,*((const ICHAR**)(myCollInfo[colNum].data)+rowNum),len);
								pipeBuffer+=len;
							}
							else
							{
								strncpy(pipeBuffer,m_cquotedValue,1);
								pipeBuffer+=1;
								memcpy(pipeBuffer,*((const ICHAR**)(myCollInfo[colNum].data)+rowNum),len);
								pipeBuffer+=len;
								strncpy(pipeBuffer,m_cquotedValue,1);
								pipeBuffer+=1;
							}
						}
						break;
					}

				case eCTYPE_TIME:
					{
						size_t sizelen = 10;
						const IDate* date = m_pDateUtils->getDateFromArray((const IDate*)(myCollInfo[colNum].data), rowNum);

						if(b_isQuoted==ITRUE)
						{
							strncpy(pipeBuffer,m_cquotedValue,1);
							pipeBuffer+=1;
						}
						ISTATUS status = m_pDateUtils->convertTo(date, pipeBuffer, datefmts[colNum].fmt, length);
						pipeBuffer+=length;

						if(b_isQuoted==ITRUE)
						{
							strncpy(pipeBuffer,m_cquotedValue,1);
							pipeBuffer+=1;
						}
 						break;
					}
				case eCTYPE_FLOAT:
					{
						if(b_isQuoted==IFALSE)
							len = sprintf(pipeBuffer, "%f", *((const ISFLOAT*)myCollInfo[colNum].data + rowNum));
						else
							len = sprintf(pipeBuffer, "%s%f%s", m_cquotedValue,*((const ISFLOAT*)myCollInfo[colNum].data + rowNum),m_cquotedValue);

						pipeBuffer+=len;
						break;
					}

				case eCTYPE_DOUBLE:
					{						
						const ISDOUBLE* pData = (const ISDOUBLE*)(myCollInfo[colNum].data);
						ISDOUBLE value = pData[rowNum];

						bool isNegative = (value > 0 ? false : true);
						
						
						if (bLoadPrecScale[colNum] == IFALSE)
						{
							if (b_isQuoted == IFALSE)
								len = sprintf(pipeBuffer, "%lf", value);
							else
								len = sprintf(pipeBuffer, "%s%lf%s", m_cquotedValue, value, m_cquotedValue);
							
							pipeBuffer = pipeBuffer + len;
							break;
						}

							else if (isNegative && value < -922337203685477580 )//limit for signed long long is -9223372036854775808 to 9223372036854775807
						{
							if(b_isQuoted==IFALSE)
								len = sprintf(pipeBuffer,"%*.*lf",cdatatypes[colNum].prec,cdatatypes[colNum].scale,value);
							else
								len = sprintf(pipeBuffer,"%s%*.*lf%s",m_cquotedValue,cdatatypes[colNum].prec,cdatatypes[colNum].scale,value,m_cquotedValue);
							
							pipeBuffer = pipeBuffer + len;
							break;
						}
						else if (!isNegative && value > 922337203685477580 )//limit for signed long long is -9223372036854775808 to 9223372036854775807
						{
							if (b_isQuoted == IFALSE)
								len = sprintf(pipeBuffer, "%*.*lf", cdatatypes[colNum].prec, cdatatypes[colNum].scale, value);
							else
								len = sprintf(pipeBuffer, "%s%*.*lf%s", m_cquotedValue, cdatatypes[colNum].prec, cdatatypes[colNum].scale, value, m_cquotedValue);

							pipeBuffer = pipeBuffer + len;
							break;
						}

						//Change for OCON-7773
						value = fabs(value);
						if (b_isQuoted == ITRUE){
							strncpy(pipeBuffer, m_cquotedValue, 1);
							pipeBuffer = pipeBuffer + 1;
						}
						int scale = cdatatypes[colNum].scale;

						//In low precision mode the maximum permited precision for double is 15
						scale = (scale <= 15 ? scale : 15);

						//Get the integral part and fractional part
						IINT64 intpart = (IINT64)value;
						ISDOUBLE fpart = value - (ISDOUBLE)intpart;

						//Round it off as per the scale provided
						ISDOUBLE exp = powersof10[scale];
						fpart = fpart + (1.0 / (exp * 2));

						//New value after the fractional part is rounded off
						//Ex:56.67 when scale is 0 will 57 (change in integral part)                                                         
						value = intpart + fpart;

						//restore fpart- cgahlot 
						IINT64 ipart = (IINT64)fpart;
						fpart = fpart - (ISDOUBLE)ipart;

						//Re fetch the integral 
						intpart = (IINT64)value;

						int len = 0;

						if (!isNegative)
						{
							len = Utils::int64ToStr(intpart, pipeBuffer);
						}
						else
						{
							pipeBuffer[0] = '-';
							len = 1 + Utils::int64ToStr(intpart, pipeBuffer + 1);
						}

						if (scale) //Only if scale > 0,convert the decimal part to string
						{
							pipeBuffer[len] = '.';
							len = len + 1;
							//Need to get the number without decimal 
							fpart = fpart * exp;
							len += Utils::int64ToStr((IINT64)fpart, pipeBuffer + len, scale);
						}
						pipeBuffer+=len;

						if (b_isQuoted == ITRUE){
							strncpy(pipeBuffer, m_cquotedValue, 1);
							pipeBuffer = pipeBuffer + 1;
						}
						break;
					}

				case eCTYPE_DECIMAL18_FIXED:    /* Decimal number with fixed precision */
					{
						memset(tempDecimal,'\0',1024);
						const IDecimal18Fixed* value = 
							m_pDecimal18Utils->getDecimalFromArray((const IDecimal18Fixed*) (myCollInfo[colNum].data), rowNum);
						IBOOLEAN bTruncated;
						if(b_isQuoted==IFALSE)
						{
							m_pDecimal18Utils->convertTo(value,tempDecimal,100, bTruncated);
							strncpy(pipeBuffer,tempDecimal,strlen(tempDecimal));
							pipeBuffer+=strlen(tempDecimal);
						}
						else
						{
							strncpy(pipeBuffer,m_cquotedValue,1);
							pipeBuffer+=1;
							m_pDecimal18Utils->convertTo(value,tempDecimal,100, bTruncated);
							strncpy(pipeBuffer,tempDecimal,strlen(tempDecimal));
							pipeBuffer+=strlen(tempDecimal);
							strncpy(pipeBuffer,m_cquotedValue,1);
							pipeBuffer+=1;
						}
						break;
					}

				case eCTYPE_DECIMAL28_FIXED:   /* Decimal number with fixed precision */
					{
						memset(tempDecimal,'\0',1024);
						const IDecimal28Fixed* value =m_pDecimal28Utils->getDecimalFromArray((const IDecimal28Fixed*) myCollInfo[colNum].data, rowNum);
						// Get the character array
						IBOOLEAN bTruncated;
						if(b_isQuoted==IFALSE)
						{
							m_pDecimal28Utils->convertTo(value,tempDecimal,100, bTruncated);
							strncpy(pipeBuffer,tempDecimal,strlen(tempDecimal));
							pipeBuffer+=strlen(tempDecimal);
						}
						else
						{
							strncpy(pipeBuffer,m_cquotedValue,1);
							pipeBuffer+=1;
							m_pDecimal28Utils->convertTo(value,tempDecimal,100, bTruncated);
							strncpy(pipeBuffer,tempDecimal,strlen(tempDecimal));
							pipeBuffer+=strlen(tempDecimal);
							strncpy(pipeBuffer,m_cquotedValue,1);
							pipeBuffer+=1;
						}
						break;
					}

				case eCTYPE_ITDKDATETIME:   /* readable date time for ITDK */
					break;
				case eCTYPE_UNICHAR:
					{					
						ISTATUS iRes ;
						IUINT32 npulBytes;
						IUString a;
						//ICHAR*  pszCharData = (ICHAR*)0;
						
						const IUNICHAR* pValueChar = *((const IUNICHAR**)(myCollInfo[colNum].data)+rowNum);						
						// Fix for CR # 176520
						if (m_InsertEscapeCHAR)
						{
								const IUNICHAR *escapechar = m_escapeChar.getBuffer();
								const IUNICHAR *nulval   = m_nullValue.getBuffer();
								IINT32 len = IUStrlen (pValueChar);
								//Here we are assuming that all the character in the col could be special one.
								IUNICHAR * temp = new IUNICHAR[len + sqldatatypes[colNum].prec+1];
								IINT32 j = 0;

								if(m_bNULLCase)
								{
									//this is the only case where data in the source can actually contain NULL string which needs to be escaped.
									if(m_bAddEscapeCharForNullChar)
									{
										//there is sufficient data to hold NULL
										if(0 == PCN_NULL.compare(pValueChar))
										{
											temp[j] = m_cescapeChar[0];
											j++;
										}
									}
								}

								for (IINT32 i =0 ; i <len ; i++)
								{
									if((m_bNULLCase == IFALSE) && (!m_nullValue.isEmpty()))
									{
										if(m_bAddEscapeCharForNullChar)
										{										
											if(pValueChar[i]== nulval[0])
											{
												temp[j] = m_cescapeChar[0];
												j++;
											}
										}
									}

									//In this case since char will be of 2 bytes, we will compare it with the byte value of delimtier. 
									if (pValueChar[i]=='\r'|| pValueChar[i]=='\n'||pValueChar[i]=='\0'||pValueChar[i]==escapechar[0]  || pValueChar[i]== m_iDelimiter)
									{
										temp[j] = escapechar[0];
										j++;
									}
									temp[j] = pValueChar[i];
									j++;
								}
								temp[j] = '\0';
								len = IUStrlen (temp);	

								//if(sqldatatypes[colNum].isColNchar==ITRUE)
								//	iRes = Utils::_ConvertToMultiByte(m_pLocale_UTF8, temp, &pszCharData, &npulBytes, ITRUE);
								//else
								//	iRes = Utils::_ConvertToMultiByte(m_pLocale_Latin9, temp, &pszCharData, &npulBytes, ITRUE);

								//memset (unicharData, 0, maxUnicharSpace);

								if(sqldatatypes[colNum].isColNchar==ITRUE)
								{
									npulBytes = m_pLocale_UTF8->U2M (temp, unicharData);
								}
								else
								{
									npulBytes = m_pLocale_Latin9->U2M (temp, unicharData);
								}


								if(b_isQuoted==IFALSE)
								{
									memcpy(pipeBuffer,unicharData,npulBytes);
									pipeBuffer+=npulBytes;
								}
								else
								{
									strncpy(pipeBuffer,m_cquotedValue,1);
									pipeBuffer+=1;

									memcpy(pipeBuffer,unicharData,npulBytes);
									pipeBuffer+=npulBytes;

									strncpy(pipeBuffer,m_cquotedValue,1);
									pipeBuffer+=1;

								}
								DELETEPTRARR <IUNICHAR>(temp);

						}
						// Fix for CR # 176520
						else
						{
							//if(sqldatatypes[colNum].isColNchar==ITRUE)
							//	iRes = Utils::_ConvertToMultiByte(m_pLocale_UTF8, pValueChar, &pszCharData, &npulBytes, ITRUE);
							//else
							//	iRes = Utils::_ConvertToMultiByte(m_pLocale_Latin9, pValueChar, &pszCharData, &npulBytes, ITRUE);

							if(sqldatatypes[colNum].isColNchar==ITRUE)
							{
								npulBytes = m_pLocale_UTF8->U2M (pValueChar, unicharData);
							}
							else
							{
								npulBytes = m_pLocale_Latin9->U2M (pValueChar, unicharData);
							}

							if(b_isQuoted==IFALSE)
							{
								memcpy(pipeBuffer,unicharData,npulBytes);
								pipeBuffer+=npulBytes;
							}
							else
							{
								strncpy(pipeBuffer,m_cquotedValue,1);
								pipeBuffer+=1;

								memcpy(pipeBuffer,unicharData,npulBytes);
								pipeBuffer+=npulBytes;

								strncpy(pipeBuffer,m_cquotedValue,1);
								pipeBuffer+=1;
							}
						}

						//FREEPTR <ICHAR>(pszCharData);
						break;
					}
				default:
					break;
				}

				if(colNum < (l_numProjectedCols-1))
				{
					strncpy(pipeBuffer,m_cdelimiter,1);
					pipeBuffer+=1;
				}
			}

			//rjain we have update strategy column in external table only in case of data driven mode
			if(l_rowType != "" && this->m_rowsAs == 3)
			{
				strncpy(pipeBuffer,m_cdelimiter,1);
				pipeBuffer+=1;

				if(b_isQuoted==IFALSE)
				{
					strncpy(pipeBuffer,l_rowType,1);
					pipeBuffer+=1;
				}
				else
				{
					strncpy(pipeBuffer,m_cquotedValue,1);
					pipeBuffer+=1;
					strncpy(pipeBuffer,l_rowType,1);
					pipeBuffer+=1;
					strncpy(pipeBuffer,m_cquotedValue,1);
					pipeBuffer+=1;
				}
			}

			strncpy(pipeBuffer,"\n",1);
			pipeBuffer+=1;

		}
		IINT32 tolalBuffLen=pipeBuffer-m_pipeBuffer;
		fwrite(m_pipeBuffer,tolalBuffLen,1,m_fHandle);

		output->setNumRowsProcessed(numRowsAvailable);

		DELETEPTRARR <ICHAR>(tempDecimal);
		DELETEPTRARR <ICHAR>(m_pipeBuffer);

	}
	catch(...)
	{
		iResult = EFailure;
	}
	return iResult;
}


/********************************************************************
* Name : openWRPipe
* Description : opens the pipe in write mode
********************************************************************/

ISTATUS PCNWriter_Partition::openWRPipe()
{
	ISTATUS iResult = ISUCCESS;	
	tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_PIPE_OPENING),m_fullPath.getBuffer());
	if(traceLevel >= SLogger::ETrcLvlNormal)
		m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
	try
	{
		#ifdef WIN32
			IINT32 handle = _open_osfhandle((LONG)m_hPipe, _O_APPEND );
			m_fHandleUpdateDeleteInsert = fdopen(handle, "wb+");
			if(m_fHandleUpdateDeleteInsert==NULL)
			{
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_OPEN_PIPE_FAIL));
				return IFAILURE;
			}
			setvbuf(m_fHandleUpdateDeleteInsert, NULL, _IOFBF , 1024*64);
			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_OPEN_PIPE_WRT_MODE),m_fullPath.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		#else
			ICHAR * temp  =  Utils::IUStringToChar(m_fullPath) ;
			//Fix for CRCR 177639
			IINT32 fd = open(temp, O_WRONLY);
			m_fHandleUpdateDeleteInsert = (fd != -1 ? fdopen(fd, "wb") : NULL);

			DELETEPTRARR <ICHAR>(temp);
			if(m_fHandleUpdateDeleteInsert==NULL)
			{
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_OPEN_PIPE_FAIL));
				return IFAILURE;
			}
			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_OPEN_PIPE_WRT_MODE),m_fullPath.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		#endif
		m_pipeCreation =1;
		m_fHandle = m_fHandleUpdateDeleteInsert;		
		m_bIsPipeOpened=ITRUE;
	}
	catch(...)
	{
		iResult = IFAILURE;
	}
	return iResult;
}


void checkRet(SQLRETURN rc,IUString query,SQLHSTMT hstmt,MsgCatalog* msgCatalog, SLogger *sLogger,IBOOLEAN& bCheckAnyfailedQuery, IUINT32 traceLevel)
{
	SQLWCHAR        sqlState[1024];
	SQLWCHAR        msgeTxt[1024];
	SQLINTEGER      ntveErr;
	SQLSMALLINT     txtLngth;
	IINT32          iStat;

	IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;
	if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	{
		bCheckAnyfailedQuery=ITRUE;
		tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_EXEC_QRY_FAIL),query.getBuffer());
		if(traceLevel >= SLogger::ETrcLvlTerse)
			sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

		iStat = 1;
		SQLRETURN rc;
		while (1)
		{
			rc = SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt,iStat, (SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth);
			//EBF4308: the above call does not return SQL_NO_DATA but only SQL_ERROR after the error is read once
			if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				break;

			if(txtLngth != 0)
			{
				tempVariableChar.sprint(msgCatalog->getMessage(MSG_CMN_MSG),IUString(msgeTxt).getBuffer());
				if(traceLevel >= SLogger::ETrcLvlTerse)
					sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			}
			//printing the sqlState
			tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_SQL_STATE),IUString(sqlState).getBuffer());
			if(traceLevel >= SLogger::ETrcLvlNormal)
				sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
			iStat++;
		}
	}
	else
	{
		tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_EXEC_QRY),query.getBuffer());
		if(traceLevel >= SLogger::ETrcLvlNormal)
			sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
	}
}
/********************************************************************
* Name : RunDeleteQuery
* Description : called by Delete thread which executes Delete Thread structure
queries
********************************************************************/

void* RunThreadQuery(LPVOID lpvParam)
{
	SQLRETURN		rc;
	ThreadArgs*		threadArgs = (ThreadArgs*)lpvParam;
	// Variables used for debug meassages from ODBC
	SQLWCHAR        sqlState[1024];
	SQLWCHAR        msgeTxt[1024];
	SQLINTEGER      ntveErr;
	SQLSMALLINT     txtLngth;
	SQLRETURN       rc2;
	SQLTCHAR *		tempSQLTChar;
	IUString	tempVariableChar=PCN_Constants::PCN_EMPTY;

	SLogger *sLogger=threadArgs->sLogger;
	MsgCatalog*	msgCatalog=threadArgs->msgCatalog;
	SQLHSTMT hstmt=threadArgs->hstmt;
	IUINT32 traceLevel=threadArgs->traceLevel;
	//in case of error child thread needs to open the pipe and close it, so that parent threads know that child thread has exited.
	IBOOLEAN bOpenClosePipe=IFALSE;	

	PCNWriter_Group *parentGroupDriver=threadArgs->pcnWriterObj->getParentGroupDriver();

	ISTATUS status=threadArgs->pUtils->initThreadStructs(threadArgs->threadName);
	int noPartLoaded; //noPartLoaded preserves no.of partitions loaded data into temp table till now

	/*
	In case data needs to be loaded from temp table to the target table, Only the last partition(which issues commit last)
	will load data in to the target table, rest all partitions will only drop their external tables. 

	In case data has to loaded from respective external table to the target table, all the partition will execute
	loading query, but the drop temp table query will be executed by last partition. 

	For making these decisions we have made two booleans which recognize whether this is the last partition or whether
	this partition load data in to the target table. 
	*/
	if(threadArgs->loadTempTableQuery.isEmpty())
	{
		parentGroupDriver->lockPartDoneMutex();
		parentGroupDriver->incrementNoPartLoadedDataTemp();
		noPartLoaded=parentGroupDriver->getNoPartLoadedDataTemp();
		if(parentGroupDriver->getNoPartLoadedDataTemp() != parentGroupDriver->getPartitionCount())
			threadArgs->b_isLastPartition=IFALSE;
		parentGroupDriver->unlockPartDoneMutex();

		if(threadArgs->b_isLastPartition || threadArgs->loadTempTableQuery.isEmpty())
			threadArgs->b_isLoadInToTgt=ITRUE;
		else
			threadArgs->b_isLoadInToTgt=IFALSE;
	}



	/*
	Each partition will load rows in to the shared temp table, and will issue the commit call. 
	*/
	if(!threadArgs->loadTempTableQuery.isEmpty())
	{
		tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->loadTempTableQuery);
		rc = SQLExecDirect(threadArgs->hstmt,tempSQLTChar,SQL_NTS);
		checkRet(rc,threadArgs->loadTempTableQuery,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);
		DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			bOpenClosePipe=ITRUE;
		}
		else if(parentGroupDriver->getPartitionCount() >1)
		{
			//Only in case there are more than one partition we commit the temp table so that last partition can read all the data. 
			rc = SQLEndTran(SQL_HANDLE_DBC,threadArgs->hdbc,SQL_COMMIT);
			if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			{
				// Failed to commit the transaction
				if(traceLevel >= SLogger::ETrcLvlTerse)
					sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,msgCatalog->getMessage(MSG_WRT_COMMIT_TRANS_FAIL_TEMP));

				threadArgs->pUtils->deinitThreadStructs();
				return NULL;
			}
			// Commit successful

			if(traceLevel >= SLogger::ETrcLvlNormal)
				sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,msgCatalog->getMessage(MSG_WRT_COMMIT_SUCC_TEMP));

			//In case partition driver loads data in to the temp table, we would like to determine last partition.
			parentGroupDriver->lockPartDoneMutex();
			parentGroupDriver->incrementNoPartLoadedDataTemp();
			noPartLoaded = parentGroupDriver->getNoPartLoadedDataTemp();
			if(parentGroupDriver->getNoPartLoadedDataTemp() != parentGroupDriver->getPartitionCount())
				threadArgs->b_isLastPartition=IFALSE;
			parentGroupDriver->unlockPartDoneMutex();

			if(threadArgs->b_isLastPartition || threadArgs->loadTempTableQuery.isEmpty())
				threadArgs->b_isLoadInToTgt=ITRUE;
			else
				threadArgs->b_isLoadInToTgt=IFALSE;


		}
	}

	//If this partition load data in to the target table. 
	if(threadArgs->b_isLoadInToTgt)
	{
		if(!threadArgs->rejectRowQuery.isEmpty())
		{
			tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->rejectRowQuery);
			rc = SQLExecDirect(threadArgs->hstmt,tempSQLTChar,SQL_NTS);
			checkRet(rc,threadArgs->rejectRowQuery,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);
			DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
		}

		int ignoreAppliedRows = GetConfigIntValue(IGNORE_APPLIED_ROWS_FLAG);
		if(!threadArgs->getAffMinusAppQry.isEmpty() && !ignoreAppliedRows)
		{
			tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->getAffMinusAppQry);
			rc = SQLExecDirect(threadArgs->hstmt,tempSQLTChar,SQL_NTS);
			DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
			if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			{
				threadArgs->bCheckAnyfailedQuery=ITRUE;
				tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_EXEC_QRY_FAIL),threadArgs->getAffMinusAppQry.getBuffer());
				sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());

				rc2 = SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt,1,(SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth);
				if(txtLngth != 0)
				{
					tempVariableChar.sprint(msgCatalog->getMessage(MSG_CMN_MSG),(IUString(msgeTxt).getBuffer()));
					sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
				}
			}
			else
			{
				SQLINTEGER rowCount;
				SQLLEN lenRowcount;
				rc=SQLBindCol(hstmt, 1, SQL_INTEGER, &rowCount,sizeof(SQLINTEGER), &lenRowcount);
				if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				{
					threadArgs->bCheckAnyfailedQuery=ITRUE;
					tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_EXEC_QRY_FAIL),threadArgs->getAffMinusAppQry.getBuffer());
					sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
					rc2 = SQLGetDiagRecW(SQL_HANDLE_STMT, hstmt,1,(SQLWCHAR*)sqlState, &ntveErr, (SQLWCHAR*)msgeTxt, 1024, &txtLngth);
					if(txtLngth != 0)
					{
						tempVariableChar.sprint(msgCatalog->getMessage(MSG_CMN_MSG),(IUString(msgeTxt).getBuffer()));
						sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
					}
				}
				else
				{
					tempVariableChar.sprint(msgCatalog->getMessage(MSG_WRT_EXEC_QRY),threadArgs->getAffMinusAppQry.getBuffer());
					if(traceLevel >= SLogger::ETrcLvlVerboseData)
						sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlVerboseData,IUString(tempVariableChar).getBuffer());
					while(TRUE)
					{
						rc = SQLFetch(hstmt);
						if(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
						{
							threadArgs->affMinusAppCount=rowCount;
						}
						else
							break;
					}
				}
				rc = SQLFreeStmt (hstmt, SQL_UNBIND);
				rc = SQLFreeStmt (hstmt, SQL_CLOSE);
			}
		}

		if (!threadArgs->insertQuery.isEmpty())
		{
			tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->insertQuery);
			rc = SQLExecDirect(hstmt,tempSQLTChar,SQL_NTS);
			checkRet(rc,threadArgs->insertQuery,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);
			DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
			if(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
			{
				rc=  SQLRowCount(hstmt,&threadArgs->rowsInserted);
				if ( threadArgs->rowsInserted < 0)
					threadArgs->rowsInserted =0;
			}
			else if(threadArgs->loadTempTableQuery.isEmpty())
			{
				bOpenClosePipe=ITRUE;

			}
		}

		if(threadArgs->b81103Upsert)
		{
			if(!threadArgs->deleteForUpsertQuery.isEmpty())
			{
				tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->deleteForUpsertQuery);
				rc = SQLExecDirect(hstmt,tempSQLTChar,SQL_NTS);
				checkRet(rc,threadArgs->deleteForUpsertQuery,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);
				DELETEPTRARR <SQLTCHAR>(tempSQLTChar);		
			}
		}

		if(!threadArgs->updateQuery.isEmpty())
		{
			tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->updateQuery);
			rc = SQLExecDirect(hstmt,tempSQLTChar,SQL_NTS);
			checkRet(rc,threadArgs->updateQuery,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);
			DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
			if(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
			{
				rc=  SQLRowCount(hstmt,&threadArgs->rowsUpdated);
				if ( threadArgs->rowsUpdated<0)
					threadArgs->rowsUpdated =0;
			}
			else if(threadArgs->loadTempTableQuery.isEmpty() && threadArgs->insertQuery.isEmpty())
			{
				bOpenClosePipe=ITRUE;
			}
		}

		if(!threadArgs->insertForUpsertQuery.isEmpty())
		{
			tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->insertForUpsertQuery);
			rc = SQLExecDirect(hstmt,tempSQLTChar,SQL_NTS);
			checkRet(rc,threadArgs->insertForUpsertQuery,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);
			DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
			SQLLEN tempRowCount=0;
			if(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
			{
				rc=  SQLRowCount(hstmt,&tempRowCount);
				if ( tempRowCount>0)
					threadArgs->rowsUpdated +=tempRowCount;
			}
		}

		if(!threadArgs->deleteQuery.isEmpty())
		{
			tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->deleteQuery);
			rc = SQLExecDirect(hstmt,tempSQLTChar,SQL_NTS);
			checkRet(rc,threadArgs->deleteQuery,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);
			DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
			if(rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO)
			{
				rc=  SQLRowCount(hstmt,&threadArgs->rowsDeleted);
				if ( threadArgs->rowsDeleted<0)
					threadArgs->rowsDeleted =0;
			}
		}

		/*
		This is a hack implemented for the EBF10243
		We ideally do not support SonG mode for the Netezza bulk mode, since customer were already
		using SonG in cases where key constraints were disabled but in this case temp table was 
		not getting dropped.
		We have implemented a hack where we are relying on the number of partitions initialzed to know
		which partition is the last one and can be used to dropping the temp table.
		Note: Temp table will not be dropped when key constraints are enabled.
		*/
		if(noPartLoaded == parentGroupDriver->getPartitionsInitializedCount())
		{
			threadArgs->b_isLastPartition = ITRUE;
		}

		//If this is the last partition. 
		if(threadArgs->b_isLastPartition)
		{
			if(!threadArgs->dropTempTable.isEmpty())
			{
				tempSQLTChar = Utils::IUStringToSQLTCHAR(threadArgs->dropTempTable);
				rc = SQLExecDirect(hstmt,tempSQLTChar,SQL_NTS);
				checkRet(rc,threadArgs->dropTempTable,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);				
				DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
			}
		}
	}

	if(!threadArgs->dropExtTab.isEmpty())
	{
		tempSQLTChar= Utils::IUStringToSQLTCHAR(threadArgs->dropExtTab);
		rc = SQLExecDirect(hstmt,tempSQLTChar,SQL_NTS);
		checkRet(rc,threadArgs->dropExtTab,hstmt,msgCatalog,sLogger,threadArgs->bCheckAnyfailedQuery,traceLevel);
		DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
	}

	//In case of errros, Netezza havnt opened pipe, then We are doing it mannualy.
	if(bOpenClosePipe==ITRUE && IFALSE==threadArgs->pcnWriterObj->bIsPipeOpened())
	{	
		ICHAR * fullPath = Utils::IUStringToChar(threadArgs->pipeFullPath);

		tempVariableChar.sprint(msgCatalog->getMessage(MSG_CMN_PIPE_OPENING),threadArgs->pipeFullPath.getBuffer());
		if(traceLevel >= SLogger::ETrcLvlNormal)
			sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());
		
		IINT32 fd = open(fullPath, O_RDONLY);
		FILE *fp = (fd != -1 ? fdopen(fd, "rb") : NULL);
		
		tempVariableChar.sprint(msgCatalog->getMessage(MSG_CMN_PIPE_OPEN),threadArgs->pipeFullPath.getBuffer());
		if(traceLevel >= SLogger::ETrcLvlNormal)
			sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,IUString(tempVariableChar).getBuffer());

		if(fp != NULL)
			fclose(fp);
		DELETEPTRARR <ICHAR>(fullPath);
	}	
			
	threadArgs->pUtils->deinitThreadStructs();
	return NULL;
}


/********************************************************************
* Name : autoCommitOFF
* Description : disable auto commit
********************************************************************/

ISTATUS PCNWriter_Partition::autoCommitOFF()
{
	ISTATUS iResult = ISUCCESS;
	SQLRETURN   rc;
	// Support for transaction recovery
	// set the autocommit off to manually commit the transaction
	// if the transaction is succesfull the transaction is committed.
	// It is set before the queries are executed.
	try
	{
		rc = SQLSetConnectAttr(m_hdbc,SQL_ATTR_AUTOCOMMIT,SQL_AUTOCOMMIT_OFF,SQL_IS_INTEGER);
		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_SET_AUTO_COMMIT_OFF));
			//this is done to read the pipe which is waiting
			//for some process to read it
			#ifndef WIN32 // defined (UNIX)
				IUString sPipe = IUString("cat ");
				sPipe.append(IUString( m_fullPath));
				sPipe.append(IUString( " > pipeData.dat"));
				ICHAR * temp = Utils::IUStringToChar(sPipe);
				system(temp);
				system("rm -f pipeData.dat");
				DELETEPTRARR <ICHAR>(temp);
			#endif
			throw IFAILURE;
		}
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_AUTO_COMMIT_OFF));
	}
	catch(...)
	{
		iResult = IFAILURE;
	}
	return iResult;
}

/********************************************************************
* Name : commit
* Description : commit the transaction
********************************************************************/

ISTATUS PCNWriter_Partition::commit()
{
	ISTATUS iResult = ISUCCESS;
	SQLRETURN rc;
	//Commit the transaction
	try
	{
		rc = SQLEndTran(SQL_HANDLE_DBC,m_hdbc,SQL_COMMIT);
		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			// Failed to commit the transaction
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_COMMIT_TRANS_FAIL));
			throw IFAILURE;
		}
		// Commit successful

		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_COMMIT_SUCC));
	}
	catch(...)
	{
		iResult = IFAILURE;
	}
	return iResult;
}

/********************************************************************
* Name : flushPipe
* Description : the pipe is flushed in case of failure
********************************************************************/

void PCNWriter_Partition::flushPipe(IUString msgKey, IUString externalTable)
{
	IUString sPipe;
	#ifndef WIN32 // defined (UNIX)
		if (!msgKey.isEmpty())
		{
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(msgKey));
		}
		//this is done to read the pipe which waiting
		//for some process to read it
		//this query is used for unix to read the pipe
		//in case of no links b/w src and tgt for other fields
		sPipe = "cat ";
		sPipe.append(IUString( m_fullPath));
		sPipe.append(IUString( " > pipeData.dat"));
		ICHAR * temp = Utils::IUStringToChar(sPipe);
		system(temp);
		system("rm -f pipeData.dat");
		DELETEPTRARR <ICHAR>(temp);
	#endif
}


/********************************************************************
* Name : CreateExtTabCol
* Description : This function set the parameter for external table query.
Depending upon the update strategy selected it will set 'SAME AS TARGET_TABLE'
for for insert/upsert and target table schema will be use for UPDATE/DELETE operation
*Will return error in case of any error in creating columns
********************************************************************/

ISTATUS PCNWriter_Partition::CreateExtTabCol()
{
	ISTATUS		iResult = ISUCCESS;
	IUString	ColumnName ;
	IUString	tempPid;

	IUINT32 l_numProjectedCols = m_inputFieldList.length();
	if(l_numProjectedCols==0)
	{
		//BEGIN New Feature for Table name and prefix override Ayan
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_REL_NOT_FOUND_NETZ),m_OriginalTableName.getBuffer());
		//END New Feature for Table name and prefix override Ayan
		if(this->traceLevel >= SLogger::ETrcLvlTerse)
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		throw IFAILURE;
	}
	
	m_strTgtTblSchema = " ( ";
	for(IUINT32 colNum=0;colNum<l_numProjectedCols;colNum++)
	{
		ITargetField *iTgtFld=(ITargetField*)m_inputFieldList.at(colNum);
		ColumnName=PCN_Constants::PCN_EMPTY;
		ColumnName.append(IUString(" \""));
		ColumnName.append(m_allLinkedFldNames.at(colNum));
		ColumnName.append(IUString("\" "));
		ColumnName.append(sqldatatypes[colNum].dataType);
		
		if(sqldatatypes[colNum].prec < sqldatatypes[colNum].scale || sqldatatypes[colNum].scale<0)
			sqldatatypes[colNum].scale=0;

		
		if(PCN_CHAR == sqldatatypes[colNum].dataType
			|| PCN_VARCHAR == sqldatatypes[colNum].dataType
			|| PCN_NCHAR == sqldatatypes[colNum].dataType
			|| PCN_NVARCHAR == sqldatatypes[colNum].dataType)
		{
			ColumnName.append(IUString("(")) ;
			IUString p=PCN_Constants::PCN_EMPTY ;
			p.sprint(IUString("%d"),sqldatatypes[colNum].prec);
			ColumnName.append(p);
			ColumnName.append(IUString(")"));
		}
		else if(PCN_NUMERIC == sqldatatypes[colNum].dataType)
		{
			ColumnName.append(IUString("(")) ;
			IUString p=PCN_Constants::PCN_EMPTY ;
			p.sprint(IUString("%d"),ICHAR(sqldatatypes[colNum].prec));
			ColumnName.append(p);
			ColumnName.append(IUString(","));
			p.sprint(IUString("%d"),ICHAR(sqldatatypes[colNum].scale));
			ColumnName.append(p);
			ColumnName.append(IUString(")"));

		}
		//if the column is defined as NOT NULL In the backend we are adding same in the temp table schema
		if(sqldatatypes[colNum].isNUllable==IFALSE)		
		{
			ColumnName.append(IUString(" "));
			ColumnName.append(IUString(" NOT NULL"));
		}
		if (colNum==0 ) //for the very first call do not put comma after the column
		{
			m_strTgtTblSchema .append(ColumnName);
		}
		else
		{
			m_strTgtTblSchema .append(IUString(","));
			m_strTgtTblSchema.append(ColumnName);
		}
	}	

	//in case of data driven mode only we are creating an extra column as update strategy in external table.
	if(this->m_rowsAs == 3)
	{
		UpdateStrategy= IUString(", UpdateStrategy_");
		tempPid.sprint(IUString("%d"),this->ParentID);
		UpdateStrategy.append(tempPid);
		UpdateStrategy.append(IUString(" char(1)"));
		m_strTgtTblSchema.append(UpdateStrategy );
	}
	m_strTgtTblSchema.append(IUString(" )"));

	return iResult;
}

ISTATUS PCNWriter_Partition:: CreateExternalTable()
{			
	IINT32		iKeyfld;
	SQLRETURN   rc;
	IUString	tempPid;
	SQLTCHAR *	tempSQLTChar;

	//query to create external table
	IUString	externalTableQuery=PCN_Constants::PCN_EMPTY;
	//query to drop temp table
	IUString	dropTempTable=PCN_Constants::PCN_EMPTY;
	//query to drop external table
	IUString	dropExtTab =PCN_Constants::PCN_EMPTY;
	//query to load data in to the temp table
	IUString	loadTempTableQuery=PCN_Constants::PCN_EMPTY;
	//this query will only create temp table, data will be inserted into temp table using loadTempTableQuery.
	IUString createTempTableQuery=PCN_Constants::PCN_EMPTY;

	IUString updateQuery=PCN_Constants::PCN_EMPTY;
	IUString insertForUpsertQuery=PCN_Constants::PCN_EMPTY;
	IUString deleteQuery=PCN_Constants::PCN_EMPTY;
	IUString insertQuery=PCN_Constants::PCN_EMPTY;
	IUString rejectRowQuery=PCN_Constants::PCN_EMPTY;
	IUString getAffMinusAppQry=PCN_Constants::PCN_EMPTY;

	//This query will filter duplicates from the temp table
	IUString tempFilterDuplicateQry=PCN_Constants::PCN_EMPTY;
	//Get duplicates from the temp table
	IUString tempGetDuplicateQry=PCN_Constants::PCN_EMPTY;
	//get keys from the temp table which are in target table too.
	IUString tempKeyInTargetQry=PCN_Constants::PCN_EMPTY;
	//get keys from the temp table which are in target table too using IN operation
	IUString tempKeyInTargetQryUsingIn=PCN_Constants::PCN_EMPTY;
	//get keys from the temp table which are not in target table.
	IUString tempKeyNotInTargetQry=PCN_Constants::PCN_EMPTY;
	IUString createBadFileQuery=PCN_Constants::PCN_EMPTY;
	IUString	nullTokenQuery=PCN_Constants::PCN_EMPTY;

	//this object has been defined to provide 81103 upsert fucntionality, in which we used to perform delete and insert.
	IUString deleteForUpsertQuery=PCN_Constants::PCN_EMPTY;	
	if(m_keyFldNames.length() == 0 )
	{
		//if Keyfileds are not present in target thn...session will fail in case of update as update/upsert/delete operation.
		//in case of insert session will not fail..earlier session used to fail if ignore key constrain is unchecked.
		if(this->m_IsDelete == 1 || this->m_IsUpdate == UPDATE || this->m_IsUpdate == UPSERT)
		{
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_KEY_CONS_MISS_TRG));
			}
			return IFAILURE;
		}
		//in case of insert and update as insert..if no of keys in target is zero..ignoreKeyConstraint flag is ignored.
		//for rest of operation session will fail in this case.
		m_ignoreKeyConstraint=1;
	}

	IUINT32     nVal = 0;
	ICHAR       errVal[10];

	//Get the error PowerCenter threshold property
	if((m_pUtils -> getIntProperty(IUtilsServer::EErrorThreshold,nVal)) == IFAILURE)
	{				
		if(this->traceLevel >= SLogger::ETrcLvlTerse)
		{
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_CMN_RETRIV_ERR_THRESH_FAIL));
		}
		return IFAILURE;
	}

	sprintf(errVal,"%d",nVal);
	if ( nVal ==0)
	{
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_STOP_ERROR_SET_CONTINUE),IUString(errVal).getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
	}
	if ( nVal > 0)
	{
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_WRT_STOP_ERROR_SET_FAIL),IUString(errVal).getBuffer(),IUString(errVal).getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
	}

	IUString pid;
	//BEGIN New Feature for Table name and prefix override Ayan
	IUString temp=m_OriginalTableName;

	// form the external table by appending the PID to the PCN_EXT_TAB
	// PCN stands for Persistent Connector to Netezza
	pid.sprint(IUString("%s_%d_%d_%d_%lld"),temp.getBuffer(), getpid(),m_nTgtNum,m_nPartNum,time(NULL));
	IUString externalTable = PCN_Constants::PCN_EMPTY;
	if (!m_OriginalSchemaName.isEmpty())
	{
		externalTable = m_OriginalSchemaName;
		externalTable.append(IUString("\".\""));
		externalTable.append(IUString("pcn_ext_"));
	}
	else
	{
		externalTable = IUString("pcn");//changed from PCN to pcn
		externalTable.append(IUString( "_ext_"));
	}
	externalTable.append(IUString( pid));
	//END New Feature for Table name and prefix override Ayan

	//Allocate database and environment handle
	rc = SQLAllocHandle(SQL_HANDLE_STMT, m_hdbc,&m_hstmt);
	if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	{			
		if(this->traceLevel >= SLogger::ETrcLvlTerse)
		{
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_ALLOC_STMT_HANDLE_FAIL));
		}
		return IFAILURE;

	}
	m_threadArgs->hstmt = m_hstmt;

	//rjain rejected rows to be logged due to NULL constraint  in nzbad file, so for this we create temp table first and
	//then we will create external table using sameas option from temp table. so we create temp table in every case, but we will put data in to temp table in
	//certain scenario only.
	//Ideally for this case we should create external table first with not null option but this feature doesnt not work due to the some problem in netezza, this needs to
	//be fixed be netezza.

	//form the query for CREATE TEMP TABLE
	/*
	We are creating temp table name by appending target instance name. We cant append pid since this table is shared
	by all partitions. 
	*/
	m_tempTblName=this->getParentGroupDriver()->getTempTableName();

	//always creating drop temp table query
	dropTempTable = "DROP TABLE ";
	dropTempTable.append(IUString( "\""));
	dropTempTable.append(IUString( m_tempTblName));
	dropTempTable.append(IUString( "\""));


	//form the query for CREATE EXTERNAL TABLE
	externalTableQuery = "CREATE EXTERNAL TABLE ";
	externalTableQuery.append(IUString( "\""));
	externalTableQuery.append(IUString( externalTable));
	externalTableQuery.append(IUString( "\""));
	externalTableQuery.append(IUString(" SAMEAS "));
	externalTableQuery.append(IUString( "\""));
	externalTableQuery.append(IUString( m_tempTblName));
	externalTableQuery.append(IUString( "\""));

	externalTableQuery.append(IUString( " USING (  DATAOBJECT ('"));
	externalTableQuery.append(IUString( m_fullPath));
	//Appending byte value of delimter in to the query.
	externalTableQuery.append(IUString( "')  DELIMITER "));
	externalTableQuery.append(IUString(m_delimiter));
	if(!m_socketBuffSize.isEmpty())
	{
		externalTableQuery.append(IUString( " SOCKETBUFSIZE "));
		externalTableQuery.append(IUString( m_socketBuffSize));
	}
	externalTableQuery.append(IUString( " MAXERRORS "));
	externalTableQuery.append(IUString( errVal));
	if(!m_errorLogDir.isEmpty())
	{
		externalTableQuery.append(IUString( " LOGDIR '"));
		externalTableQuery.append(IUString( m_errorLogDir));
		externalTableQuery.append(IUString( "'"));
	}
	externalTableQuery.append(IUString( " CTRLCHARS "));
	externalTableQuery.append(IUString( m_ctrlchars));
	externalTableQuery.append(IUString( " CRINSTRING "));
	externalTableQuery.append(IUString( m_crlnstring));

	externalTableQuery.append(IUString( " FILLRECORD TRUE "));
	
	//EBF:11049 
	if(m_bTreatNoDataAsEmpty)
	{
		if(!m_nullValue.isEmpty())
		{
			nullTokenQuery.append(IUString(" NULLVALUE '"));
			nullTokenQuery.append(this->m_nullValue);
			nullTokenQuery.append(IUString("' "));
		}
	}
	else
	{
		nullTokenQuery.append(IUString(" NULLVALUE '"));
		nullTokenQuery.append(this->m_nullValue);
		nullTokenQuery.append(IUString("' "));
	}

	externalTableQuery.append(nullTokenQuery);
	
	if(!m_escapeChar.isEmpty())
	{
		externalTableQuery.append(IUString( " ESCAPECHAR '"));
		externalTableQuery.append(IUString( m_escapeChar));
		externalTableQuery.append(IUString( "' "));
	}
	if(m_quoted !=  PCN_NO)
	{
		externalTableQuery.append(IUString( " QUOTEDVALUE '"));
		externalTableQuery.append(IUString( m_quoted));
		externalTableQuery.append(IUString( "' "));
	}

	externalTableQuery.append(IUString( " REMOTESOURCE 'ODBC' ENCODING 'internal')"));

	IBOOLEAN bTemp=IFALSE;

	/*
	We will create only a single temp table across entire partitions, if this is the first partition to execute init, 
	this will create the temp table and will set this boolean to true. 
	The reason we are creating a single temp table across all partitions is because of thelimitation on the Netezza side:
	In Netezza target if multiple connections try to insert (and if any of them have where clause) then we run in to serialization errors.

	When the first partition(which executes init call first) creates the temp table it issues a commit so that other
	partitions can use this table. We dont issue a commit in case we are having only a single partition. 
	*/
	if(IFALSE == this->getParentGroupDriver()->isTempTableCreated())
	{
		createTempTableQuery = "CREATE TABLE ";
		createTempTableQuery.append(IUString( "\""));
		createTempTableQuery.append(IUString( m_tempTblName));
		createTempTableQuery.append(IUString( "\""));
		createTempTableQuery.append(IUString( m_strTgtTblSchema));

		addDistributionKey(&createTempTableQuery,m_distributionKeyFlds,m_allLinkedFldNames);

		//execute the CREATE TEMP TABLE query
		tempSQLTChar = Utils::IUStringToSQLTCHAR(createTempTableQuery);
		rc = SQLExecDirect(m_hstmt,tempSQLTChar,SQL_NTS);
		DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
		checkRet(rc,createTempTableQuery,m_hstmt,m_msgCatalog,m_sLogger,bTemp,this->traceLevel);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			return IFAILURE;
		else
		{
			this->getParentGroupDriver()->setIfTempTableCreated(ITRUE);
			if(this->getParentGroupDriver()->getPartitionCount() >1)
			{
				//incase of more than one partitions we issue commit so that other partitions can use this temp table. 
				rc = SQLEndTran(SQL_HANDLE_DBC,m_hdbc,SQL_COMMIT);
				if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
				{
					// Failed to commit the transaction
					if(this->traceLevel >= SLogger::ETrcLvlTerse)
						m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_COMMIT_TRANS_FAIL_CRT_TEMPTBL));
					throw IFAILURE;
				}
				// Commit successful

				if(this->traceLevel >= SLogger::ETrcLvlNormal)
					m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_COMMIT_SUCC_CRT_TEMPTBL));
			}

		}
	}

	//execute the CREATE EXTERNAL TABLE
	tempSQLTChar = 	Utils::IUStringToSQLTCHAR(externalTableQuery);
	rc = SQLExecDirect(m_hstmt,tempSQLTChar,SQL_NTS);
	DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
	checkRet(rc,externalTableQuery,m_hstmt,m_msgCatalog,m_sLogger,bTemp,this->traceLevel);
	if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		return IFAILURE;


	//form the query for DROP EXTERNAL TABLE and will execute if any function
	//while CREATing the query fails

	dropExtTab = "DROP TABLE ";
	dropExtTab.append(IUString( "\""));
	dropExtTab.append(IUString( externalTable));
	dropExtTab.append(IUString( "\""));
	//This is set to false initially and if any of the Function while creating query fails
	//this will set to true and will drop the EXT table.
	IBOOLEAN execQueryDropExtTab=IFALSE;


	//creatin some temporary IUstrigs which will be used at several places in other Iusting objects
	IUString tempRowid=PCN_Constants::PCN_EMPTY;
	IUString selectRowidFrmTemp=PCN_Constants::PCN_EMPTY;
	tempRowid.append(IUString( "\""));
	tempRowid.append(IUString( m_tempTblName));
	tempRowid.append(IUString( "\""));
	tempRowid.append(IUString( "."));
	tempRowid.append(IUString( "\""));
	tempRowid.append(IUString( "rowid"));
	tempRowid.append(IUString( "\""));

	selectRowidFrmTemp.append(IUString( " (SELECT "));
	selectRowidFrmTemp.append(IUString( m_cduplicateRowMechanism));
	selectRowidFrmTemp.append(IUString( "(\""));
	selectRowidFrmTemp.append(IUString( m_tempTblName));
	selectRowidFrmTemp.append(IUString( "\".\"rowid\") FROM "));
	selectRowidFrmTemp.append(IUString( "\""));
	selectRowidFrmTemp.append(IUString( m_tempTblName));
	selectRowidFrmTemp.append(IUString( "\""));
	selectRowidFrmTemp.append(IUString( " GROUP BY "));

	for(iKeyfld = 0; iKeyfld < m_keyFldNames.length(); iKeyfld++)
	{
		selectRowidFrmTemp.append(IUString( "\""));
		selectRowidFrmTemp.append(IUString( m_tempTblName));
		selectRowidFrmTemp.append(IUString( "\""));
		selectRowidFrmTemp.append(IUString( "."));
		selectRowidFrmTemp.append(IUString( "\""));
		selectRowidFrmTemp.append(IUString( (m_keyFldNames.at(iKeyfld))));
		selectRowidFrmTemp.append(IUString( "\""));
		if(iKeyfld != m_keyFldNames.length()-1)
			selectRowidFrmTemp.append(IUString( " , "));
	}
	selectRowidFrmTemp.append(IUString( ")"));

	tempFilterDuplicateQry.append(IUString(tempRowid));
	tempFilterDuplicateQry.append(IUString( " IN "));
	tempFilterDuplicateQry.append(IUString(selectRowidFrmTemp));

	tempGetDuplicateQry.append(IUString(tempRowid));
	tempGetDuplicateQry.append(IUString( " NOT IN "));
	tempGetDuplicateQry.append(IUString(selectRowidFrmTemp));


	//create tempKeyInTargetQry if key in target
	for(iKeyfld = 0; iKeyfld < m_keyFldNames.length(); iKeyfld++)
	{
		tempKeyInTargetQry.append(IUString( "\""));
		tempKeyInTargetQry.append(IUString( m_tgtTable));
		tempKeyInTargetQry.append(IUString( "\""));
		tempKeyInTargetQry.append(IUString( "."));
		tempKeyInTargetQry.append(IUString( "\""));
		tempKeyInTargetQry.append(IUString( (m_keyFldNames.at(iKeyfld))));
		tempKeyInTargetQry.append(IUString( "\""));
		tempKeyInTargetQry.append(IUString( "="));
		tempKeyInTargetQry.append(IUString( "\""));
		tempKeyInTargetQry.append(IUString( m_tempTblName));
		tempKeyInTargetQry.append(IUString( "\""));
		tempKeyInTargetQry.append(IUString( "."));
		tempKeyInTargetQry.append(IUString( "\""));
		tempKeyInTargetQry.append(IUString( (m_keyFldNames.at(iKeyfld))));
		tempKeyInTargetQry.append(IUString( "\""));
		if(iKeyfld != m_keyFldNames.length()-1)
			tempKeyInTargetQry.append(IUString( " and "));
	}

	IUString tempKeyField=PCN_Constants::PCN_EMPTY;
	IUString selectKeyFromTgt=PCN_Constants::PCN_EMPTY;

	tempKeyField.append(IUString(" ( "));
	for(iKeyfld = 0; iKeyfld < m_keyFldNames.length(); iKeyfld++)
	{
		tempKeyField.append(IUString( "\""));
		tempKeyField.append(IUString( m_tempTblName));
		tempKeyField.append(IUString( "\""));
		tempKeyField.append(IUString( "."));
		tempKeyField.append(IUString( "\""));
		tempKeyField.append(IUString( (m_keyFldNames.at(iKeyfld))));
		tempKeyField.append(IUString( "\""));

		if(iKeyfld != m_keyFldNames.length()-1)
			tempKeyField.append(IUString( ","));
	}

	tempKeyField.append(IUString( ") "));


	selectKeyFromTgt.append(IUString(" ( SELECT "));

	for(iKeyfld = 0; iKeyfld < m_keyFldNames.length(); iKeyfld++)
	{
		selectKeyFromTgt.append(IUString( "\""));
		selectKeyFromTgt.append(IUString( m_tgtTable));
		selectKeyFromTgt.append(IUString( "\""));
		selectKeyFromTgt.append(IUString( "."));
		selectKeyFromTgt.append(IUString( "\""));
		selectKeyFromTgt.append(IUString( (m_keyFldNames.at(iKeyfld))));
		selectKeyFromTgt.append(IUString( "\""));

		if(iKeyfld != m_keyFldNames.length()-1)
			selectKeyFromTgt.append(IUString( ","));
	}
	selectKeyFromTgt.append(IUString( " FROM "));
	selectKeyFromTgt.append(IUString( "\""));
	selectKeyFromTgt.append(IUString(  m_tgtTable));
	selectKeyFromTgt.append(IUString( "\""));
	selectKeyFromTgt.append(IUString( " ) "));


	tempKeyInTargetQryUsingIn.append(IUString(tempKeyField));
	tempKeyInTargetQryUsingIn.append(IUString(" IN "));
	tempKeyInTargetQryUsingIn.append(IUString(selectKeyFromTgt));

	tempKeyNotInTargetQry.append(IUString(tempKeyField));
	tempKeyNotInTargetQry.append(IUString(" NOT IN "));
	tempKeyNotInTargetQry.append(IUString(selectKeyFromTgt));


	//we are making query for loading data in to the temp table	every time, incase we dont need this
	//query we will make this query empty.
	loadTempTableQuery = "INSERT INTO ";
	loadTempTableQuery.append(IUString( "\""));
	loadTempTableQuery.append(IUString( m_tempTblName));
	loadTempTableQuery.append(IUString( "\""));
	loadTempTableQuery.append(IUString( " SELECT * FROM "));
	loadTempTableQuery.append(IUString( "\""));
	loadTempTableQuery.append(IUString( externalTable));
	loadTempTableQuery.append(IUString( "\""));

	//we are using create external table utility to log rejected rows(by nz connector) in to the file
	createBadFileQuery=" create EXTERNAL TABLE '";
	createBadFileQuery.append(IUString(m_badFileName));
	createBadFileQuery.append(IUString( "' USING ( "));
	createBadFileQuery.append(IUString(" DELIMITER "));
	createBadFileQuery.append(m_delimiter);
	createBadFileQuery.append(nullTokenQuery);
	if(!m_escapeChar.isEmpty())
	{
		createBadFileQuery.append(IUString( " ESCAPECHAR '"));
		createBadFileQuery.append(IUString( m_escapeChar));
		createBadFileQuery.append(IUString( "' "));
	}
	createBadFileQuery.append(IUString(" REMOTESOURCE 'ODBC'  ENCODING 'internal' ) AS  select "));
	for(iKeyfld = 0; iKeyfld < m_allLinkedFldNames.length(); iKeyfld++)
	{
		createBadFileQuery.append(IUString( "\""));
		createBadFileQuery.append(IUString( m_tempTblName));
		createBadFileQuery.append(IUString( "\""));
		createBadFileQuery.append(IUString( "."));
		createBadFileQuery.append(IUString( "\""));
		createBadFileQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
		createBadFileQuery.append(IUString( "\""));

		if(iKeyfld != m_allLinkedFldNames.length()-1)
			createBadFileQuery.append(IUString( ","));
	}
	createBadFileQuery.append(IUString(" from "));
	createBadFileQuery.append(IUString( "\""));
	createBadFileQuery.append(IUString( m_tempTblName));
	createBadFileQuery.append(IUString( "\""));
	createBadFileQuery.append(IUString(" where "));
	//rest of the bad file query will be created later

	//Affected and applied rows would only in case of update as update, upsert and delete operation.
	//also if target already contains duplicates
	//this query will be used to get affected minus applied rows count.
	//The basic idea of this query to get no of duplicates in the target table which are also present in the temp table.
	if(m_IsUpdate==UPDATE || m_IsUpdate==UPSERT || m_IsDelete)
	{
		getAffMinusAppQry="select count(*) from \"";
		getAffMinusAppQry.append(IUString(m_tgtTable));
		getAffMinusAppQry.append(IUString("\" where "));
		getAffMinusAppQry.append(IUString(" \""));
		getAffMinusAppQry.append(IUString( m_tgtTable));
		getAffMinusAppQry.append(IUString( "\".\"rowid\" not in ( select "));

		getAffMinusAppQry.append(IUString( m_cduplicateRowMechanism));
		getAffMinusAppQry.append(IUString( "(\""));
		getAffMinusAppQry.append(IUString( m_tgtTable));
		getAffMinusAppQry.append(IUString( "\".\"rowid\") from \""));
		getAffMinusAppQry.append(IUString( m_tgtTable));
		getAffMinusAppQry.append(IUString( "\" group by"));

		for(iKeyfld = 0; iKeyfld < m_keyFldNames.length(); iKeyfld++)
		{
			getAffMinusAppQry.append(IUString( "\""));
			getAffMinusAppQry.append(IUString( m_tgtTable));
			getAffMinusAppQry.append(IUString( "\".\""));
			getAffMinusAppQry.append(IUString( (m_keyFldNames.at(iKeyfld))));
			getAffMinusAppQry.append(IUString( "\""));
			if(iKeyfld != m_keyFldNames.length()-1)
				getAffMinusAppQry.append(IUString( " , "));
		}
		getAffMinusAppQry.append(IUString( ")"));
		getAffMinusAppQry.append(IUString(" and ("));
		//need to put this piece of code in a bracket to handle multiple keys
		for(iKeyfld = 0; iKeyfld < m_keyFldNames.length(); iKeyfld++)
		{
			getAffMinusAppQry.append(IUString( "\""));
			getAffMinusAppQry.append(IUString( m_tgtTable));
			getAffMinusAppQry.append(IUString( "\".\""));
			getAffMinusAppQry.append(IUString( (m_keyFldNames.at(iKeyfld))));
			getAffMinusAppQry.append(IUString( "\""));
			if(iKeyfld != m_keyFldNames.length()-1)
				getAffMinusAppQry.append(IUString( ","));
		}
		getAffMinusAppQry.append(IUString(") in ( select "));
		for(iKeyfld = 0; iKeyfld < m_keyFldNames.length(); iKeyfld++)
		{
			getAffMinusAppQry.append(IUString( "\""));
			getAffMinusAppQry.append(IUString( m_tempTblName));
			getAffMinusAppQry.append(IUString( "\".\""));
			getAffMinusAppQry.append(IUString( (m_keyFldNames.at(iKeyfld))));
			getAffMinusAppQry.append(IUString( "\""));

			if(iKeyfld != m_keyFldNames.length()-1)
				getAffMinusAppQry.append(IUString( ","));
		}
		getAffMinusAppQry.append(IUString(" from \""));
		getAffMinusAppQry.append(IUString(  m_tempTblName));
		getAffMinusAppQry.append(IUString("\""));
		if(this->m_rowsAs != 3)
			getAffMinusAppQry.append(IUString(")"));
		//in case of data driven mode, we will add update strategy constraint
	}

	//We are inserting only those cols in to the target which are connected
	IUString insertColsInTgt="INSERT INTO ";
	insertColsInTgt.append(IUString( "\""));
	insertColsInTgt.append(IUString( m_tgtTable));
	insertColsInTgt.append(IUString( "\"("));
	for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
	{
		insertColsInTgt.append(IUString( "\""));
		insertColsInTgt.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
		insertColsInTgt.append(IUString( "\""));
		if(iKeyfld != m_allLinkedFldNames.length()-1)
			insertColsInTgt.append(IUString(", "));
	}
	insertColsInTgt.append(IUString( ")"));


	if(this->m_rowsAs != 3)
	{
		if ( m_IsInsert)
		{
			if(m_ignoreKeyConstraint)
			{
				insertQuery.append(insertColsInTgt);
				insertQuery.append(IUString( " SELECT "));
				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( externalTable));
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( "."));
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					insertQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						insertQuery.append(IUString(" , "));
				}
				insertQuery.append(IUString( " FROM "));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString( externalTable));
				insertQuery.append(IUString( "\""));

				loadTempTableQuery="";
			}
			else
			{
				insertQuery.append(insertColsInTgt);
				insertQuery.append(IUString( " SELECT "));

				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( m_tempTblName));
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( "."));
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					insertQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						insertQuery.append(IUString(" , "));
				}
				insertQuery.append(IUString( " FROM "));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString( m_tempTblName));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString(" WHERE "));
				insertQuery.append(IUString(tempKeyNotInTargetQry));
				//Fix for CR 176528, 107699
				insertQuery.append(IUString( " AND "));
				insertQuery.append(IUString(tempFilterDuplicateQry));
				//end of fix


				//If user wants to create a bad file preparing query to log reject row in to bad file.
				if(m_bCreateBadFile)
				{
					rejectRowQuery.append(IUString(createBadFileQuery));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( ".\""));
					rejectRowQuery.append(IUString( "rowid"));
					rejectRowQuery.append(IUString( "\" IN ( "));
					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));
					rejectRowQuery.append(IUString(tempGetDuplicateQry));
					rejectRowQuery.append(IUString(" ) UNION ( "));
					rejectRowQuery.append(IUString(" select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where"));
					rejectRowQuery.append(IUString(tempKeyInTargetQryUsingIn));
					rejectRowQuery.append(IUString(" ))"));
				}
			}
		}
		if (m_IsDelete)
		{
			deleteQuery = "DELETE FROM ";
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString( m_tgtTable));
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString( " WHERE EXISTS (SELECT * FROM "));
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString( m_tempTblName));
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString(" WHERE "));
			deleteQuery.append(IUString(tempKeyInTargetQry));
			deleteQuery.append(IUString( ")"));

			if(m_bCreateBadFile)
			{
				rejectRowQuery.append(IUString(createBadFileQuery));
				rejectRowQuery.append(IUString( "\""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\""));
				rejectRowQuery.append(IUString( ".\""));
				rejectRowQuery.append(IUString( "rowid"));
				rejectRowQuery.append(IUString( "\" IN ( "));
				rejectRowQuery.append(IUString(" ( select \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\" where "));
				rejectRowQuery.append(IUString(tempGetDuplicateQry));
				rejectRowQuery.append(IUString(" ) UNION ( "));
				rejectRowQuery.append(IUString(" select \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\" where"));
				rejectRowQuery.append(IUString(tempKeyNotInTargetQry));
				rejectRowQuery.append(IUString(" ))"));
			}
		}
		if ( m_IsUpdate ==UPDATE)
		{
			IINT32 itrCol =0;
			if(m_nonKeyFldNames.length() == 0)
			{
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_NO_COL_LINK_FOR_UPDT));
				execQueryDropExtTab = ITRUE;
			}
			else
			{
				updateQuery = "UPDATE ";
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( m_tgtTable));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( " SET "));
				for(itrCol = 0; itrCol < m_nonKeyFldNames.length(); itrCol++)
				{
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( (m_nonKeyFldNames.at(itrCol))));
					updateQuery.append(IUString( "\" = \""));
					updateQuery.append(IUString( m_tempTblName));
					updateQuery.append(IUString( "\".\""));
					updateQuery.append(IUString( (m_nonKeyFldNames.at(itrCol))));
					updateQuery.append(IUString( "\""));
					if(itrCol != m_nonKeyFldNames.length()-1)
						updateQuery.append(IUString( ","));
				}
				updateQuery.append(IUString( " WHERE "));
				updateQuery.append(IUString(tempKeyInTargetQry));
				updateQuery.append(IUString( " AND "));
				updateQuery.append(IUString(tempFilterDuplicateQry));

				if(m_bCreateBadFile)
				{
					rejectRowQuery.append(IUString(createBadFileQuery));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( ".\""));
					rejectRowQuery.append(IUString( "rowid"));
					rejectRowQuery.append(IUString( "\" IN ( "));

					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));
					rejectRowQuery.append(IUString(tempGetDuplicateQry));
					rejectRowQuery.append(IUString(" ) UNION ( "));
					rejectRowQuery.append(IUString(" select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where"));
					rejectRowQuery.append(IUString(tempKeyNotInTargetQry));
					rejectRowQuery.append(IUString(" ))"));
				}
			}
		}
		else if( m_IsUpdate ==UPSERT)
		{
			if(m_b81103Upsert || m_b81103UpsertWithIgnoreKey)
			{
				/*
				if user wants 81103 upsert he needs to add upsert81103Functionality or upsert81103FunctionalityWithIgnoreKey
				flag in the integration service, in this case we will not provide reject row logging functionality,
				also we will not diffrentiate beteween affected rows and applied rows.
				*/
				getAffMinusAppQry=PCN_Constants::PCN_EMPTY;
				rejectRowQuery=PCN_Constants::PCN_EMPTY;
				m_bCreateBadFile=IFALSE;
				makeQueriesFor81103Upsert(&deleteForUpsertQuery,&updateQuery,insertColsInTgt,tempFilterDuplicateQry);
			}
			else
			{
				//in case no of key fields are not zero we perform insert operation, if no of non key fields are also not zero we will perform update.
				//if no of key fields are zero session will fail.
				insertForUpsertQuery.append(insertColsInTgt);
				insertForUpsertQuery.append(IUString( " SELECT "));

				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					insertForUpsertQuery.append(IUString( "\""));
					insertForUpsertQuery.append(IUString( m_tempTblName));
					insertForUpsertQuery.append(IUString( "\""));
					insertForUpsertQuery.append(IUString( "."));
					insertForUpsertQuery.append(IUString( "\""));
					insertForUpsertQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					insertForUpsertQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						insertForUpsertQuery.append(IUString(" , "));
				}
				insertForUpsertQuery.append(IUString( " FROM "));
				insertForUpsertQuery.append(IUString( "\""));
				insertForUpsertQuery.append(IUString( m_tempTblName));
				insertForUpsertQuery.append(IUString( "\""));
				insertForUpsertQuery.append(IUString(" WHERE "));

				insertForUpsertQuery.append(IUString(tempKeyNotInTargetQry));
				insertForUpsertQuery.append(IUString( " AND "));
				insertForUpsertQuery.append(IUString(tempFilterDuplicateQry));


				if(m_nonKeyFldNames.length() != 0)
				{
					updateQuery = "UPDATE ";
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( m_tgtTable));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( " SET "));
					for(iKeyfld = 0; iKeyfld < m_nonKeyFldNames.length(); iKeyfld++)
					{
						updateQuery.append(IUString( "\""));
						updateQuery.append(IUString( (m_nonKeyFldNames.at(iKeyfld))));
						updateQuery.append(IUString( "\" = \""));
						updateQuery.append(IUString( m_tempTblName));
						updateQuery.append(IUString( "\".\""));
						updateQuery.append(IUString( (m_nonKeyFldNames.at(iKeyfld))));
						updateQuery.append(IUString( "\""));
						if(iKeyfld != m_nonKeyFldNames.length()-1)
							updateQuery.append(IUString( ","));
					}
					updateQuery.append(IUString( " WHERE "));
					updateQuery.append(IUString(tempKeyInTargetQry));
					updateQuery.append(IUString( " AND "));
					updateQuery.append(IUString(tempFilterDuplicateQry));

					if(m_bCreateBadFile)
					{
						rejectRowQuery.append(IUString(createBadFileQuery));
						rejectRowQuery.append(IUString(tempGetDuplicateQry));
					}

				}
				else
				{
					if(this->traceLevel >= SLogger::ETrcLvlTerse)
						m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_NO_COL_LINK_FOR_UPDT));

					getAffMinusAppQry=PCN_Constants::PCN_EMPTY;

					if(m_bCreateBadFile)
					{
						rejectRowQuery.append(IUString(createBadFileQuery));
						rejectRowQuery.append(IUString( "\""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\""));
						rejectRowQuery.append(IUString( ".\""));
						rejectRowQuery.append(IUString( "rowid"));
						rejectRowQuery.append(IUString( "\" IN ( "));
						rejectRowQuery.append(IUString(" ( select \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\" where "));
						rejectRowQuery.append(IUString(tempGetDuplicateQry));
						rejectRowQuery.append(IUString(" ) UNION ( "));
						rejectRowQuery.append(IUString(" select \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\" where"));
						rejectRowQuery.append(IUString(tempKeyInTargetQryUsingIn));
						rejectRowQuery.append(IUString(" ))"));
					}
				}
			}
		}
		else if(m_IsUpdate==INSERT)
		{
			//rjain support for update as insert
			if(m_ignoreKeyConstraint)
			{
				updateQuery.append(insertColsInTgt);
				updateQuery.append(IUString( " SELECT "));
				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( externalTable));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( "."));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					updateQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						updateQuery.append(IUString(" , "));
				}
				updateQuery.append(IUString( " FROM "));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( externalTable));
				updateQuery.append(IUString( "\""));

				loadTempTableQuery="";
			}
			else
			{
				//query to insert the rows

				updateQuery.append(insertColsInTgt);
				updateQuery.append(IUString( " SELECT "));

				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( m_tempTblName));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( "."));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					updateQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						updateQuery.append(IUString(" , "));
				}
				updateQuery.append(IUString( " FROM "));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( m_tempTblName));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString(" WHERE "));

				updateQuery.append(IUString(tempKeyNotInTargetQry));
				updateQuery.append(IUString( " AND "));
				updateQuery.append(IUString(tempFilterDuplicateQry));

				if(m_bCreateBadFile)
				{
					rejectRowQuery.append(IUString(createBadFileQuery));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( ".\""));
					rejectRowQuery.append(IUString( "rowid"));
					rejectRowQuery.append(IUString( "\" IN ( "));
					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));
					rejectRowQuery.append(IUString(tempGetDuplicateQry));
					rejectRowQuery.append(IUString(" ) UNION ( "));
					rejectRowQuery.append(IUString(" select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where"));
					rejectRowQuery.append(IUString(tempKeyInTargetQryUsingIn));
					rejectRowQuery.append(IUString(" ))"));
				}
			}
		}

	} // if ends for not data driven
	else
	{
		if ( m_IsInsert)
		{
			//query to insert the rows
			if(m_ignoreKeyConstraint)
			{
				insertQuery.append(insertColsInTgt);
				insertQuery.append(IUString( " SELECT "));
				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( m_tempTblName));
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( "."));
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					insertQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						insertQuery.append(IUString(" , "));
				}
				insertQuery.append(IUString( " FROM "));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString( m_tempTblName));
				insertQuery.append(IUString( "\""));

				insertQuery.append(IUString(" WHERE ( "));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString(m_tempTblName));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString("."));
				UpdateStrategy= IUString("UpdateStrategy_");
				tempPid.sprint(IUString("%d"),this->ParentID);
				UpdateStrategy.append(tempPid);
				UpdateStrategy.append(IUString(" = '2' )"));
				insertQuery.append(IUString( UpdateStrategy));
			}
			else
			{
				//query to insert the rows
				insertQuery.append(insertColsInTgt);
				insertQuery.append(IUString( " SELECT "));

				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( m_tempTblName));
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( "."));
					insertQuery.append(IUString( "\""));
					insertQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					insertQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						insertQuery.append(IUString(" , "));
				}
				insertQuery.append(IUString( " FROM "));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString( m_tempTblName));
				insertQuery.append(IUString( "\""));

				insertQuery.append(IUString(" WHERE ( "));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString(m_tempTblName));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString("."));
				UpdateStrategy = IUString("UpdateStrategy_");
				tempPid.sprint(IUString("%d"),this->ParentID);
				UpdateStrategy.append(tempPid);
				UpdateStrategy.append(IUString(" = '2' "));

				insertQuery.append(IUString( UpdateStrategy));
				insertQuery.append(IUString( ") and "));
				insertQuery.append(IUString(tempKeyNotInTargetQry));
				insertQuery.append(IUString( " AND "));
				insertQuery.append(IUString(tempFilterDuplicateQry));

				if(m_bCreateBadFile)
				{
					rejectRowQuery.append(IUString(createBadFileQuery));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( ".\""));
					rejectRowQuery.append(IUString( "rowid"));
					rejectRowQuery.append(IUString( "\" IN ( "));
					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));
					rejectRowQuery.append(IUString(tempGetDuplicateQry));

					rejectRowQuery.append(IUString(" AND \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));

					rejectRowQuery.append(IUString(" ) UNION ( "));
					rejectRowQuery.append(IUString(" select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString( " AND "));

					rejectRowQuery.append(IUString(tempKeyInTargetQryUsingIn));
					rejectRowQuery.append(IUString(" )"));
				}
			}
		}
		else
		{
			if(m_bCreateBadFile)
			{
				//in case insert option is not checked we will reject all rows which are marked for insert.
				UpdateStrategy = IUString("UpdateStrategy_");
				tempPid.sprint(IUString("%d"),this->ParentID);
				UpdateStrategy.append(tempPid);
				UpdateStrategy.append(IUString(" = '2' "));


				rejectRowQuery.append(IUString(createBadFileQuery));
				rejectRowQuery.append(IUString( "\""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\""));
				rejectRowQuery.append(IUString( ".\""));
				rejectRowQuery.append(IUString( "rowid"));
				rejectRowQuery.append(IUString( "\" IN ( "));
				rejectRowQuery.append(IUString(" ( select \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\" where \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\"."));
				rejectRowQuery.append(IUString(UpdateStrategy));
				rejectRowQuery.append(IUString(" ) "));
			}
		}
		if (m_IsDelete)
		{
			deleteQuery = "DELETE FROM ";
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString( m_tgtTable));
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString( " WHERE EXISTS (SELECT * FROM "));
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString( m_tempTblName));
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString(" WHERE "));
			deleteQuery.append(IUString(tempKeyInTargetQry));
			deleteQuery.append(IUString(" and "));
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString(m_tempTblName));
			deleteQuery.append(IUString( "\""));
			deleteQuery.append(IUString("."));
			UpdateStrategy = IUString("UpdateStrategy_");
			tempPid.sprint(IUString("%d"),this->ParentID);
			UpdateStrategy.append(tempPid);
			UpdateStrategy.append(IUString(" = '0'"));
			deleteQuery.append(IUString( UpdateStrategy));
			deleteQuery.append(IUString( ")"));

			if(m_bCreateBadFile)
			{
				if(rejectRowQuery.isEmpty())
				{
					rejectRowQuery.append(IUString(createBadFileQuery));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( ".\""));
					rejectRowQuery.append(IUString( "rowid"));
					rejectRowQuery.append(IUString( "\" IN ( "));

					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));
					rejectRowQuery.append(IUString(tempGetDuplicateQry));
					rejectRowQuery.append(IUString(" AND \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString(" ) "));
				}
				else
				{
					rejectRowQuery.append(IUString(" UNION ( "));
					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));
					rejectRowQuery.append(IUString(tempGetDuplicateQry));
					rejectRowQuery.append(IUString(" AND \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString(" )) "));
				}

				rejectRowQuery.append(IUString(" UNION ( "));
				rejectRowQuery.append(IUString(" select \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\" where "));

				rejectRowQuery.append(IUString( "\""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\"."));
				rejectRowQuery.append(IUString(UpdateStrategy));
				rejectRowQuery.append(IUString( " AND "));

				rejectRowQuery.append(IUString(tempKeyNotInTargetQry));
				rejectRowQuery.append(IUString(" )"));
			}

			getAffMinusAppQry.append(IUString(" where "));
			getAffMinusAppQry.append(IUString(UpdateStrategy));
			if(m_IsUpdate!=UPDATE && m_IsUpdate!=UPSERT)
				getAffMinusAppQry.append(IUString(")"));
		}
		else
		{
			if(m_bCreateBadFile)
			{
				//in case delete option is not checked we will reject all rows which are marked for delete.
				UpdateStrategy = IUString("UpdateStrategy_");
				tempPid.sprint(IUString("%d"),this->ParentID);
				UpdateStrategy.append(tempPid);
				UpdateStrategy.append(IUString(" = '0'"));

				if(rejectRowQuery.isEmpty())
				{
					rejectRowQuery.append(IUString(createBadFileQuery));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( ".\""));
					rejectRowQuery.append(IUString( "rowid"));
					rejectRowQuery.append(IUString( "\" IN ( "));

					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString(" ) "));
				}
				else
				{
					rejectRowQuery.append(IUString(" UNION ( "));
					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString(" )) "));
				}
			}
		}
		if ( m_IsUpdate ==UPDATE)
		{
			IINT32 itrCol =0;

			if(m_nonKeyFldNames.length() == 0)
			{
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_NO_COL_LINK_FOR_UPDT));
				execQueryDropExtTab = ITRUE;
			}
			else
			{
				updateQuery = "UPDATE ";
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( m_tgtTable));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( " SET "));
				for(itrCol = 0; itrCol < m_nonKeyFldNames.length(); itrCol++)
				{
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( (m_nonKeyFldNames.at(itrCol))));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( "="));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( m_tempTblName));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( "."));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( (m_nonKeyFldNames.at(itrCol))));
					updateQuery.append(IUString( "\""));
					if(itrCol != m_nonKeyFldNames.length()-1)
						updateQuery.append(IUString( ","));
				}
				updateQuery.append(IUString( " WHERE "));

				updateQuery.append(IUString(tempKeyInTargetQry));
				updateQuery.append(IUString( " AND "));
				updateQuery.append(IUString(tempFilterDuplicateQry));

				updateQuery.append(IUString(" and "));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString(m_tempTblName));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString("."));
				UpdateStrategy = IUString("UpdateStrategy_");
				tempPid.sprint(IUString("%d"),this->ParentID);
				UpdateStrategy.append(tempPid);
				UpdateStrategy.append(IUString(" = '1'"));
				updateQuery.append(IUString( UpdateStrategy));


				if(m_bCreateBadFile)
				{
					rejectRowQuery.append(IUString(" UNION ( "));
					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));
					rejectRowQuery.append(IUString(tempGetDuplicateQry));
					rejectRowQuery.append(IUString(" AND \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString(" )) "));
					rejectRowQuery.append(IUString(" UNION ( "));

					rejectRowQuery.append(IUString(" select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));

					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString( " AND "));

					rejectRowQuery.append(IUString(tempKeyNotInTargetQry));
					rejectRowQuery.append(IUString(" )"));
				}

				if(m_IsDelete)
				{
					getAffMinusAppQry.append(IUString(" or "));
				}
				else
				{
					getAffMinusAppQry.append(IUString(" where "));
				}
				getAffMinusAppQry.append(IUString(UpdateStrategy));
				getAffMinusAppQry.append(IUString(")"));
			}
		}
		else if( m_IsUpdate ==UPSERT)
		{
			IINT32 itrCol =0;
			UpdateStrategy = IUString("UpdateStrategy_");
			tempPid.sprint(IUString("%d"),this->ParentID);
			UpdateStrategy.append(tempPid);
			UpdateStrategy.append(IUString(" = '1'"));

			if(m_b81103Upsert || m_b81103UpsertWithIgnoreKey)
			{
				/*
				if user wants 81103 upsert he needs to add upsert81103Functionality or upsert81103FunctionalityWithIgnoreKey
				flag in the integration service, in this case we will not provide reject row logging functionality,
				also we will not diffrentiate beteween affected rows and applied rows.
				*/
				getAffMinusAppQry=PCN_Constants::PCN_EMPTY;
				rejectRowQuery=PCN_Constants::PCN_EMPTY;
				m_bCreateBadFile=IFALSE;
				makeQueriesFor81103Upsert(&deleteForUpsertQuery,&updateQuery,insertColsInTgt,tempFilterDuplicateQry);
			}
			else
			{
				insertForUpsertQuery.append(insertColsInTgt);
				insertForUpsertQuery.append(IUString( " SELECT "));

				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					insertForUpsertQuery.append(IUString( "\""));
					insertForUpsertQuery.append(IUString( m_tempTblName));
					insertForUpsertQuery.append(IUString( "\""));
					insertForUpsertQuery.append(IUString( "."));
					insertForUpsertQuery.append(IUString( "\""));
					insertForUpsertQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					insertForUpsertQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						insertForUpsertQuery.append(IUString(" , "));
				}
				insertForUpsertQuery.append(IUString( " FROM "));
				insertForUpsertQuery.append(IUString( "\""));
				insertForUpsertQuery.append(IUString( m_tempTblName));
				insertForUpsertQuery.append(IUString( "\""));

				insertForUpsertQuery.append(IUString(" WHERE ( "));
				insertForUpsertQuery.append(IUString( "\""));
				insertForUpsertQuery.append(IUString(m_tempTblName));
				insertForUpsertQuery.append(IUString( "\""));
				insertForUpsertQuery.append(IUString("."));
				insertForUpsertQuery.append(IUString( UpdateStrategy));
				insertForUpsertQuery.append(IUString(" ) and "));
				insertForUpsertQuery.append(IUString(tempKeyNotInTargetQry));
				insertForUpsertQuery.append(IUString( " AND "));
				insertForUpsertQuery.append(IUString(tempFilterDuplicateQry));

				if(m_nonKeyFldNames.length() >0)
				{
					updateQuery = "UPDATE ";
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( m_tgtTable));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( " SET "));
					for(itrCol = 0; itrCol < m_nonKeyFldNames.length(); itrCol++)
					{
						updateQuery.append(IUString( "\""));
						updateQuery.append(IUString( (m_nonKeyFldNames.at(itrCol))));
						updateQuery.append(IUString( "\""));
						updateQuery.append(IUString( "="));
						updateQuery.append(IUString( "\""));
						updateQuery.append(IUString( m_tempTblName));
						updateQuery.append(IUString( "\""));
						updateQuery.append(IUString( "."));
						updateQuery.append(IUString( "\""));
						updateQuery.append(IUString( (m_nonKeyFldNames.at(itrCol))));
						updateQuery.append(IUString( "\""));
						if(itrCol != m_nonKeyFldNames.length()-1)
							updateQuery.append(IUString( ","));
					}
					updateQuery.append(IUString( " WHERE "));

					updateQuery.append(IUString(tempKeyInTargetQry));
					updateQuery.append(IUString( " AND "));
					updateQuery.append(IUString(tempFilterDuplicateQry));
					updateQuery.append(IUString(" and "));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString(m_tempTblName));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString("."));
					updateQuery.append(IUString( UpdateStrategy));

					if(m_bCreateBadFile)
					{
						rejectRowQuery.append(IUString(" UNION ( "));
						rejectRowQuery.append(IUString(" ( select \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\" where "));
						rejectRowQuery.append(IUString(tempGetDuplicateQry));
						rejectRowQuery.append(IUString(" AND \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\"."));
						rejectRowQuery.append(IUString(UpdateStrategy));
						rejectRowQuery.append(IUString(" )) "));
					}

					if(m_IsDelete)
					{
						getAffMinusAppQry.append(IUString(" or "));
					}
					else
					{
						getAffMinusAppQry.append(IUString(" where "));
					}
					getAffMinusAppQry.append(IUString(UpdateStrategy));
					getAffMinusAppQry.append(IUString(")"));
				}
				else
				{
					if(this->traceLevel >= SLogger::ETrcLvlTerse)
						m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_NO_COL_LINK_FOR_UPDT));

					if(m_bCreateBadFile)
					{
						rejectRowQuery.append(IUString(" UNION ( "));
						rejectRowQuery.append(IUString(" ( select \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\" where "));
						rejectRowQuery.append(IUString(tempGetDuplicateQry));
						rejectRowQuery.append(IUString(" AND \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\"."));
						rejectRowQuery.append(IUString(UpdateStrategy));
						rejectRowQuery.append(IUString(" )) "));
						rejectRowQuery.append(IUString(" UNION ( "));
						rejectRowQuery.append(IUString(" select \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\" where "));

						rejectRowQuery.append(IUString( "\""));
						rejectRowQuery.append(IUString( m_tempTblName));
						rejectRowQuery.append(IUString( "\"."));
						rejectRowQuery.append(IUString(UpdateStrategy));
						rejectRowQuery.append(IUString( " AND "));

						rejectRowQuery.append(IUString(tempKeyInTargetQryUsingIn));
						rejectRowQuery.append(IUString(" )"));
					}

					if(m_IsDelete)
					{
						getAffMinusAppQry.append(IUString(")"));
					}
					else
					{
						getAffMinusAppQry=PCN_Constants::PCN_EMPTY;
					}
				}
			}
		}
		else if(m_IsUpdate==INSERT)
		{
			//query to insert the rows
			if(m_ignoreKeyConstraint)
			{
				updateQuery.append(insertColsInTgt);
				updateQuery.append(IUString( " SELECT "));
				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( m_tempTblName));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( "."));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					updateQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						updateQuery.append(IUString(" , "));
				}
				updateQuery.append(IUString( " FROM "));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( m_tempTblName));
				updateQuery.append(IUString( "\""));

				updateQuery.append(IUString(" WHERE ( "));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString(m_tempTblName));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString("."));
				UpdateStrategy= IUString("UpdateStrategy_");
				tempPid.sprint(IUString("%d"),this->ParentID);
				UpdateStrategy.append(tempPid);
				UpdateStrategy.append(IUString(" = '1' )"));
				updateQuery.append(IUString( UpdateStrategy));
			}
			else
			{
				//query to insert the rows
				updateQuery = "INSERT INTO ";
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( m_tgtTable));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( " SELECT "));

				for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
				{
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( m_tempTblName));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( "."));
					updateQuery.append(IUString( "\""));
					updateQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
					updateQuery.append(IUString( "\""));
					if(iKeyfld != m_allLinkedFldNames.length()-1)
						updateQuery.append(IUString(" , "));
				}
				updateQuery.append(IUString( " FROM "));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( m_tempTblName));
				updateQuery.append(IUString( "\""));

				updateQuery.append(IUString(" WHERE ( "));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString(m_tempTblName));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString("."));
				UpdateStrategy = IUString("UpdateStrategy_");
				tempPid.sprint(IUString("%d"),this->ParentID);
				UpdateStrategy.append(tempPid);
				UpdateStrategy.append(IUString(" = '1'"));

				updateQuery.append(IUString( UpdateStrategy));
				updateQuery.append(IUString(" ) and "));
				updateQuery.append(IUString(tempKeyNotInTargetQry));
				updateQuery.append(IUString( " AND "));
				updateQuery.append(IUString(tempFilterDuplicateQry));

				if(m_bCreateBadFile)
				{
					rejectRowQuery.append(IUString(" UNION ( "));
					rejectRowQuery.append(IUString(" ( select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));
					rejectRowQuery.append(IUString(tempGetDuplicateQry));
					rejectRowQuery.append(IUString(" AND \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString(" )) "));
					rejectRowQuery.append(IUString(" UNION ( "));
					rejectRowQuery.append(IUString(" select \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\" where "));

					rejectRowQuery.append(IUString( "\""));
					rejectRowQuery.append(IUString( m_tempTblName));
					rejectRowQuery.append(IUString( "\"."));
					rejectRowQuery.append(IUString(UpdateStrategy));
					rejectRowQuery.append(IUString( " AND "));

					rejectRowQuery.append(IUString(tempKeyInTargetQryUsingIn));
					rejectRowQuery.append(IUString(" )"));
				}

			}
		}
		else
		{
			if(m_bCreateBadFile)
			{
				//in case update is set as None, we will reject all rows which are marked for update.
				UpdateStrategy = IUString("UpdateStrategy_");
				tempPid.sprint(IUString("%d"),this->ParentID);
				UpdateStrategy.append(tempPid);
				UpdateStrategy.append(IUString(" = '1'"));

				rejectRowQuery.append(IUString(" UNION ( "));
				rejectRowQuery.append(IUString(" ( select \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\".\"rowid\" from \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\" where \""));
				rejectRowQuery.append(IUString( m_tempTblName));
				rejectRowQuery.append(IUString( "\"."));
				rejectRowQuery.append(IUString(UpdateStrategy));
				rejectRowQuery.append(IUString(" )) "));
			}
		}
		//incase of datadriven mode if only insert is selected with ignore key constraint checked..we will
		//not load target table from the external table only.
		if( (m_IsInsert && m_ignoreKeyConstraint) && !m_IsDelete && m_IsUpdate==NONE )
		{
			insertQuery=insertColsInTgt;
			insertQuery.append(IUString( " SELECT "));
			for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
			{
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString( externalTable));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString( "."));
				insertQuery.append(IUString( "\""));
				insertQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
				insertQuery.append(IUString( "\""));
				if(iKeyfld != m_allLinkedFldNames.length()-1)
					insertQuery.append(IUString(" , "));
			}
			insertQuery.append(IUString( " FROM "));
			insertQuery.append(IUString( "\""));
			insertQuery.append(IUString( externalTable));
			insertQuery.append(IUString( "\""));

			insertQuery.append(IUString(" WHERE "));
			insertQuery.append(IUString( "\""));
			insertQuery.append(IUString( externalTable));
			insertQuery.append(IUString( "\""));
			insertQuery.append(IUString("."));
			UpdateStrategy= IUString("UpdateStrategy_");
			tempPid.sprint(IUString("%d"),this->ParentID);
			UpdateStrategy.append(tempPid);
			UpdateStrategy.append(IUString(" = '2'"));
			insertQuery.append(IUString( UpdateStrategy));

			rejectRowQuery="";
			loadTempTableQuery="";
		}

		if( (m_IsUpdate==INSERT && m_ignoreKeyConstraint) && !m_IsInsert && !m_IsDelete)
		{
			updateQuery=insertColsInTgt;
			updateQuery.append(IUString( " SELECT "));
			for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
			{
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( externalTable));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( "."));
				updateQuery.append(IUString( "\""));
				updateQuery.append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
				updateQuery.append(IUString( "\""));
				if(iKeyfld != m_allLinkedFldNames.length()-1)
					updateQuery.append(IUString(" , "));
			}
			updateQuery.append(IUString( " FROM "));
			updateQuery.append(IUString( "\""));
			updateQuery.append(IUString( externalTable));
			updateQuery.append(IUString( "\""));

			updateQuery.append(IUString(" WHERE "));
			updateQuery.append(IUString( "\""));
			updateQuery.append(IUString( externalTable));
			updateQuery.append(IUString( "\""));
			updateQuery.append(IUString("."));
			UpdateStrategy= IUString("UpdateStrategy_");
			tempPid.sprint(IUString("%d"),this->ParentID);
			UpdateStrategy.append(tempPid);
			UpdateStrategy.append(IUString(" = '1'"));
			updateQuery.append(IUString( UpdateStrategy));

			rejectRowQuery="";
			loadTempTableQuery="";
		}
		
		if(!rejectRowQuery.isEmpty())
		{
			rejectRowQuery.append(IUString(" )"));
		}
	}

	if(execQueryDropExtTab == ITRUE)
	{
		IBOOLEAN b;
		tempSQLTChar = 	Utils::IUStringToSQLTCHAR(dropExtTab);
		rc = SQLExecDirect(m_hstmt,tempSQLTChar,SQL_NTS);
		DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
		checkRet(rc,dropExtTab,m_hstmt,m_msgCatalog,m_sLogger,b,this->traceLevel);
		m_threadArgs->dropExtTab  = "";
		return IFAILURE;
	}

	m_threadArgs->updateQuery=updateQuery;
	m_threadArgs->insertForUpsertQuery=insertForUpsertQuery;
	m_threadArgs->deleteQuery=deleteQuery;
	m_threadArgs->insertQuery=insertQuery;
	m_threadArgs->loadTempTableQuery=loadTempTableQuery;
	m_threadArgs->getAffMinusAppQry=getAffMinusAppQry;
	m_threadArgs->dropTempTable =  dropTempTable;
	m_threadArgs->dropExtTab = dropExtTab;
	m_threadArgs->rejectRowQuery=rejectRowQuery;
	m_threadArgs->pUtils=m_pUtils;
	m_threadArgs->sLogger=m_sLogger;
	m_threadArgs->msgCatalog=m_msgCatalog;
	m_threadArgs->threadName.sprint("%s_%d_*_%d",NETZZA_WRT_THREAD.getBuffer(),m_nTgtNum+1,m_nPartNum+1);
	m_threadArgs->traceLevel=this->traceLevel;
	m_threadArgs->pipeFullPath=m_fullPath;
	m_threadArgs->deleteForUpsertQuery=deleteForUpsertQuery;
	if(m_b81103Upsert || m_b81103UpsertWithIgnoreKey)
		m_threadArgs->b81103Upsert=ITRUE;

	//Storing the partition driver obejct in to the thread structure. 
	m_threadArgs->pcnWriterObj=this;
	m_threadArgs->hdbc=m_hdbc;

	return ISUCCESS;
}


/*****************************************************************
*Function       : handlingHasDTMStop
*Description    : Function to handle DTM stop/abort
*Input          : None
******************************************************************/

ISTATUS  PCNWriter_Partition::handlingHasDTMStop()
{
	ISTATUS iResult=ISUCCESS;
	if(m_pUtils->hasDTMRequestedStop()==TRUE && m_dtmStopHandled==IFALSE)
	{
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_DTM_STOP_RQST_KILL_PARNT));
		iResult=IFAILURE;
	}	
	else if(m_pUtils->hasDTMRequestedStop()==TRUE)
	{
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_DTM_STOP_RQST_KILL_PARNT));		
		iResult=IFAILURE;
	}
	return iResult;
}

/*****************************************************************
*Function       : freeHandle
*Description    : Function to free all the allocated handle
*Input          : None
******************************************************************/
ISTATUS  PCNWriter_Partition::freeHandle()
{
	ISTATUS iResult = ISUCCESS;
	IINT32  tempMsgCode;
	SQLRETURN  rc;

	try
	{
		if (m_hdbc)
		{
			rc = SQLDisconnect(m_hdbc);
			if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			{
				tempMsgCode = MSG_WRT_DIS_CONN_FAIL;
				throw tempMsgCode;
			}
		}

		if(m_hdbc)
		{
			rc =  SQLFreeHandle(SQL_HANDLE_DBC, m_hdbc);
			if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			{
				tempMsgCode = MSG_WRT_FREE_HANDLE_CONN_FAIL;
				throw tempMsgCode;
			}
		}

		if(m_henv)
		{
			rc = SQLFreeHandle(SQL_HANDLE_ENV, m_henv);
			if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			{
				tempMsgCode = MSG_WRT_FREE_HANDLE_ENV_FAIL;
				throw tempMsgCode;
			}
		}

		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_ALL_RES_HANDLE_FREE));
	}
	catch(IINT32 ErrMsgCode)
    {
        if(this->traceLevel >= SLogger::ETrcLvlTerse)
		{
            m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(ErrMsgCode));
        }
        iResult = IFAILURE;
    }
    catch (...)
    {
        iResult = IFAILURE;
    }
	return iResult;
}


/*******************************************************************
* Name        : createLocale
* Description : create the locale
* Returns     : ISUCCESS or IFAILURE
*******************************************************************/
ISTATUS PCNWriter_Partition::createLocale()
{
	ISTATUS iResult     = ISUCCESS;

	try
	{
		IUtilsCommon *pUtils = IUtilsCommon::getInstance ();
		m_pLocale_Latin9 = pUtils->createLocale(LATIN9_CODEPAGE_ID);
		m_pLocale_UTF8 = pUtils->createLocale(UTF8_CODEPAGE_ID);
		m_pLocale_Latin1 = pUtils->createLocale(LATIN1_CODEPAGE_ID);
		if (m_pLocale_Latin9 == NULL || m_pLocale_UTF8 ==NULL || m_pLocale_Latin1==NULL)
			throw IFAILURE;
	}
	catch(...)
	{
		iResult = IFAILURE;
	}
	return iResult;
}


/*******************************************************************
* Name        : destroyLocale
* Description : destroy the locale
* Returns     : ISUCCESS or IFAILURE
*******************************************************************/
ISTATUS PCNWriter_Partition::destroyLocale()
{
	ISTATUS iResult     = ISUCCESS;
	try
	{
		IUtilsCommon *pUtils = IUtilsCommon::getInstance ();
		if( m_pLocale_Latin9 )
			pUtils->destroyLocale(this->m_pLocale_Latin9);
		if( m_pLocale_UTF8 )
			pUtils->destroyLocale(this->m_pLocale_UTF8);
		if(m_pLocale_Latin1)
			pUtils->destroyLocale(this->m_pLocale_Latin1);
	}
	catch(...)
	{
		iResult = IFAILURE;
	}
	return iResult;

}
void PCNWriter_Partition::addDistributionKey(IUString *createTempTableQuery, IVector<const IUString> distKeyFldNames, IVector<const IUString> allLinkedFldNames)
{
	if(distKeyFldNames.length() > 0)
	{
		IBOOLEAN allDistKeyLinked=ITRUE;
		IINT32 lenDistKey=distKeyFldNames.length();		

		for(IINT32 index=0;index<lenDistKey;index++)
		{
			if(!allLinkedFldNames.contains(distKeyFldNames.at(index)))
			{
				allDistKeyLinked=IFALSE;
				break;
			}
		}

		if(allDistKeyLinked==ITRUE)
		{
			createTempTableQuery->append(IUString( " DISTRIBUTE ON ("));
			for(IINT32 iKeyfld=0; iKeyfld<lenDistKey; iKeyfld++)
			{
				createTempTableQuery->append(IUString( "\""));
				createTempTableQuery->append(IUString( (distKeyFldNames.at(iKeyfld))));
				createTempTableQuery->append(IUString( "\""));
				if(iKeyfld != distKeyFldNames.length()-1)
					createTempTableQuery->append(IUString( " ,"));
			}
			createTempTableQuery->append(IUString( ")"));
		}
	}
}
void PCNWriter_Partition::makeQueriesFor81103Upsert(IUString *deleteForUpsertQuery,IUString *updateQuery,IUString insertColsInTgt,
													IUString tempFilterDuplicateQry)
{
	IINT32 iKeyfld;
	//preparing delete query for update else insert operation
	deleteForUpsertQuery->append(IUString( "DELETE FROM "));
	deleteForUpsertQuery->append(IUString( "\""));
	deleteForUpsertQuery->append(IUString( m_tgtTable));
	deleteForUpsertQuery->append(IUString( "\""));
	deleteForUpsertQuery->append(IUString( " WHERE EXISTS (SELECT * FROM "));
	deleteForUpsertQuery->append(IUString( "\""));
	deleteForUpsertQuery->append(IUString( m_tempTblName));
	deleteForUpsertQuery->append(IUString( "\""));
	deleteForUpsertQuery->append(IUString( " WHERE "));
	for(iKeyfld=0; iKeyfld<m_keyFldNames.length(); iKeyfld++)
	{
		deleteForUpsertQuery->append(IUString( "\""));    
		deleteForUpsertQuery->append(IUString( m_tgtTable));
		deleteForUpsertQuery->append(IUString( "\""));
		deleteForUpsertQuery->append(IUString( "."));
		deleteForUpsertQuery->append(IUString( "\""));
		deleteForUpsertQuery->append(IUString( (m_keyFldNames.at(iKeyfld))));
		deleteForUpsertQuery->append(IUString( "\""));
		deleteForUpsertQuery->append(IUString( "="));
		deleteForUpsertQuery->append(IUString( "\""));
		deleteForUpsertQuery->append(IUString( m_tempTblName));
		deleteForUpsertQuery->append(IUString( "\""));
		deleteForUpsertQuery->append(IUString( "."));
		deleteForUpsertQuery->append(IUString( "\""));
		deleteForUpsertQuery->append(IUString( (m_keyFldNames.at(iKeyfld))));
		deleteForUpsertQuery->append(IUString( "\""));
		if(iKeyfld != m_keyFldNames.length()-1)
			deleteForUpsertQuery->append(IUString(" and "));
	}
	if(this->m_rowsAs == 3)
	{
		//in case of datadriven mode we are appending update strategy
		deleteForUpsertQuery->append(IUString(" AND "));
		deleteForUpsertQuery->append(IUString( "\""));
		deleteForUpsertQuery->append(IUString(m_tempTblName));
		deleteForUpsertQuery->append(IUString( "\""));
		deleteForUpsertQuery->append(IUString("."));
		deleteForUpsertQuery->append(IUString( UpdateStrategy));
	}
	deleteForUpsertQuery->append(IUString( ")"));
			

	//preparing insert query
	updateQuery->append(insertColsInTgt);
	updateQuery->append(IUString( " SELECT "));

	for(iKeyfld=0; iKeyfld<m_allLinkedFldNames.length(); iKeyfld++)
	{
		updateQuery->append(IUString( "\""));
		updateQuery->append(IUString( m_tempTblName));
		updateQuery->append(IUString( "\""));
		updateQuery->append(IUString( "."));
		updateQuery->append(IUString( "\""));
		updateQuery->append(IUString( (m_allLinkedFldNames.at(iKeyfld))));
		updateQuery->append(IUString( "\""));
		if(iKeyfld != m_allLinkedFldNames.length()-1)
			updateQuery->append(IUString(" , "));
	}
	updateQuery->append(IUString( " FROM "));
	updateQuery->append(IUString( "\""));
	updateQuery->append(IUString( m_tempTblName));
	updateQuery->append(IUString( "\""));
	if(this->m_rowsAs == 3)
	{
		//incase of data driven mode append update strategy
		updateQuery->append(IUString(" WHERE "));	
		if(!m_b81103UpsertWithIgnoreKey || !m_ignoreKeyConstraint)
		{
			//in case user wants mix behavior of 81103 and pre 81103 we are inserting duplicates if user has
			//added upsert81103FunctionalityWithIgnoreKey flag in integration service and ignore key checked.
			updateQuery->append(IUString(tempFilterDuplicateQry));
			updateQuery->append(IUString(" AND "));
		}
		updateQuery->append(IUString( "\""));
		updateQuery->append(IUString(m_tempTblName));
		updateQuery->append(IUString( "\""));
		updateQuery->append(IUString("."));
		updateQuery->append(IUString( UpdateStrategy));

	}
	else if(!m_b81103UpsertWithIgnoreKey || !m_ignoreKeyConstraint)
	{
		//in case user wants mix behavior of 81103 and pre 81103 we are inserting duplicates if user has added
		//upsert81103FunctionalityWithIgnoreKey flag in integration service and ignore key checked.
		updateQuery->append(IUString(" WHERE "));	
		updateQuery->append(IUString(tempFilterDuplicateQry));
	}
}

PCNWriter_Group* PCNWriter_Partition::getParentGroupDriver()
{
	return this->m_parent_groupDriver;
}


void PCNWriter_Partition::calMaxRowLength()
{
	m_rowMaxLength=0;
	IUINT32 l_numProjectedCols = m_inputFieldList.length();
	IINT32 nMaxMBCSBytesPerChar=m_pLocale_UTF8->getMaxMBCSBytesPerChar();
	for(IUINT32 colNum=0;colNum<l_numProjectedCols;colNum++)
	{
		//We need to deal each type of column seprately, and need to allocate buffer accordingly.
		//Also if we can get previous transformation precision thn it would be ideal.
		if(cdatatypes[colNum].dataType==eCTYPE_SHORT || cdatatypes[colNum].dataType== eCTYPE_INT32
			||cdatatypes[colNum].dataType== eCTYPE_LONG || cdatatypes[colNum].dataType== eCTYPE_LONG64 )
		{
			m_rowMaxLength+=sqldatatypes[colNum].prec+1; //1 for negative sign
		}
		else if(cdatatypes[colNum].dataType==eCTYPE_FLOAT || cdatatypes[colNum].dataType==eCTYPE_DOUBLE)
		{
			m_rowMaxLength+=sqldatatypes[colNum].prec+1+1;  //1 for negative sign and 1 for decimal point.
		}
		else if(cdatatypes[colNum].dataType==eCTYPE_CHAR)
		{
			m_rowMaxLength+=sqldatatypes[colNum].prec;
			if(m_InsertEscapeCHAR)
			{
				m_rowMaxLength+=sqldatatypes[colNum].prec; //escape character for each character.
			}
		}
		else if(cdatatypes[colNum].dataType==eCTYPE_UNICHAR)
		{
			if(sqldatatypes[colNum].isColNchar)
			{
				m_rowMaxLength+=nMaxMBCSBytesPerChar*sqldatatypes[colNum].prec;
			}
			else
			{
				m_rowMaxLength+=sqldatatypes[colNum].prec;
			}

			if(m_InsertEscapeCHAR)
			{
				m_rowMaxLength+=sqldatatypes[colNum].prec; //escape character for each character.
			}
		}
		else if(cdatatypes[colNum].dataType==eCTYPE_TIME)
		{
			m_rowMaxLength+=sqldatatypes[colNum].prec;
		}
		else if(cdatatypes[colNum].dataType==eCTYPE_DECIMAL18_FIXED)
		{
			m_rowMaxLength+=18+2;		//2 for minus sign
		}
		else if(cdatatypes[colNum].dataType==eCTYPE_DECIMAL28_FIXED)
		{
			m_rowMaxLength+=28+2;		//2 for minus sign
		}
		else
		{
			m_rowMaxLength+=sqldatatypes[colNum].prec;
		}

		//If update strategy
		if(this->m_rowsAs == 3)
		{
			m_rowMaxLength+=1+1;		//1 for collumn delimiter and 1 for update strategy value.
		}
	}
	//Add Collumn and Row delimiter
	m_rowMaxLength+=l_numProjectedCols+1; //l_numProjectedCols-1 collumn delimiter and 2 row delimiter.
	if(m_quotedValue == PCN_Constants::PCN_DOUBLE_QUOAT || m_quotedValue == PCN_Constants::PCN_SINGLE_QUOAT)
	{
		m_rowMaxLength+=(2*l_numProjectedCols)*2; //Quote will be used for eac0h collumn in start and last.
	}
	//This is just to make sure we dont cause memory corrution.
	m_rowMaxLength+=DEFAULT_BUFFER;
}


ISTATUS PCNWriter_Partition::childThreadHandling()
{
	/*
	We are opening closing the pipe at the start of deinit/commit, with our checking for retStatus. This is done 
	to simulate the kind of as we are doing at the end of last execute call.	
	*/
	if(m_bChildThreadHandlingCalled)
	{
		return ISUCCESS;
	}
	else
	{
		m_bChildThreadHandlingCalled=ITRUE;
	}

	ISTATUS retStat=ISUCCESS;
	if(m_bIsChildThreadSpawned)
	{
		if(m_bIsPipeOpened==IFALSE)
		{
			if(IFAILURE==openWRPipe())
				retStat=IFAILURE;			
			if(IFAILURE==closeWRPipe())
				retStat=IFAILURE;
			m_iThreadMgr.join(m_hThread);
#ifdef UNIX
			removePipe();
#endif
		}
		else if(m_bIsPipeClosed==IFALSE)
		{
			if(IFAILURE==closeWRPipe())
				retStat=IFAILURE;
			m_iThreadMgr.join(m_hThread);
#ifdef UNIX
			removePipe();
#endif
		}
		else
		{
			m_iThreadMgr.join(m_hThread);
#ifdef UNIX
			removePipe();
#endif
		}
	}
	else
	{
		if(m_bIsPipeCreated)
		{
#ifdef UNIX
			removePipe();
#endif
		}
	}
	return retStat;
}

IBOOLEAN PCNWriter_Partition::isAnyQueryFailed()
{
	return m_threadArgs->bCheckAnyfailedQuery;
}

IBOOLEAN PCNWriter_Partition::isNoDataCase()
{
	if(m_no_data_in_file == 0)
		return ITRUE;
	else
		return IFALSE;
}

IBOOLEAN PCNWriter_Partition::isLoadInToTgt()
{
	return m_threadArgs->b_isLoadInToTgt;
}

void PCNWriter_Partition::manageDropExtTempTable()
{
	/*
	We need to make sure before rollbacking that child thread has been done. We already have join at the
	start of deinit. 
	since this is an error case and we dont support any data commit in this case, so first we 
	are rolling back.
	After this we are dropping the table and issue a commit. The reason of commit is that Netezza expects a 
	commit for DDL statements too. 
	we are not firing a commit and dropping temp table in case of single partiton; the reason being is
	that in this case we dont commit for creation of temp table too, so table would not be there after
	end of the transection. 
	*/

	PCNWriter_Group *parentGrp=this->getParentGroupDriver();
	if(parentGrp->getPartitionCount() >1 && parentGrp->isTempTableCreated())
	{
		SQLRETURN rc = SQLEndTran(SQL_HANDLE_DBC,m_hdbc,SQL_ROLLBACK);
		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			// Failed to rollback all the DDL and DML statements
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_ROLLBCK_TRANS_FAIL));
		}
		// rollback successful
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_ROLLBCK_SUCC));


		//We are checking for loadtemp table query too because if this query is empty then creation of external 
		// table would be rolledback as part of rollback called.
		if(!m_threadArgs->dropExtTab.isEmpty() && !m_threadArgs->loadTempTableQuery.isEmpty())
		{
			SQLTCHAR* tempSQLTChar= Utils::IUStringToSQLTCHAR(m_threadArgs->dropExtTab);
			rc = SQLExecDirect(m_hstmt,tempSQLTChar,SQL_NTS);
			checkRet(rc,m_threadArgs->dropExtTab,m_hstmt,m_msgCatalog,m_sLogger,m_threadArgs->bCheckAnyfailedQuery,traceLevel);
			DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
		}

		if(m_threadArgs->b_isLastPartition && IFALSE==m_threadArgs->dropTempTable.isEmpty())
		{
			SQLTCHAR * tempSQLTChar = Utils::IUStringToSQLTCHAR(m_threadArgs->dropTempTable);
			rc = SQLExecDirect(m_hstmt,tempSQLTChar,SQL_NTS);
			checkRet(rc,m_threadArgs->dropTempTable,m_hstmt,m_msgCatalog,m_sLogger,m_threadArgs->bCheckAnyfailedQuery,traceLevel);			
			DELETEPTRARR <SQLTCHAR>(tempSQLTChar);
		}


		rc = SQLEndTran(SQL_HANDLE_DBC,m_hdbc,SQL_COMMIT);
		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			//Failed to commit drop the temp table
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_COMMIT_TRANS_FAIL_DRP_TBL));				
		}
		// Succesful to commit drop temp table
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_COMMIT_SUCC_DRP_TBL));

	}								
}

IBOOLEAN PCNWriter_Partition::bIsPipeOpened()
{
	return m_bIsPipeOpened;
}

IUString PCNWriter_Partition::getTargetTblName()
{
	return m_tgtTable;
}

IBOOLEAN PCNWriter_Partition::isQueryHaveWhereClause()
{
	if(m_IsDelete || m_IsUpdate==UPSERT || m_IsUpdate==UPDATE || 
		((m_IsInsert || m_IsUpdate==INSERT) && m_ignoreKeyConstraint==0))
	{
		return ITRUE;
	}
	else
		return IFALSE;
}

SQLHDBC PCNWriter_Partition::getDBHandle()
{
	return m_hdbc;
}

void PCNWriter_Partition::printSessionAttributes()
{
	for ( IINT32 i = 0; i < writeAttrArrayLength; i++ )
	{
		IUString attrname(PCNwriterAttributeDefinitions[i]);
		IUString msgPrint;
		bool toPrint = false;
		switch(i)
		{
			case 0: if ( 0 != m_fullPath.compare(PCN_Constants::PCN_EMPTY) )
					{//Pipe Directory Path
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING) ,\
						attrname.getBuffer(), m_fullPath.getBuffer());
						toPrint = true;
					}
					break;
			case 1: if ( 0 != m_errorLogDir.compare(PCN_Constants::PCN_EMPTY) )
					{//Error Log Directory Name
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_errorLogDir.getBuffer());
						toPrint = true;
					}
					break;
			case 2: //Insert
					msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_INT), \
					attrname.getBuffer(), m_IsInsert);
					toPrint = true;
					break;
			case 3: //Delete
					msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_INT), \
					attrname.getBuffer(), m_IsDelete);
					toPrint = true;
					break;
			case 4: //Update
				{
					IUString iuUpdateStrategy = PCN_Constants::PCN_EMPTY;
					if ( UPDATE ==  m_IsUpdate )
						iuUpdateStrategy = PCN_UPDATE_AS_UPDATE;
					else if ( UPSERT ==  m_IsUpdate )
						iuUpdateStrategy = PCN_UPDATE_ELSE_INSERT;
					else if ( INSERT ==  m_IsUpdate )
						iuUpdateStrategy = m_msgCatalog->getMessage(MSG_WRT_PCN_UPDATE_AS_INSERT);
					else
						iuUpdateStrategy = PCN_NONE;
					msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
					attrname.getBuffer(), iuUpdateStrategy.getBuffer());
					toPrint = true;
				}
					break;
			case 5: //Truncate Target Table Option
				{
					IINT32 truncTab=0;
					if(m_pSessExtn->getAttributeInt(WRT_PROP_TRUN_TRG_TABLE_OPTION,truncTab) == IFAILURE)
					{
						truncTab= 0;
					}
					msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_INT), \
					attrname.getBuffer(), truncTab);
					toPrint = true;
				}
					break;
			case 6: //Delimiter
					if ( 0 != m_delimiter.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_delimiter.getBuffer());
						toPrint = true;
					}
					break;
			case 7: //Null Value
					if ( 0 != m_nullValue.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_nullValue.getBuffer());
						toPrint = true;
					}
					break;
			case 8: //Escape character
					if ( 0 != m_escapeChar.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_escapeChar.getBuffer());
						toPrint = true;
					}
					break;
			case 9: //Quoted Value
					if ( 0 != m_quoted.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_quoted.getBuffer());
						toPrint = true;
					}
					break;
			case 10://Ignore Key Constraint 
					msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_INT), \
					attrname.getBuffer(), m_ignoreKeyConstraint);
					toPrint = true;
					break;
			case 11://Duplicate Row Handling 
					if ( 0 != m_duplicateRowMechanism.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_duplicateRowMechanism.getBuffer());
						toPrint = true;
					}
					break;
			case 12://Bad File Name 
					if ( 0 != m_badFileName.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_badFileName.getBuffer());
						toPrint = true;
					}
					break;
			case 13://Socket Buffer Size 
					if ( 0 != m_socketBuffSize.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_socketBuffSize.getBuffer());
						toPrint = true;
					}
					break;
			case 14://Control Characters 
					if ( 0 != m_ctrlchars.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_ctrlchars.getBuffer());
						toPrint = true;
					}
					break;
			case 15: toPrint = false;
					break;
			case 16://CRINSTRING 
					if ( 0 != m_crlnstring.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_crlnstring.getBuffer());
						toPrint = true;
					}
					break;
			case 17://Table Name Prefix 
					if ( 0 != m_OriginalSchemaName.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_OriginalSchemaName.getBuffer());
						toPrint = true;
					}
					break;
			case 18://Target Table Name 
					if ( 0 != m_tgtTable.compare(PCN_Constants::PCN_EMPTY) )
					{
						msgPrint.sprint(m_msgCatalog->getMessage(MSG_WRT_PRINT_SESSION_ATTRIBUTE_STRING), \
						attrname.getBuffer(), m_tgtTable.getBuffer());
						toPrint = true;
					}
					break;
			default: break;
		}
		if ( toPrint )
		{
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlNormal,msgPrint.getBuffer());
		}
	}
}

//TODO: create some more sub functions to reduce the functions length.
ISTATUS PCNWriter_Partition::getSessionExtensionAttributes()
{
	IINT32 prop_insert, prop_delete, prop_update, prop_trun_trg_table_option, prop_delimiter,
		prop_null_val, prop_escape_char, prop_quoted_val, prop_ignore_key_constraint, 
		prop_dup_row_handle_mech, prop_bad_file_name, prop_socket_buff_size, prop_pipe_dir_path, 
		prop_err_log_dir_name,prop_control_char,prop_crinstring;

	prop_insert=WRT_PROP_INSERT;
	prop_delete=WRT_PROP_DELETE;
	prop_update=WRT_PROP_UPDATE;
	prop_trun_trg_table_option=WRT_PROP_TRUN_TRG_TABLE_OPTION;
	prop_delimiter=WRT_PROP_DELIMITER;
	prop_null_val=WRT_PROP_NULL_VAL;
	prop_escape_char=WRT_PROP_ESCAPE_CHAR;
	prop_quoted_val=WRT_PROP_QUOTED_VAL;
	prop_ignore_key_constraint=WRT_PROP_IGNORE_KEY_CONSTRAINT;
	prop_dup_row_handle_mech=WRT_PROP_DUP_ROW_HANDLE_MECH;
	prop_bad_file_name=WRT_PROP_BAD_FILE_NAME;
	prop_socket_buff_size=WRT_PROP_SOCKET_BUFF_SIZE;
	prop_pipe_dir_path=WRT_PROP_PIPE_DIR_PATH;
	prop_err_log_dir_name=WRT_PROP_ERR_LOG_DIR_NAME;
	prop_control_char=WRT_PROP_CONTROL_CHAR;
	prop_crinstring=WRT_PROP_CRINSTRING;

	IUString delimiter = PCN_Constants::PCN_PIPE;
	// To get the delimiter from session attribute
	if(m_pSessExtn->getAttribute(prop_delimiter,delimiter,m_nPartNum) == IFAILURE)
	{
		delimiter = PCN_Constants::PCN_PIPE;
	}

	if(m_pUtils->expandString(delimiter,m_delimiter,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
	{
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),delimiter.getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlTerse)
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		return IFAILURE;
	}

	IUString ctrlchars;
	// To get the ctrlchars from session attribute
	if(m_pSessExtn->getAttribute(prop_control_char,ctrlchars,m_nPartNum) == IFAILURE)
	{
		ctrlchars = PCN_TRUE;
	}

	m_ctrlchars = ctrlchars;

	IUString crlnstring;
	// To get the crlnstring from session attribute
	if(m_pSessExtn->getAttribute(prop_crinstring,crlnstring,m_nPartNum) == IFAILURE)
	{
		crlnstring = PCN_TRUE;
	}

	m_crlnstring = crlnstring;

	// To get the nullvalue from session attribute
	IUString nullValue;
	if(m_pSessExtn->getAttribute(prop_null_val,nullValue,m_nPartNum) == IFAILURE)
	{
		nullValue = PCN_NULL;

	}

	if(m_pUtils->expandString(nullValue,m_nullValue,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
	{
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),nullValue.getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlTerse)
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		return IFAILURE;
	}

	// To get the escapeChar from session attribute
	IUString escapeChar = "";

	if(m_pSessExtn->getAttribute(prop_escape_char,escapeChar,m_nPartNum) == IFAILURE)
	{
		escapeChar = "";
	}

	if(m_pUtils->expandString(escapeChar,m_escapeChar,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
	{
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),escapeChar.getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlTerse)
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		return IFAILURE;
	}		


	IUString socketBuffSize=PCN_Constants::PCN_EMPTY;
	if(m_pSessExtn->getAttribute(prop_socket_buff_size,socketBuffSize) == IFAILURE)
	{
		socketBuffSize = IUString("8388608");
	}

	if(m_pUtils->expandString(socketBuffSize,m_socketBuffSize,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDefault)==IFAILURE)
	{
		tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),socketBuffSize.getBuffer());
		if(this->traceLevel >= SLogger::ETrcLvlTerse)
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
		return IFAILURE;
	}		

	// To get the quoted value from session attribute
	IUString quotedValue;

	if(m_pSessExtn->getAttribute(prop_quoted_val,quotedValue,m_nPartNum) == IFAILURE)
	{
		quotedValue = PCN_NO;
	}
	m_quotedValue = quotedValue;
	m_quoted = quotedValue;

	if(m_pSessExtn->getAttributeInt(prop_ignore_key_constraint,m_ignoreKeyConstraint,m_nPartNum) == IFAILURE)
	{
		m_ignoreKeyConstraint = 0;
	}

	if( m_ignoreKeyConstraint)
	{
		if(this->traceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_KEY_CONS_MISS_TRG));
	}

	//Fix for CR 176528, 107699
	if(m_pSessExtn->getAttribute(prop_dup_row_handle_mech,m_duplicateRowMechanism,m_nPartNum) == IFAILURE)
	{
		m_duplicateRowMechanism = PCN_FIRST;
	}

	if(m_duplicateRowMechanism == PCN_FIRST)
		m_cduplicateRowMechanism = PCN_MIN;
	else if(m_duplicateRowMechanism == PCN_LAST)
		m_cduplicateRowMechanism = PCN_MAX;

		//Get Insert attribute
		if(m_pSessExtn->getAttributeInt(prop_insert,m_IsInsert,m_nPartNum) == IFAILURE)
		{
			m_IsInsert = 0;
		}
		//Get Delete attribute
		if(m_pSessExtn->getAttributeInt(prop_delete,m_IsDelete,m_nPartNum) == IFAILURE)
		{
			m_IsDelete = 0;
		}
		//Get Update attribute
		
		if(m_pSessExtn->getAttribute(prop_update,l_updStrat,m_nPartNum) != IFAILURE)
		{
			//Checking Update
			if ( l_updStrat == PCN_UPDATE_AS_UPDATE)
				m_IsUpdate = UPDATE;

			if (l_updStrat == PCN_UPDATE_ELSE_INSERT)
				m_IsUpdate = UPSERT;

			//rjain Update as insert support

			if (l_updStrat == m_msgCatalog->getMessage(MSG_WRT_PCN_UPDATE_AS_INSERT))
				m_IsUpdate = INSERT;

			if (l_updStrat == PCN_NONE)
				m_IsUpdate = NONE;
		}
		// Fix for CR # 176520
		
		//In Netezza bulk Writer, if user has given input in Escape character then Escape character will be 
		//taken in to consideration, It would not have Add Escape character unlike Netezza Writer.

		m_InsertEscapeCHAR=0;
		if(!m_escapeChar.isEmpty())
		{
			m_InsertEscapeCHAR=1;
			m_cescapeChar = Utils::IUStringToChar(m_escapeChar);
		}
	
		IUString strPipePath=PCN_Constants::PCN_EMPTY;
		//Fix for CR133218

		// To get the Pipe directory path from session attribute
		if(m_pSessExtn->getAttribute(prop_pipe_dir_path,strPipePath,m_nPartNum) == IFAILURE)
		{
			// Failed to get the session attribute
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgWarning, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_CMN_GET_PIPE_DIR_PATH_FAIL));
		}
		//end fix

		// make full path
		#ifndef WIN32
		//Fix for CR133218			  
		//Create pipes in $PMTempDir dir by default
		if(strPipePath.isEmpty())
			strPipePath="$PMTempDir";

		if(m_pUtils->expandString(strPipePath,m_fullPath,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDirFull)==IFAILURE)
		{
			tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),strPipePath.getBuffer());
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
			return IFAILURE;
		}				
		#else
			//Fix for CR133218
			//Added if statement for CR133218
			m_fullPath= IUString("\\\\.\\pipe\\");
			if(strPipePath.isEmpty())
				m_fullPath.append(IUString("PCN_Pipe"));
			else
			{
				m_fullPath.append(strPipePath);
				m_fullPath.append(IUString("\\PCN_Pipe"));
			}
		#endif


		IUString    errorFile;
		// To get the error log director from session attribute
		if(m_pSessExtn->getAttribute(prop_err_log_dir_name,errorFile,m_nPartNum) == IFAILURE)
		{
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_GET_ERR_DIR_SESS_ATTR_FAIL));
			}
			return IFAILURE;
		}

		m_errorLogDir=PCN_Constants::PCN_EMPTY;
		if(!errorFile.isEmpty())
		{
			if(m_pUtils->expandString(errorFile,m_errorLogDir,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDirFull)==IFAILURE)
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),errorFile.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
				return IFAILURE;
			}
		}

		//get Bad file name, we wll parse the user entry in this attribute and wll take the file name only.
		//this badfile will always be created in the bad file directory.
		IUString badfileNameTemp;
		m_badFileName=PCN_Constants::PCN_EMPTY;
		m_bCreateBadFile=IFALSE;

		if(m_pSessExtn->getAttribute(prop_bad_file_name,badfileNameTemp,m_nPartNum) == IFAILURE)
		{		
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_GET_BAD_FILE_NAME_ERR));
			}
			return IFAILURE;
		}

		if(!badfileNameTemp.isEmpty())
		{
			if(m_pUtils->expandString(badfileNameTemp,m_badFileName,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDirFull)==IFAILURE)
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),badfileNameTemp.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
				return IFAILURE;
			}
			m_bCreateBadFile=ITRUE;
		}


		if(m_bCreateBadFile)
		{
			size_t currPos, newPos,lenStr;
			IUString badFile=PCN_Constants::PCN_EMPTY;
			IUNICHAR c1;
			currPos=0;newPos=0;

#ifndef UNIX
			c1='\\';
#else
			c1='/';
#endif

			do
			{
				newPos=m_badFileName.find(c1,currPos);
				if(newPos!=-1)
					currPos=newPos+1;
			}while(newPos!=-1);

			lenStr=m_badFileName.getLength();

			if(currPos==lenStr)
			{
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_INVALID_BAD_FILE_NAME));
				return IFAILURE;
			}
			badfileNameTemp=PCN_Constants::PCN_EMPTY;
			badfileNameTemp.append(IUString("$PMBadFileDir/"));
			badfileNameTemp.append(m_badFileName.right(lenStr-currPos));


			if(m_pUtils->expandString(badfileNameTemp,m_badFileName,IUtilsServer::EVarParamSession,IUtilsServer::EVarExpandDirFull)==IFAILURE)
			{
				tempVariableChar.sprint(m_msgCatalog->getMessage(MSG_CMN_EXPAND_FAIL),badfileNameTemp.getBuffer());
				if(this->traceLevel >= SLogger::ETrcLvlTerse)
					m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,IUString(tempVariableChar).getBuffer());
				return IFAILURE;
			}
		}
		return ISUCCESS;
}
