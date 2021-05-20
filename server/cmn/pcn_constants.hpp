#pragma once
#ifndef __NET_CONSTANTS_INCLUDED__
#define  __NET_CONSTANTS_INCLUDED__

#ifdef WIN32
#ifdef PCNREADER_EXPORTS
#include "../Reader/stdafx.hpp"
#else
#include "../Writer/stdafx.hpp"
#endif
#endif

namespace PCN_Constants
{
	const IUINT32 VENDORID    = 1;   //vender Id from XML registration file
	const IUINT32 MEDOMAINID = 453000; //Metadata domain Id from XML
	const IINT32 CODE_ODBC_ERROR = 19999;
	const IINT32 OLD_NETEZZA_EXTENSION_SUBTYPE=953000;

	const IUString PCN_EMPTY				= IUString("");
	const IUString PCN_PIPE					= IUString("|");
	const IUString PCN_DOUBLE_QUOAT			= IUString("\"");
	const IUString PCN_SINGLE_QUOAT			= IUString("'");
}
#endif


static const IUString NETZZA_RDR_THREAD =IUString ("NZ_RDR");
static const IUString NETZZA_WRT_THREAD =IUString ("NZ_WRT");
static const char *UPSERT_FLAG = "upsert81103Functionality";
static const char *UPSERT_WITH_IGNORE_KEY_FLAG = "upsert81103FunctionalityWithIgnoreKey";
static const char *DELIMITER_FLAG = "delimiter81103Functionality";

static const char *TREAT_NODATA_EMPTY = "treatNoDataAsEmptyString";
static const char *SYNC_NETEZZA_NULL_WITH_PCNULL = "syncNetezzaNullWithPCNull";

//custom variable to set timeout for logging in.
static const char *LOGIN_TIMEOUT_FLAG = "netezzaLoginTimeout"; 
static const char *IGNORE_APPLIED_ROWS_FLAG = "ignoreAppliedRowsCountForNetezza";

/*
NPS supports different datatypes char/varchar for storing latin characters and nchar/nvarchar to 
store Unicode characters. The data stored in char/varchar columns is encoded in latin9 and that 
stored in nchar/nvarchar is encoded in utf-8 *all the time*. And in case of working with external 
loaders, netezza odbc driver *does not do any conversion* of the byte stream written into the pipe 
by an application and just dumps the byte stream into the backend storage. So, it is application’s 
responsibility to ensure that the byte stream is properly encoded while writing to the pipe and is 
interpreted properly when reading off the pipe, *latin9 for char/varchar and utf-8 for nchar/nvarchar*.

If Integration service is running in ASCII mode, then powercenter pipe line doesnt support UTF8 data for
nchar/nvarchar column, so in this case We will use Latin1 for nchar/nvarchar column, data might get corrupted
For char,varchar column data will be converted to Latin1(from latin9) so there are also chances of data corruption.
in this case.
*/
static const IINT32 LATIN9_CODEPAGE_ID=201;
static const IINT32 UTF8_CODEPAGE_ID=106;
static const IINT32 LATIN1_CODEPAGE_ID=2252;

//Connection Attributes
#define		PLG_CONN_DATABASENAME			"Database"
#define		PLG_CONN_SCHEMANAME				"Schemaname"
#define		PLG_CONN_SERVERNAME				"Servername"
#define		PLG_CONN_PORT					"Port"
#define		PLG_CONN_DRIVERNAME				"Driver"
#define		PLG_CONN_ADDITIONAL_CONFIGS		"Runtime Additional Connection Configuration"
#define		PLG_CONN_DSNNAME				"DSN Name"

#define MMD_EXTN_READER_ATTR_LIST	"Reader Session Attributes"
#define MMD_EXTN_WRITER_ATTR_LIST	"Writer Session Attributes"
#define DATE_FMT_YMD "YYYY-MM-DD"
#define DATE_FMT_HMS "HH24:MI:SS.US"
#define DATE_FMT_YMD_HMS "YYYY-MM-DD HH24:MI:SS.US"

static const IUString SELECT_DISTINCT="Select Distinct";
static const IUString SQL_QUERY="SQL Query";
static const IUString SOURCE_FILTER="Source Filter";
static const IUString USER_DEF_JOIN="User Defined Join";
static const IUString NUMBER_OF_SORTED_PORTS="Number of Sorted Ports";
static const IUString PCN_NUMERIC="numeric";
static const IUString PCN_DECIMAL="decimal";
static const IUString PCN_BIGINT="bigint";
static const IUString PCN_INTEGER="integer";
static const IUString PCN_SMALL_INT="smallint";
static const IUString PCN_BYTE_INT="byteint";
static const IUString PCN_VARCHAR="varchar";
static const IUString PCN_CHAR="char";
static const IUString PCN_NCHAR="nchar";
static const IUString PCN_NVARCHAR="nvarchar";
static const IUString PCN_DATE="date";
static const IUString PCN_TIME="time";
static const IUString PCN_TIMESTAMP="timestamp";
static const IUString PCN_TIMETZ="timetz";
static const IUString PCN_TRUE="TRUE";
static const IUString PCN_NULL="NULL";
static const IUString PCN_NO="NO";
static const IUString PCN_DOUBLE="DOUBLE";
static const IUString PCN_SINGLE="SINGLE";
static const IUString PCN_FIRST="FIRST";
static const IUString PCN_LAST="LAST";
static const IUString PCN_MIN="min";
static const IUString PCN_MAX="max";
static const IUString PCN_YES="YES";
static const IUString PCN_UPDATE_AS_UPDATE="Update as Update";
static const IUString PCN_UPDATE_ELSE_INSERT="Update else Insert";
static const IUString PCN_UPDATE_AS_INSERT="Update as Insert";
static const IUString PCN_NONE="None";
static const IUString PCN_TARGET_TABLE_NAME="Target Table Name";
//BEGIN New Feature for Table name and prefix override Ayan
static const IUString PCN_TARGET_SCHEMA_NAME="Table Name Prefix";
static const IUString PCN_SOURCE_SCHEMA_NAME="Owner Name";
static const IUString PCN_SOURCE_TABLE_NAME="Source Table Name";
//END New Feature for Table name and prefix override Ayan


//Attribute id's

#define RDR_PROP_PIPE_DIR_PATH 1
#define RDR_PROP_DELIMITER 2
#define RDR_PROP_NULL_VAL 3
#define RDR_PROP_ESCAPE_CHAR 4
#define RDR_PROP_SOCKET_BUFF_SIZE 5
#define RDR_PROP_SQL_QUERY_OVERRIDE 6
#define RDR_PROP_OWNER_NAME 7
#define RDR_PROP_SOURCE_TABLE_NAME 8
#define RDR_PROP_PRE_SQL 9
#define RDR_PROP_POST_SQL 10

#define WRT_PROP_PIPE_DIR_PATH 1
#define WRT_PROP_ERR_LOG_DIR_NAME 2
#define WRT_PROP_INSERT 3
#define WRT_PROP_DELETE 4
#define WRT_PROP_UPDATE 5
#define WRT_PROP_TRUN_TRG_TABLE_OPTION 6
#define WRT_PROP_DELIMITER 7
#define WRT_PROP_NULL_VAL 8
#define WRT_PROP_ESCAPE_CHAR 9
#define WRT_PROP_QUOTED_VAL 10
#define WRT_PROP_IGNORE_KEY_CONSTRAINT 11
#define WRT_PROP_DUP_ROW_HANDLE_MECH 12
#define WRT_PROP_BAD_FILE_NAME 13
#define WRT_PROP_SOCKET_BUFF_SIZE 14
#define WRT_PROP_CONTROL_CHAR 15
//New attribute is given id 17 because we were not able to set 16, attibute name wasn't getting set into the repository incase id is 16.
#define WRT_PROP_CRINSTRING 17
#define WRT_PROP_TABLEPREFIX 18
#define WRT_PROP_TABLENAME 19
#define WRT_PROP_PRE_SQL 20
#define WRT_PROP_POST_SQL 21


