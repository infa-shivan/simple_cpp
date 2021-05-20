#ifndef  __PCN_READER__UTILS_
#define __PCN_READER__UTILS_

#pragma once
#ifdef WIN32
#include <afxwin.h>         // MFC core and standard components
#include <afxext.h>         // MFC extensions
#endif


#include <sql.h>
#include <sqlext.h>

//informatica header files
#include "sdkcmn/iconn.hpp"                 //Contains declaration of IConnection
#include "sdkcmn/iconnref.hpp"              //Contains declaration of IConnectionRef
#include "sdkcmn/idsqinst.hpp"              //Contains declaration of IAppSQInstance
#include "sdkcmn/ifield.hpp"                //Contains declaration of IField
#include "sdkcmn/igroup.hpp"                //Contains declaration of IGroup
#include "sdkcmn/ilink.hpp"                 //Contains declaration of ILink
#include "sdkcmn/ilocale.hpp"               //Contains declaration of ILocale
#include "sdksrv/imap.hpp"                  //Contains declaration of IMap
#include "sdkcmn/imetaext.hpp"              //Metadata extensions external interfaces header file
#include "sdkcmn/imsgcata.hpp"
#include "sdkcmn/iplugin.hpp"               //Contains declaration of IPlugin
#include "sdkcmn/ireposit.hpp"
#include "sdkcmn/isesextn.hpp"              //Contains declaration of ISessionExtension
#include "sdkcmn/isession.hpp"              //Contains declaration of ISession
#include "sdkcmn/isource.hpp"               //Contains declaration of ISource
#include "sdkcmn/isrcfld.hpp"               //Contains declaration of ISourceField
#include "sdkcmn/isrcinst.hpp"              //Contains declaration of ISourceInstance
#include "sdkcmn/isrctgtfld.hpp"            //Contains declaration of ISrcTgtFld
#include "sdkcmn/itable.hpp"                //Contains declaration of ITable
#include "sdkcmn/iucode.hpp"                //Contains declaration of IUCode
#include "sdkcmn/iutilcmn.hpp"              //Contains declaration of IUtilsCommon
#include "sdkcmn/iutildec.hpp"              //Contains declaration of IUtilsDecimal
#include "sdkcmn/iutildat.hpp"              //Contains declaration of IUtilsDate
#include "sdkcmn/iustring.hpp"              //Contains declaration of IUString
#include "sdkcmn/ivector.hpp"               //Contains declaration of IVector
#include "sdkcmn/iversion.hpp"              //Contains declaration of IVersion
#include "sdkcmn/iwdgfld.hpp"               //Contains declaration of IWidgetField
#include "sdkcmn/iwidget.hpp"               //Contains declaration of IWidget
#include "sdkcmn/itgtfld.hpp"				//Contains declaration of ITargetField

#include "sdksrv/ilog.hpp"                  //Contains declaration of ILog
#include "sdksrv/ipartition.hpp"            //Contains declaration of IPartitionKey
#include "sdksrv/isess.hpp"                 //Contains declaration of ISession
#include "sdksrv/iutilsrv.hpp"              //Contains declaration of IUtilsServer
#include "sdksrv/srvtypes.h"                //Defines various server-only PowerMart types

#include "sdksrv/reader/irdrbuf.hpp"        //Contains declaration of IRdrBuffer
#include "sdksrv/reader/prdsqdrv.hpp"       //Contains declaration of PRdrSQDriver
#include "sdksrv/reader/prplgdrv.hpp"       //Contains declaration of PRdrPluginDriver
#include "sdksrv/reader/prprtdrv.hpp"       //Contains declaration of PRdrPartitionDriver

#include "sdksrv/writer/iwptinit.hpp"		//Contains declaration of IWrtPartitionInit
#include "sdksrv/writer/iwrtbuf.hpp"		//Contains declaration of IWrtBuffer
#include "sdksrv/writer/iwptexin.hpp"		//Contains declaration of IWrtPartitionExecIn
#include "sdksrv/writer/iwptexou.hpp"       //Contains declaration of IWrtPartitionExecOut
#include "sdksrv/writer/pwptdrv.hpp"        //Contains declaration of PWrtPartitionDriver
#include "sdksrv/writer/pwplgdrv.hpp"		//Contains declaration of PWrtPluginDriver
#include "sdksrv/writer/pwtgdrv.hpp"
#include "sdksrv/writer/pwrtconn.hpp"		//Contains declaration of PWrtConnection
#include "sdkcmn/itgtinst.hpp"


#include "sdkcmneb/utils.hpp"
//---------------
////#include "sdkcmn/typesc.h"

#define writeAttrArrayLength 21

/* --- [Utils] ----------------------------------------------------- */
template <class tDataType >
inline void FREEPTR( tDataType *p) {
    if(p) {
        free(p); 
        p = NULL;
    }
}

template <class tDataType>
inline void DELETEPTR( tDataType *p) {
    if(p) {
        delete p;
        p = NULL;
    }
}


template <class tDataType>
inline void DELETEPTRARR( tDataType *p) {
    if(p) {
        delete [] p;
        p = NULL;
    }
}
/* --- [Utils] ----------------------------------------------------- */

class Utils
{
public:
    Utils(void);
    ~Utils(void);

     //Utility Functions starts here
	 static int int32ToStr(IINT32 num, char * buf, int dl = 0);
	 static int int64ToStr(IINT64 num, char * buf, int dl = 0);
	 static ISTATUS setSessionAttributesFromMMDExtn(const ISessionExtension* m_psessExtn, IUString sMMD_extn_val, ILocale* pLocale, IINT32 partcount);
	 static ISTATUS ProcessStringForSessionAttributes(ISessionExtension* m_psessExtn, ICHAR* strNameValuePair, IINT32 partcount,ILocale* pLocale);

     static SQLTCHAR* IUStringToSQLTCHAR(IUString classString)
	 {
		IUINT32 count = 0;
		IUNICHAR *tempStr = (IUNICHAR*)classString.getBuffer();
		IUINT32 len = (IUINT32)classString.getLength();
		SQLTCHAR* retStr = new SQLTCHAR[len+1];
		for( count=0; count < len; count++)
			retStr[count] = (SQLTCHAR)tempStr[count];
		retStr[count] = '\0';
        return retStr;
	 }

     static ICHAR* IUStringToChar(IUString classString)
	 {
		IUINT32 i = 0;
		IUNICHAR *tempStr = (IUNICHAR*)classString.getBuffer();
		IUINT32 len = (IUINT32)classString.getLength();
		ICHAR* retStr = new ICHAR[len+1];
	    for( i=0; i < len; i++)
			retStr[i] = (ICHAR)tempStr[i];
		retStr[i] = '\0';
		return retStr;
	 }

	 /********************************************************************
	* Name                         : _ConvertToMultiByte
	* Description          : Converts the unicode data to MBCS using the Locale object created
	* Returns                      : None 
	*********************************************************************/

	static ISTATUS _ConvertToMultiByte( ILocale* pLocale, const IUNICHAR* wzCharSet,ICHAR** pszCharSet, IUINT32 *pulBytes, IBOOLEAN bAllocMem)
	{                        
		ISTATUS iResult     = ISUCCESS;
		IUINT32 ulCharSetA  = 0;
		IUINT32 nRetVal     = 0;
		size_t byteLen;
		*pulBytes   = 0;    
		try
		{
			//Get max size of narrow string
			ulCharSetA = IUStrlen (wzCharSet);
			//TODO: make use of getSizeOfShiftCode
			byteLen = pLocale->computeMBCSBytesILocale(ulCharSetA);
			//max bytes
			*pulBytes = byteLen;

			if (ITRUE == bAllocMem)
			{
				*pszCharSet = NULL;
				if((*pszCharSet = (ICHAR *)malloc(*pulBytes)) == NULL) 
				{
					*pulBytes = 0;
					throw IFAILURE;
				}
			}
		
			memset (*pszCharSet, 0, byteLen);
			//U2M
			nRetVal = pLocale->U2M (wzCharSet, *pszCharSet);
			//exact bytes excluding the NULL terminator
			*pulBytes = nRetVal;
		}
		catch(...)
		{
			iResult = IFAILURE;
		}
		return iResult;
	}

	 //Converts a char * string to IUNICHAR* using locale object basmed on code page set in connection object
     static ISTATUS _ConvertToUnicode(	ILocale *pLocale, ICHAR* szCharSet,IUNICHAR** pwzCharSet,IUINT32 *pulBytes, BOOL bAllocMem);
};
#endif
