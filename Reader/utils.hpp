#ifndef  __PCN_READER__UTILS_
#define __PCN_READER__UTILS_

#pragma once
#ifdef WIN32
    #include <afxwin.h>
#endif


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

#include "sdksrv/ilog.hpp"                  //Contains declaration of ILog
#include "sdksrv/ipartition.hpp"            //Contains declaration of IPartitionKey
#include "sdksrv/isess.hpp"                 //Contains declaration of ISession
#include "sdksrv/iutilsrv.hpp"              //Contains declaration of IUtilsServer
#include "sdksrv/srvtypes.h"                //Defines various server-only PowerMart types

#include "sdksrv/reader/irdrbuf.hpp"        //Contains declaration of IRdrBuffer
#include "sdksrv/reader/prdsqdrv.hpp"       //Contains declaration of PRdrSQDriver
#include "sdksrv/reader/prplgdrv.hpp"       //Contains declaration of PRdrPluginDriver
#include "sdksrv/reader/prprtdrv.hpp"       //Contains declaration of PRdrPartitionDriver
#include "sdkcmneb/utils.hpp"

#include "pmtl/pmtldefs.h"
#include "sdksrvin/isessi.hpp"              //Contains declaration of ISessionImpl
#include "pmi18n/pmustring.hpp"

#include <map>
#include "pcn_constants.hpp"
#include <sql.h>
#include <sqlext.h>

//---------------
////#include "sdkcmn/typesc.h"


#define readAttrArrayLength 10

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

     static SQLTCHAR* IUStringToSQLTCHAR(IUString);

     static ICHAR* IUStringToChar(IUString);
	 static ISTATUS _ConvertToMultiByte( ILocale* pLocale, const IUNICHAR* wzCharSet,ICHAR** pszCharSet, IUINT32 *pulBytes, IBOOLEAN bAllocMem);
	  //Converts a char * string to IUNICHAR* using locale object basmed on code page set in connection object
     static ISTATUS _ConvertToUnicode(	ILocale *pLocale, ICHAR* szCharSet,IUNICHAR** pwzCharSet,IUINT32 *pulBytes, BOOL bAllocMem);
	 static PM_UINT32 DJBHash(const PmUString& str);
	 static std::map<IINT32, IUString> setSessionAttributesFromMMDExtn(IUString sMMD_extn_val, ILocale* pLocale);
	 static std::pair<IINT32, IUString> ProcessStringForSessionAttributes(ICHAR* strNameValuePair,ILocale* pLocale);
};
#endif
