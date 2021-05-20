/****************************************************************
* Name 			: MsgCatalog.hpp
* Description	: Message catalog Declaration

*****************************************************************/

#if !defined __MSG_CATALOG_INCLUDED__
#define __MSG_CATALOG_INCLUDED__



#include "cmn_msg.h"


#ifdef PCNREADER_EXPORTS
#include "../Reader/utils.hpp"
#include "../Reader/rdr_msg.h"
#else
#include "../Writer/utils.hpp"
#include "../Writer/wrt_msg.h"
#endif

#include "pcn_constants.hpp"

class MsgCatalog  
{
private:
		IVector<IUString> m_vSupportedCatalog;
		IUString m_sDefaultMessage;
		IUString m_sCatalogErrorMessage;
        IUString m_sFilePrefix;
        const IUtilsCommon* m_pUtilsCommon;
public:
        IMsgCatalog *m_pMsgCatalog;
        MsgCatalog(const IUtilsCommon* pUtilsCommon, IUString sFilePrefix, IUString sLogPrefix);
        MsgCatalog();
        virtual ~MsgCatalog(void);
        IUString getMessage(IINT32 nMsgCode);
		ISTATUS setSupportedCatalog(IVector<IUString>);
		ISTATUS setSupportedCatalog(IUString);
		ISTATUS clearSupportedCatalog(void);
		ISTATUS setDefaultMessage(IUString sMessage);
		ISTATUS setCatalogErrorMessage(IUString sMessage);
		IBOOLEAN isCatalogSupported(IUString sLangCode);
private:
		ISTATUS initCatalog(const IUtilsCommon* pUtilsCommon, IUString sLogPrefix);
};

#endif // !defined(__MSG_CATALOG_INCLUDED__)
