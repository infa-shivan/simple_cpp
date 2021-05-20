#include "msgcatalog.hpp"
#include<stdio.h>

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////
const IUINT16 MAXDIRPATH= 1024;

FILE *pFile2;

MsgCatalog::MsgCatalog()
{
}
MsgCatalog::MsgCatalog(const IUtilsCommon* pUtilsCommon, IUString sFilePrefix, IUString sLogPrefix)
{
    //Initialize IMsgCatalog here
 	try
	{
		m_sFilePrefix=sFilePrefix;
    	if (initCatalog(pUtilsCommon,sLogPrefix) ==IFAILURE)
			throw IFAILURE;
		m_pMsgCatalog->getMsg(MSG_CMN_INVALID_CAT_MSG_ID,m_sDefaultMessage);
	}
	catch(...)
	{
		throw IFAILURE;
	}
}

MsgCatalog::~MsgCatalog(void)
{
    m_vSupportedCatalog.clear();
    m_pUtilsCommon->destroyMsgCatalog(m_pMsgCatalog);
}

IUString MsgCatalog::getMessage(IINT32 nMsgCode)
{
    IUString sMsg;
    try
	{
        m_pMsgCatalog->getMsg(nMsgCode, sMsg);
		if(sMsg.compare(PCN_Constants::PCN_EMPTY)==0)
			sMsg=m_sDefaultMessage;
    }catch(...)
    {
        sMsg=m_sCatalogErrorMessage;
    }
    return sMsg;
}

ISTATUS MsgCatalog::initCatalog(const IUtilsCommon* pUtilsCommon, IUString sLogPrefix)
{
    try{
			m_pUtilsCommon=pUtilsCommon;
			IUString sLangCode;
			if (pUtilsCommon->getLanguageCode(sLangCode) != ISUCCESS)
				return IFAILURE;
	        
			IUString sCatName;
			sCatName=IUString(m_sFilePrefix);
			sCatName.append(sLangCode.getBuffer());

			ICHAR  szResDir[MAXDIRPATH]; 
			if (ISUCCESS != IGetProductInstallDir(NULL, szResDir, MAXDIRPATH))
			{
				return IFAILURE;
			}        
			IUString szResourceDir(szResDir);
			m_pMsgCatalog = pUtilsCommon->createMsgCatalog(szResourceDir, sCatName);
			if (m_pMsgCatalog == NULL)
			{
				sCatName.makeEmpty();
				sCatName.append(m_sFilePrefix.getBuffer());
				sCatName.append(IUString("EN"));
				m_pMsgCatalog = pUtilsCommon->createMsgCatalog(szResourceDir, sCatName);
				if(m_pMsgCatalog == NULL)
					return IFAILURE;
			}

			//set the Prefix for the message
			m_pMsgCatalog->setMsgPrefix(sLogPrefix);
			return ISUCCESS;
        }catch(...)
        {
                return IFAILURE;
        }
}

ISTATUS MsgCatalog::setSupportedCatalog(IVector<IUString> vSupportedCatalog)
{
    for(IUINT32 itrCat=0; itrCat < vSupportedCatalog.length(); itrCat++)
    {
        m_vSupportedCatalog.insert(vSupportedCatalog.at(itrCat));
    }
    return ISUCCESS;
}
ISTATUS MsgCatalog::setSupportedCatalog(IUString sLanguageCode)
{
    m_vSupportedCatalog.insert(sLanguageCode);
    return ISUCCESS;
}
ISTATUS MsgCatalog::clearSupportedCatalog()
{
    m_vSupportedCatalog.clear();
    return ISUCCESS;
}


ISTATUS MsgCatalog::setDefaultMessage(IUString sMessage)
{
    m_sDefaultMessage=sMessage;
    return ISUCCESS;
}
ISTATUS MsgCatalog::setCatalogErrorMessage(IUString sMessage)
{
    m_sCatalogErrorMessage=sMessage;
    return ISUCCESS;
}

IBOOLEAN MsgCatalog::isCatalogSupported(IUString sLangCode)
{
    for(IUINT32 itrCat=0; itrCat < m_vSupportedCatalog.length(); itrCat++)
    {
        if(IUStrcmp(m_vSupportedCatalog.at(itrCat),sLangCode)==0)
        {
            return ITRUE;
        }
    }
    return IFALSE;
}
