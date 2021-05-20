
#include "utils.hpp"

ICHAR* PCNreaderAttributeDefinitions[] = {"Pipe Directory Path","Delimiter","Null Value","Escape Character","Socket Buffer Size","SQL Query Override","Owner Name","Source Table Name","PreSQL","PostSQL"};
Utils::Utils(void)
{
}

Utils::~Utils(void)
{
}

SQLTCHAR* Utils::IUStringToSQLTCHAR(IUString classString)
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

ICHAR* Utils::IUStringToChar(IUString classString)
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

ISTATUS Utils::_ConvertToMultiByte( ILocale* pLocale, const IUNICHAR* wzCharSet,ICHAR** pszCharSet, IUINT32 *pulBytes, IBOOLEAN bAllocMem)
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


/********************************************************************
* Name                          : _ConvertToUnicode
* Description           : Converts the MBCS data to Unicode using the Locale object created
szCharSet                : MBCS string to be converted 
pwzCharSet               : IUNICHAR string passed by reference where unicode data is stored
pulBytes                         : No of characters scanned 
* Returns                       : failure or success 
*********************************************************************/


ISTATUS Utils::_ConvertToUnicode(ILocale *m_pLocale, ICHAR* szCharSet,IUNICHAR** pwzCharSet, IUINT32 *pulBytes, BOOL bAllocMem)
{
    ISTATUS iResult     = ISUCCESS;
    IUINT32 ulCharSetW  = 0;
    IUINT32 nRetVal     = 0;
    size_t byteLen;

    *pulBytes = 0;
	try
	{
		//Get max size of wide string
		ulCharSetW = (IUINT32) strlen (szCharSet);
		byteLen = (sizeof (IUNICHAR) * (ulCharSetW + 1));
		//max bytes
		*pulBytes = (IUINT32) byteLen;
		if (ITRUE == bAllocMem)
		{
			*pwzCharSet = NULL;

			if ((*pwzCharSet = (IUNICHAR *)malloc(*pulBytes)) == NULL) 
			{
				iResult = IFAILURE;
				*pulBytes = 0;
				return iResult;
			}               

		}
		memset (*pwzCharSet, 0, byteLen);
		// M2U
		//returns number of IUNICHARs written to pwzCharSet, excluding the NULL terminator
		nRetVal = m_pLocale->M2U (szCharSet, *pwzCharSet);
	}catch(...)
	{
		iResult = IFAILURE;
	}
    return iResult;
}


PM_UINT32 Utils::DJBHash(const PmUString& str)
{
   PM_UINT32 hash = 5381;
	PM_UINT32 len=str.getLength();

   for(std::size_t i = 0; i < len; i++)
   {
      hash = ((hash << 5) + hash) + str[i];
   }

   return hash;
}

std::map<IINT32, IUString> Utils::setSessionAttributesFromMMDExtn(IUString sMMD_extn_val, ILocale* pLocale)
{

	std::map<IINT32, IUString> metadataMap;
	if ( sMMD_extn_val.isEmpty() || 0 == sMMD_extn_val.compare(IUString("{}").getBuffer()) )
	{
		for ( IINT32 i = 0; i < readAttrArrayLength ; i++)
			metadataMap.insert(std::pair<IINT32,IUString>(i+1,PCN_Constants::PCN_EMPTY));
		return metadataMap;
	}
	sMMD_extn_val.strip(IUString("{").getBuffer(), IUString::eST_LEADING );

	ICHAR* strValue;
	IUINT32 byteLen = 0;
	ISTATUS iRes    = ISUCCESS;
	iRes = _ConvertToMultiByte(pLocale, sMMD_extn_val.getBuffer(), &(strValue), &byteLen, ITRUE);
	strValue[byteLen - 1] = 0;
	byteLen -= 1;

	for ( IUINT32 index = 0; index < byteLen; index++ )
	{
		//Sample format for the MMD extension value: 
		//"Infrastructure Tracing Level":"TD_OFF","Trace File Name":"","Query Band Expression":"","useTPTRuntimeFromPC":"true","Spool Mode":"Spool","Driver Tracing Level":"TD_OFF"
		ICHAR strNameValuePair[8192];
		IINT32 nvpIndex = 0;
		while ( index < byteLen )
		{
			strNameValuePair[nvpIndex++] = strValue[index++];
			if( strValue[index] == ',' && strValue[index - 1] == '"' )
			{
				break;
			}
		}
		strNameValuePair[nvpIndex] = 0;
		metadataMap.insert(ProcessStringForSessionAttributes(strNameValuePair,pLocale));
	}
	DELETEPTRARR<ICHAR>(strValue);
	return metadataMap;
}

void removeEscapeCharFromSQLStmt(ICHAR *attrValue)
{	
	if(attrValue == NULL)
		return;	 
	ICHAR *temp = attrValue;
	temp = strchr(temp,'\\');
	while(temp != NULL){		
		if((temp+1 != NULL) &&( (temp[1] == 'n') || (temp[1] == 'r') || (temp[1] == 't')))
		{
			temp[0]=' ';		
			temp[1]=' ';	//overwrites second byte of "\n" i.e. n	
			temp+=2;
		}	
		//to prevent infinite loop
		else
		{
			temp+=1;
		}
		temp = strchr(temp,'\\');
	}	
}

std::pair<IINT32, IUString> Utils::ProcessStringForSessionAttributes(ICHAR* strNameValuePair,ILocale* pLocale)
{
	IUINT32 iTotalLen = strlen(strNameValuePair);
	IUINT32 nvpIndex = 0;
	//The char string passed on should be of the format "Infrastructure Tracing Level":"TD_OFF"
	ICHAR attrName[1024];
	IUINT32 index = 0;
	nvpIndex++; //to avoid copying the enclosing quuote character.
	attrName[index++] = strNameValuePair[nvpIndex++];
	while ( nvpIndex < iTotalLen )
	{
		attrName[index++] = strNameValuePair[nvpIndex++];
		if (strNameValuePair[nvpIndex] == ':' && strNameValuePair[nvpIndex - 1] == '"')
		{
			break;
		}
	}
	attrName[index - 1] = 0;//to avoid copying the enclosing quote character.
	
	index = 0;
	ICHAR attrValue[8192];
	nvpIndex += 2; //incrementing by 2 places to avoid copying the quote character.
	while ( nvpIndex < strlen(strNameValuePair) - 1 ) //copying till last but one place to avoid the enclosing quote character.
	{
		attrValue[index++] = strNameValuePair[nvpIndex++];
	}
	attrValue[index] = 0;
	//readAttrArrayLength is a constant defined based on the number of attributes, if any new attribute is added, increment it accordingly.
	for ( IINT32 i = 0; i < readAttrArrayLength ; i++) 
	{
		if( 0 == strcmp(PCNreaderAttributeDefinitions[i], attrName) )
		{
			//Handling Escape Characters in SQL Query Override - CCON-11484			
			if ((0 == strcmp("SQL Query Override",attrName)) || (0 == strcmp("PreSQL",attrName)) || (0 == strcmp("PostSQL",attrName)))
			{
				removeEscapeCharFromSQLStmt(attrValue);
			}
			//Handling Escape Characters in SQL Query Override - CCON-11484

			//CCON-11457 - handling consecutive backward slashes
			IINT32 attrValLength = strlen(attrValue);
			ICHAR tempAttrValue[8192];
			IINT32 j=0;
			for (IINT32 counter = 0; counter < attrValLength; counter++)
			{
				if('\\' == attrValue[counter])
				{
					//to check for two backslashes
					if( ((counter+1) < attrValLength) && ('\\' == attrValue[counter + 1]))
						++counter;
					//to check for a backslash followed by a double quote together.
					if( ((counter+1) < attrValLength) && ('\"' == attrValue[counter + 1]))
						++counter;
				}
			 	tempAttrValue[j++]= attrValue[counter];
	       } 

			tempAttrValue[j] ='\0';
			//end CCON-11457 - handling consecutive backward slashes

			IUNICHAR* pwzBuffer;
			IUINT32 pulbyte;
			
			_ConvertToUnicode(pLocale,tempAttrValue,&pwzBuffer,&pulbyte,ITRUE);

			IUString uistrVal(pwzBuffer);
			return std::pair<IINT32,IUString>(i+1,uistrVal);
			DELETEPTRARR <IUNICHAR> (pwzBuffer);
		}
	}
	return std::pair<IINT32,IUString>(0,PCN_Constants::PCN_EMPTY);
}
