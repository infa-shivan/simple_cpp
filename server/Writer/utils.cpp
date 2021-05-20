
#include "utils.hpp"
#include <stdio.h>
/** maximum allowed number of characters in a signed 32-bit integer value type including a '-' */
#define INT32_MAX_LENGTH 11

/** maximum allowed number of characters in a signed 64-bit integer value type including a '-' */
#define INT64_MAX_LENGTH 21

/** Array used for fast number character lookup */
const char digitsArray[10] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
ICHAR* PCNwriterAttributeDefinitions[] = {"Pipe Directory Path","Error Log Directory Name","Enable Insert","Enable Delete","Enable Update",\
										  "Truncate Target Table Option","Delimiter","Null Value",\
										  "Escape Character","Quoted Value","Ignore Key Constraint",\
										  "Duplicate Row Handling","Bad File Name","Socket Buffer Size",\
										  "Control Character","dummy","CRINSTRING","Table Name Prefix","Target Table Name","PreSQL","PostSQL"};
bool attrDatatypeIsString[] = {true,true,false,false,true,\
							   false,true,true,\
							   true,true,false,\
							   true,true,true,\
							   true,true,true,true,true,true,true};
bool attrDatatypeIsBoolean[] = {false,false,true,true,false,\
							   true,false,false,\
							   false,false,true,\
							   false,false,false,\
							   false,false,false,false,false,false,false};



/*
Converts a 32 bit integer to a string.
The string is written in buf,and the caller is responsible
to allocate enough memory(atleast _INT32_MAX_LENGTH + 1).
dl->The number of desired characters to be returned
if dl is 0(default), the conversion will be as it is
else if the length of the converted string is less then dl,
it will prefix with zeroes so that the total length is dl.
For ex: int32ToStr(12,buf) will return 12
whereas int32ToStr(12,buf,3) will return 012
Returns the length of the string just converted.
*/

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

int Utils::int32ToStr(IINT32 num, char * buf, int dl)
{
	int len = 0;
	if (-2147483648 == num)//the minimum signed integer
	{
		len = sprintf(buf, "%d", num);
		return len;
	}

	int max = (INT32_MAX_LENGTH > dl ? INT32_MAX_LENGTH : dl);

	char *ptr = buf + max + 1;

	*--ptr = '\0';

	bool isNegative = false;

	if (num < 0)
	{
		isNegative = true;
		num *= -1;
	}

	len = 0;
	do
	{
		*--ptr = digitsArray[num % 10];
		len++;
		num /= 10;
	} while (num);

	int diff = dl - len; //If the number character to be printed is more then len
	if (diff > 0)
	{
		for (int index = diff; index >0; index--)
		{
			*--ptr = '0';
		}
		len = dl;
	}

	if (isNegative)
	{
		*--ptr = '-';
		len += 1;
	}

	memmove(buf, ptr, len + 1); //Move the strint to the front

	return len;
}
/*
Converts a 64 bit integer to a string.
The string is written in buf,and the caller is responsible
to allocate enough memory(atleast _INT32_MAX_LENGTH + 1).
dl->The number of desired characters to be returned
if dl is 0(default), the conversion will be as it is
else if the length of the converted string is less then dl,
it will prefix with zeroes so that the total length is dl.
For ex: int64ToStr(12,buf) will return 12
whereas int64ToStr(12,buf,3) will return 012
Returns the length of the string just converted.
*/

int Utils::int64ToStr(IINT64 num, char * buf, int dl)
{
	int len = 0;
	if (num < 0)
	{
		IINT64 tempNum = -9223372036854775808;
		if (tempNum == num)//the minimum signed integer64
		{
#ifdef WIN32
			len = sprintf(buf, "%I64d", num);
#else
			len = sprintf(buf, "%lld", num);
#endif
			return len;
		}
	}

	int max = (INT64_MAX_LENGTH > dl ? INT64_MAX_LENGTH : dl);

	char *ptr = buf + max + 1;

	*--ptr = '\0';

	bool isNegative = false;

	if (num < 0)
	{
		isNegative = true;
		num *= -1;
	}


	len = 0;
	do
	{
		*--ptr = digitsArray[num % 10];
		len++;
		num /= 10;
	} while (num);

	int diff = dl - len; //If the number character to be printed is more then len
	if (diff > 0)
	{
		for (int index = diff; index >0; index--)
		{
			*--ptr = '0';
		}
		len = dl;
	}

	if (isNegative)
	{
		*--ptr = '-';
		len += 1;
	}

	memmove(buf, ptr, len + 1); //Move the strint to the front

	return len;
}

ISTATUS Utils::setSessionAttributesFromMMDExtn(const ISessionExtension* m_pSessExtn, IUString sMMD_extn_val, ILocale* pLocale, IINT32 partCount)
{
	if ( sMMD_extn_val.isEmpty() || 0 == sMMD_extn_val.compare(IUString("{}").getBuffer()) )
	{
		return ISUCCESS;
	}
	sMMD_extn_val.strip(IUString("{").getBuffer(), IUString::eST_LEADING );
	ISessionExtension* m_psessExtn = const_cast<ISessionExtension*>(m_pSessExtn);
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
			if( strValue[index] == ',' && strValue[index - 1] == '"' && strValue[index - 2] != ':')//in case of delimiter , can be sole part of data
			{
				break;
			}
		}
		strNameValuePair[nvpIndex] = 0;
		ProcessStringForSessionAttributes(m_psessExtn, strNameValuePair, partCount, pLocale);
	}
	DELETEPTRARR<ICHAR>(strValue);
	return ISUCCESS;
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
			temp[1]=' ';
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

ISTATUS Utils::ProcessStringForSessionAttributes(ISessionExtension* m_psessExtn, ICHAR* strNameValuePair, IINT32 partCount,ILocale* pLocale)
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

	if ( 0 != strlen(attrValue) ) //only fill if a value is provided since setSessionAttributes() will be called before getMetaExtensionInfo()
	{
		for ( IINT32 i = 0; i < writeAttrArrayLength ; i++) //counting 5 attributes for reader
		{
			if( 0 == strcmp(PCNwriterAttributeDefinitions[i], attrName) )
			{   
				//Handling Escape Characters in SQL Queries			
				if ((0 == strcmp("PreSQL",attrName)) || (0 == strcmp("PostSQL",attrName)))
				{
					removeEscapeCharFromSQLStmt(attrValue);
				}
				//Handling Escape Characters in SQL Queries

				//CCON-11457 - handling consecutive backward slashes
				ICHAR *finalValueOfattrValue;
				if(true == attrDatatypeIsString[i]){
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
					finalValueOfattrValue = tempAttrValue;
				}
				else
					finalValueOfattrValue = attrValue;
				//end CCON-11457 - handling consecutive backward slashes

				IUNICHAR* pwzBuffer;
				IUINT32 pulbyte;
						
				_ConvertToUnicode(pLocale,finalValueOfattrValue,&pwzBuffer,&pulbyte,ITRUE);

				IUString uistrVal(pwzBuffer);
				DELETEPTRARR <IUNICHAR> (pwzBuffer);
				IINT32 attrId = i+1;
				if ( true == attrDatatypeIsString[i] )
				{
					if ( partCount == 1 )
						m_psessExtn->setAttribute(attrId, uistrVal);
					else
					{
						for ( IUINT32 partNum = 0; partNum < partCount; partNum++)
							m_psessExtn->setAttribute(attrId, uistrVal, partNum);
					}
				}
				else
				{
					IINT32 uiAttrVal;
					/*added following if-else loop to check for boolean session attributes - CCON-11432*/
					if (true == attrDatatypeIsBoolean[i])
					{
						if(0 == strcmp(attrValue,"YES"))
							uiAttrVal = 1;
						else
							uiAttrVal = 0;
					}					
					else
						uiAttrVal = atoi(attrValue);
					/*end if-else loop to check for boolean session attributes - CCON-11432*/

					if ( partCount == 1 )
						m_psessExtn->setAttributeInt(attrId, uiAttrVal);
					else
					{
						for ( IUINT32 partNum = 0; partNum < partCount; partNum++)
							m_psessExtn->setAttributeInt(attrId, uiAttrVal, partNum);
					}
				}
			}
		}
	}
	return ISUCCESS;
}

Utils::Utils(void)
{
}

Utils::~Utils(void)
{
}

