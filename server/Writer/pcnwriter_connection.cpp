#include "pcnwriter_connection.hpp"
#include "pcnwriter_partition.hpp"


PCNWriter_Connection::PCNWriter_Connection()
{}

PCNWriter_Connection::PCNWriter_Connection(IUINT32 targetIndex,IUINT32 groupIndex,IUINT32 partIndex,const IConnection* pConn)
{
	this->m_targetIndex=targetIndex;
	this->m_groupIndex=groupIndex;
	this->m_partIndex=partIndex;
	this->m_pConnection=pConn;
	this->m_bIsDBHanleAllocated=IFALSE;
	this->m_bIsCommitCalled=IFALSE;
	this->m_commitMutex=new IThreadMutex();
	this->m_CommitRetStatus=ISUCCESS;
}

PCNWriter_Connection::~PCNWriter_Connection()
{
	delete m_commitMutex;
}

ISTATUS PCNWriter_Connection::init()
{	
	return ISUCCESS;
}

ISTATUS PCNWriter_Connection::deinit()
{
	return ISUCCESS;
}

ISTATUS PCNWriter_Connection::commit()
{
	return ISUCCESS;
}

ISTATUS PCNWriter_Connection::commitFromDeinit()
{
	//TODO: Handle commit for each target and partition..........

	/*We are first closing all pipe handles from the writer side, for all targets in the TCG. After that waiting 
	for child thread to be done. This will make sure, loading in to the target table from temp table can be done 
	in parallel for targets in the TCG.*/
	m_bIsCommitCalled=ITRUE;
	for(IINT32 i=0;i<m_partitionList.length();i++)
	{
		PCNWriter_Partition *pPcnWriterPart = (PCNWriter_Partition *)(m_partitionList.at(i));
		if(pPcnWriterPart->isNoDataCase())
		{
			if(this->traceLevel >= SLogger::ETrcLvlNormal)
				m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_NO_DATA_WRITE_INTO_PIPE_FAIL));

			if(pPcnWriterPart->openWRPipe()==IFAILURE)
			{
				return IFAILURE;
			}
		}

		if(pPcnWriterPart->closeWRPipe()==IFAILURE)
		{
			return IFAILURE;
		}
	}

	for(IINT32 i=0;i<m_partitionList.length();i++)
	{
		PCNWriter_Partition *pPcnWriterPart = (PCNWriter_Partition *)(m_partitionList.at(i));
		if(IFAILURE==pPcnWriterPart->childThreadHandling())
		{
			return IFAILURE;
		}
	}

	IBOOLEAN bCheckAnyfailedQuery=IFALSE;
	IBOOLEAN bIsAnyPartitionLoadTgt=IFALSE;
	for(IINT32 i=0;i<m_partitionList.length();i++)
	{
		PCNWriter_Partition *pPcnWriterPart = (PCNWriter_Partition *)(m_partitionList.at(i));
		if(pPcnWriterPart->isAnyQueryFailed())
		{
			return IFAILURE;
		}
		//If any partition in connection group loads data in to the target thn we should issue commit on that env.
		if(pPcnWriterPart->isLoadInToTgt())
		{
			bIsAnyPartitionLoadTgt=ITRUE;
		}

		if(pPcnWriterPart->handlingHasDTMStop() ==IFAILURE)
		{
			return IFAILURE;
		}
	}

	//issuing commit for the env handle, so it will commit all the connection under this env.	

	/*Currently each target instance creates its own transection so if we have different instarnces of same target in the mapping under same TCG,
	and more than one transection(corresponding to same target one) tries to execute a query, which have a where clause, then the NPS might throw 
	serialization error during the qury execution.
	But in some cases NPS throws this error during the commit time, leads to successfull commit for one connection, while failure for one. 
	As a workaround we are validating our TCG group. If we have this kind of scenario we fail the session, before we start commiting transections.
	*/
	if(IFAILURE==validateSerializationError())
	{
		return IFAILURE;
	}

	SQLRETURN rc;
	for(IINT32 i=0;i<m_partList_commitOrder.length();i++)
	{
		PCNWriter_Partition *part=(PCNWriter_Partition *)m_partList_commitOrder.at(i);
		rc = SQLEndTran(SQL_HANDLE_DBC,part->getDBHandle(),SQL_COMMIT);
		if(rc!=SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			// Failed to commit the transaction
			if(this->traceLevel >= SLogger::ETrcLvlTerse)
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_COMMIT_TRANS_FAIL));
			return IFAILURE;
		}
	}

	// Commit successful
	if(this->traceLevel >= SLogger::ETrcLvlNormal)
		m_sLogger->logMsg(ILog::EMsgInfo, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_COMMIT_SUCC));

	return ISUCCESS;
}

ISTATUS PCNWriter_Connection::rollback()
{
	//TODO: Log a message in the session log that rollback is not supported. 
	return ISUCCESS;
}

IBOOLEAN PCNWriter_Connection::isEqual(const PWrtConnection* pConn)
{
	//return true if connection attributes are same; so all the PWrtConnection object will be 
	//equal if connection attributes are same and all targets will be grouped in to a single group.
	/*
	In 86101, TCG functionality was implemented, because of Netezza serialization error, and limitation
	of sharing a connection by different threads, different connection was created for targets under the same 
	connection group. This lead to a behavior change from 861 version (see validateSerializationError function).

	Creating different connections to have seprate connection group is not acceptable to customers as it would
	lead to lots of maintenance work. So here we are providing an undocumentation flag name as setTCGOff which if set
	at integration level then will create different connection group for each target.
	*/
	m_bTCGOffFlag=PM_FALSE;
	GetConfigBoolValue(TCG_FLAG,m_bTCGOffFlag);

	if(m_bTCGOffFlag)
	{
		return IFALSE;
	}
	else
	{
		PCNWriter_Connection *pConnection=(PCNWriter_Connection *)pConn;

		if(this->m_pConnection->getUserName() == pConnection->m_pConnection->getUserName() &&
			this->m_pConnection->getPassword() == pConnection->m_pConnection->getPassword() &&
			this->m_pConnection->getConnectString() == pConnection->m_pConnection->getConnectString())
		{
			return ITRUE;
		}
		else
		{
			return IFALSE;
		}
	}
}

void PCNWriter_Connection::setMessageHandles(IINT32 traceLevel, SLogger *m_sLogger,MsgCatalog*	m_msgCatalog)
{
	this->traceLevel=traceLevel;
	this->m_sLogger=m_sLogger;
	this->m_msgCatalog=m_msgCatalog;
}
ISTATUS PCNWriter_Connection::allocEnvHandles(SQLHENV &hEnv)
{
	//If DB handle is already allocated, set the older value; otherwise allocated a new one. 
	if(m_bIsDBHanleAllocated)
	{
		hEnv=m_henv;
	}
	else
	{
		SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &m_henv);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			if(traceLevel >= SLogger::ETrcLvlNormal)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(MSG_WRT_ALLOC_ENV_HANDLE_FAIL));
			}
			return IFAILURE;
		}
		
		rc = SQLSetEnvAttr(m_henv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, SQL_IS_INTEGER);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			if(traceLevel >= SLogger::ETrcLvlNormal)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(MSG_WRT_SET_ENV_ATTR_FAIL));
			}
			return IFAILURE;
		}
		
		#ifndef WIN32
		rc = SQLSetEnvAttr(m_henv, SQL_ATTR_APP_UNICODE_TYPE,(SQLPOINTER)SQL_DD_CP_UTF16, SQL_IS_INTEGER);
		if(rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
		{
			if(traceLevel >= SLogger::ETrcLvlNormal)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(MSG_CMN_SET_UNICODE_MODE_FAIL));
			}
			return IFAILURE;
		}
		#endif
			
		hEnv=m_henv;
		m_bIsDBHanleAllocated=ITRUE;
	}
	return ISUCCESS;		
}

void PCNWriter_Connection::addPartitionObject(PCNWriter_Partition *pPcnWriterPart)
{
	m_partitionList.insert(pPcnWriterPart);
}

IBOOLEAN PCNWriter_Connection::isCommitCalled()
{
	return m_bIsCommitCalled;
}

ISTATUS PCNWriter_Connection::validateSerializationError()
{	
	IUString tgtTablenamei=PCN_Constants::PCN_EMPTY;
	IUString tgtTablenamej=PCN_Constants::PCN_EMPTY;
	PCNWriter_Partition *pPcnWriterParti;
	PCNWriter_Partition *pPcnWriterPartj;
	IINT32 i,j, totalCount=0;
	/*this array will store the index for all the partitions which has same target table name. suppose there are 6 partition and 0, 3 and 5 
	are having same tgt name and 1 and 4 have same tgt name, then the index would be [0,1,2,0,1,0].
	*/
	m_tgtTableIndex=new IINT32[m_partitionList.length()];
	for(i=0;i<m_partitionList.length();i++)
	{
		pPcnWriterParti = (PCNWriter_Partition *)(m_partitionList.at(i));
		tgtTablenamei=pPcnWriterParti->getTargetTblName();
		m_tgtTableIndex[i]=totalCount;
		for(j=0;j<i;j++)
		{
			pPcnWriterPartj = (PCNWriter_Partition *)(m_partitionList.at(j));
			tgtTablenamej=pPcnWriterPartj->getTargetTblName();
			if(tgtTablenamei== tgtTablenamej)
			{
				m_tgtTableIndex[i]=m_tgtTableIndex[j];
				j=i;          //found the right index, getting out of the loop.
			}
			else if(j==i-1)
			{
				m_tgtTableIndex[i]=++totalCount; //didnt find any same tgtName so storing new index.
			}
		}
	}
	totalCount++;

	//totalcount store how many partition have same target table name. noOfWhereClauseForTgt count number of transection firing a where clause
	//query for a perticular target table.
	IINT32 *noOfWhereClauseForTgt=new IINT32[totalCount];
	for(i=0;i<totalCount;i++)
	{
		noOfWhereClauseForTgt[i]=0;
	}

	IINT32 *whoHasWhereClause=new IINT32[totalCount];
	for(i=0;i<m_partitionList.length();i++)
	{
		pPcnWriterParti = (PCNWriter_Partition *)(m_partitionList.at(i));
		if(pPcnWriterParti->isQueryHaveWhereClause())
		{
			noOfWhereClauseForTgt[m_tgtTableIndex[i]]++;
			whoHasWhereClause[m_tgtTableIndex[i]]=i;
		}
	}

	for(i=0;i<totalCount;i++)
	{
		if(noOfWhereClauseForTgt[i] >1)
		{
			if(traceLevel >= SLogger::ETrcLvlNormal)
			{
				m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlNormal,m_msgCatalog->getMessage(MSG_WRT_NZ_SERIALIZATION_ERR));
			}
			return IFAILURE;
		}
	}

	/*We need to decide final commit order..... this is important as we need to make sure those instance are having a query with where clause 
	are committed first, after this we need to make sure we insure target level automacity atleast.
	for above example if partition 3 and 4 have where clause then the commit order would be:
	3,0,5,4,1,2.
	*/
	for(i=0;i<totalCount;i++)
	{
		//need which partition has where clause for this target... if any..
		if(noOfWhereClauseForTgt[i]==1)
		{
			PCNWriter_Partition *part=m_partitionList[whoHasWhereClause[i]];
			m_partList_commitOrder.insert(part);
		}
		for(j=0;j<m_partitionList.length();j++)
		{
			if(m_tgtTableIndex[j]==i)
			{
				PCNWriter_Partition *part1=m_partitionList[j];
				if(!m_partList_commitOrder.contains(part1))
				{
					m_partList_commitOrder.insert(part1);
				}
			}				
		}
	}

	DELETEPTRARR <IINT32>(m_tgtTableIndex);
	DELETEPTRARR <IINT32>(noOfWhereClauseForTgt);
	DELETEPTRARR <IINT32>(whoHasWhereClause);

	return ISUCCESS;
}

void PCNWriter_Connection::lockCommitMutex()
{
	this->m_commitMutex->lock();
}

void PCNWriter_Connection::unlockCommitMutex()
{
	this->m_commitMutex->unlock();
}

void PCNWriter_Connection::setCommitRetStatus(ISTATUS retStatus)
{
	m_CommitRetStatus=retStatus;
}

ISTATUS PCNWriter_Connection::getCommitRetStatus()
{
	return m_CommitRetStatus;
}
