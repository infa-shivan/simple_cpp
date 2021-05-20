/****************************************************************
* Name          : PCNWriter_Group.cpp
* Description   : Group level interface class
*****************************************************************/



//PCNWriter header files
#include "pcnwriter_target.hpp"
#include "pcnwriter_group.hpp"

/********************************************************************
 * Name		   : PCNWriter_Target
 * Description : Constructor
 * Returns	   : None
 *********************************************************************/

PCNWriter_Target::PCNWriter_Target(const ISession*	m_pSess,
								   const ITargetInstance* pTargetInstance, 
                                   IUINT32 targNum, PCNWriter_Plugin* plugDrv,
								   const ISessionExtension* pSessExtn,
								   const ILog* pLogger,
								   const IUtilsServer* pUtils)
:m_pSession(m_pSess),
 m_pTargetInstance(pTargetInstance), m_nTargNum(targNum),
 m_pPluginDriver(plugDrv), m_pSessExtn(pSessExtn), m_pLogger(pLogger),
 m_pUtils(pUtils)
{ 
    assert(m_pTargetInstance);
    assert(m_pPluginDriver);
    assert(m_pSessExtn);
}

/********************************************************************
 * Name		   : PCNWriter_Target
 * Description : Constructor
 * Returns	   : None
 *********************************************************************/
PCNWriter_Target::PCNWriter_Target(const ISession*	m_pSess,
								   const ITargetInstance* pTargetInstance, 
                                   IUINT32 targNum, PCNWriter_Plugin* plugDrv,
								   const ISessionExtension* pSessExtn,
								    SLogger* sLogger,
								    const IUtilsServer* pUtils,
								   MsgCatalog *msgCatalog,const ILog* pLogger)
:m_pSession(m_pSess),
 m_pTargetInstance(pTargetInstance), m_nTargNum(targNum),
 m_pPluginDriver(plugDrv), m_pSessExtn(pSessExtn), m_sLogger(sLogger),
 m_pUtils(pUtils),m_msgCatalog(msgCatalog),m_pLogger(pLogger)
{ 
    assert(m_pTargetInstance);
    assert(m_pPluginDriver);
    assert(m_pSessExtn);
	m_pluginRequestedRows=0;
	m_pluginAppliedRows=0;
	m_pluginRejectedRows=0;
}
  
/*******************************************************************
 * Name		   : ~PCNWriter_Target
 * Description : Virtual destructor
 * Returns	   : None
 *******************************************************************/
    
 PCNWriter_Target::~ PCNWriter_Target()
{
	for(size_t i=0; i<m_vecGrpDrivers.length();i++)
    {
		DELETEPTRARR<PCNWriter_Group>(m_vecGrpDrivers.at(i));
    }
    m_vecGrpDrivers.clear();
}

/*******************************************************************
 * Name		   : init
 * Description : Init the Target
 * Returns	   : init status
 *******************************************************************/

ISTATUS PCNWriter_Target::init(const IWrtTargetInit* )
{	
    // Get the total number of groups from the widget instance
    IVector<const IGroup*> groupList;

    m_pTargetInstance->getTable()->getGroupList(groupList);
	IUString objecttypename = m_pTargetInstance->getObjectTypeName();
   
    // IMPORTANT NOTE: Group Driver is needed even if the target is not 
	// containing any groups. In such cases it is treated as 1 group. 
	// This has been functional specifications for SDK Framework
	// Specification: For group less targets, the plugin should
	// treat the targets as a special case of targets with one group
	size_t numLoops = (groupList.length() == 0) ? 1 : groupList.length();
    m_itraceLevel = this->m_sLogger->getTrace();
	 
    // Go through all the groups and create an instance of group driver
    for(size_t j=0; j<numLoops; j++)
    {
      // Create a group driver
      PCNWriter_Group* pGrpDriver = new PCNWriter_Group(m_pSession,
														m_pTargetInstance, 
														m_nTargNum,j,
														m_pPluginDriver,
														m_pSessExtn,m_sLogger,
														m_pUtils,this->m_msgCatalog,m_pLogger,m_pluginRequestedRows,m_pluginAppliedRows,m_pluginRejectedRows);
      assert(pGrpDriver);
      // Insert the group into the list
      m_vecGrpDrivers.insert(pGrpDriver);
    }
    return ISUCCESS;
}

/***********************************************************************
 * Name		   : getGroupDriver
 * Description : Get the Group level handle with the given Group ID      
 * Returns	   : Returns the Group level handle with the given Group ID      
 ***********************************************************************/   
 
PWrtGroupDriver* PCNWriter_Target::getGroupDriver(IUINT32 grpIndex)
{
    if(grpIndex >= m_vecGrpDrivers.length())
	{
        if(this->m_itraceLevel >= SLogger::ETrcLvlNormal)
			m_sLogger->logMsg(ILog::EMsgError, SLogger::ETrcLvlTerse,m_msgCatalog->getMessage(MSG_WRT_REQST_GRP_DRIVER_EXCED_LIMIT));
		//Get Group driver failed for Target driver
        return NULL;         
	}
    return m_vecGrpDrivers.at(grpIndex);
}

/**********************************************************************
 * Name		   : deinit
 * Description : Deinit Method of the Target
 * Returns	   : deinit status 
 **********************************************************************/

ISTATUS PCNWriter_Target::deinit(ISTATUS)
{
	const ITable* pTgtTable  = m_pTargetInstance->getTable();
	IUString tgtTbl = pTgtTable->getName();
	IUString tgtInstanceName = m_pTargetInstance->getName();    
//FIX for CR 174877
	return ISUCCESS;
}


