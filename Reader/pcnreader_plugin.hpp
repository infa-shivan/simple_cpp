/*****************************************************************
 * ----- Persistent Systems Pvt. Ltd., Pune ----------- 
 * Copyright (c) 2005
 * All rights reserved.
 * Name        : PCNReader_Plugin.hpp
 * Description : Plugin Driver Declaration
 * Author      : Rashmi Varshney
 *****************************************************************/

#ifndef _PCN_READER_PLUGIN_
#define _PCN_READER_PLUGIN_

//PCNReader header files
#include "pcnreader_dsq.hpp"            //Data Source Qualifier Driver declaration

class PCNReader_Plugin : public PRdrPluginDriver
{
public:
    //Constructor
    PCNReader_Plugin();

    //Destructor
    ~PCNReader_Plugin();
     
    /****************************************************************
     *  Called by the SDK framework once per PowerCenter session.
     *  All the metadata is passed to the plug-in at this level.     
     ***************************************************************/
    ISTATUS init(const ISession*, const IPlugin*, const IUtilsServer*);

    //Can be used to do any session validation.
    ISTATUS validate();
     
    /****************************************************************
     *  SDK framework calls this method once per Source Qualifier 
     *  instance and per session run when the Source Qualifier is 
     *  about to be started. dsqNum is the Source Qualifier number.  
     ***************************************************************/
    PRdrSQDriver* createSQDriver(IUINT32 dsqNum);

    /****************************************************************
     * SDK framework prompts the plug-in to destroy the
     * PRdrSQDriver-derived object. It is called when
     * the Source Qualifier and all its partitions have shutdown.
     ****************************************************************/
    void destroySQDriver(PRdrSQDriver*);

    /****************************************************************
     * Queries the plug-in to see if the Source Qualifier
     * can produce real-time flush requests. Returns FALSE
     ***************************************************************/
    ISTATUS isRealTimeSQ(IUINT32 dsqNum, IBOOLEAN& result);

    /****************************************************************
    * Called by the SDK to mark the end of the plugin's lifetime.
    * The session run status until this point is passed as an input.
    ****************************************************************/
    ISTATUS deinit(ISTATUS runStatus);

private:
    const ISession*                 m_pSession;     //Contains all the session-level metadata.
                                                    //It is passed by the SDK.
    const IPlugin*                  m_pPlugin;      //Contains the metadata registered by the plug-in 
                                                    //during plug-in install. It is passed by the SDK.
    const IUtilsServer*             m_pUtils;       //Contains the utils methods. It is passed by the SDK.
    
    IVector<const IAppSQInstance*>  m_DSQList;  

    MsgCatalog*                     m_pMsgCatalog;  // Message catalog

    SLogger*                        m_pLogger;      // Used to log messages in the session log

};
#endif  //_PCN_READER_PLUGIN_
