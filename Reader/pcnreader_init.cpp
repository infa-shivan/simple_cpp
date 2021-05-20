/*******************************************************************************
 * ----- Persistent Systems Pvt. Ltd., Pune ----------- 
 * Copyright (c) 2005
 * All rights reserved.
 * Name        : PCNReader_init.cpp
 * Description : The global functions which are called by the SDK
 *               framework are defined.
 * Author      : Rashmi Varshney
 *******************************************************************************/
	
#include "stdafx.hpp"
#include "pcnreader_plugin.hpp"     //Plugin Driver Declaration

const ICHAR *VERSION = "6.0.0";      //This framework's version

/***************************************************************************
 * Name : RdrGetPluginVersion
 * Description : Called by the SDK framework to get the plugin's version
 * Returns : this framework's version
 ***************************************************************************/
extern "C" PLUGIN_EXPORT
ISTATUS RdrGetPluginVersion(const IVersion* /*frameVersion*/, IVersion* myVersion)
{    
    return myVersion->setVersion(VERSION);
}

/**************************************************************************************
 * Name : RdrCreatePluginDriver
 * Description : Called by the SDK framework to get an instance of the Plug-in class
 * Returns : Return an "empty" top-level driver 
 **************************************************************************************/
extern "C" PLUGIN_EXPORT
PPluginDriver* RdrCreatePluginDriver()
{
    return new PCNReader_Plugin();
}

/********************************************************************************
 * Name : RdrDestroyPluginDriver
 * Description : Called by the SDK framework to destroy the top-level driver 
 ********************************************************************************/
extern "C" PLUGIN_EXPORT
void RdrDestroyPluginDriver(PPluginDriver* pluginDriver)
{
    DELETEPTR <PPluginDriver>(pluginDriver);
}
