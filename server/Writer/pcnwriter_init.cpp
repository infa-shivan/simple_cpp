/****************************************************************
// ----- Persistent Systems Pvt. Ltd., Pune ----------- 
// Copyright (c) 2005
// All rights reserved.
// Name          : PCNWriter_init.cpp
// Description   : Global function called by informatica 
				   session framework
// Author        : Sunita Patil 02-11-2005

  				   Sunita Patil  17-06-2005
				   Missing links between source qualifier 
				   and the netezza target table is supported

				   Sunita Patil  20-06-2005
				   Support for transaction and recovery

				   Sunita Patil  23-06-2005
				   Update strategy is added

				   Sunita Patil  27-06-2005
				   Localization support is added

*****************************************************************/


// Header Files
#include "sdkcmn/iversion.hpp"
#include "pcnwriter_plugin.hpp"


#if defined(DOS) || defined(OS2) || defined(NT)  || defined(WIN32) || defined (MSDOS)
	#include "windows.h"
#endif

const char* VERSION="6.0.0";

/****************************************************************
 * Name		   : WrtGetPluginVersion
 * Description : Gets the plugin version
 * Returns	   : ISTATUS
 ****************************************************************/

extern "C" PLUGIN_EXPORT ISTATUS WrtGetPluginVersion(const IVersion* , 
                                                      IVersion* plVer)
{
   	plVer->setVersion(VERSION);
    return ISUCCESS;
}

/****************************************************************
 * Name		   : WrtCreatePluginDriver
 * Description : Creates the writer plugin
 * Returns	   : Pointer to the plugin driver
 ****************************************************************/

extern "C" PLUGIN_EXPORT PPluginDriver* WrtCreatePluginDriver()
{      	 
     return new PCNWriter_Plugin;
}

/****************************************************************
 * Name		   : WrtDestroyPluginDriver
 * Description : Destroys the plugin driver
 * Returns	   : None
 ****************************************************************/

extern "C" PLUGIN_EXPORT void WrtDestroyPluginDriver(PPluginDriver* plgDriver)
{
    delete plgDriver;
	plgDriver = NULL;
}

