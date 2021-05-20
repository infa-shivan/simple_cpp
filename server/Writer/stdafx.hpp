/****************************************************************
* File Name : stdafx.hpp
* Details   : 
*****************************************************************/

#ifdef WIN32
    // Disable certain MSVC "warnings" (also disabled by MFC)
    #pragma warning(disable: 4275 4251 4018 4514)

#endif

#include <assert.h>
#include <stdio.h>
#define TRUE 1
#define FALSE 0
#ifdef WIN32
    #define PLUGIN_EXPORT __declspec(dllexport)
	 
#else
    #define PLUGIN_EXPORT
	 
#endif

