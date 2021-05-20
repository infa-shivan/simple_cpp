#ifdef WIN32
    // Disable certain MSVC "warnings" (also disabled by MFC)
    #pragma warning(disable: 4275 4251 4018 4514)

#endif

#include <assert.h>
//#include <stdio.h>

#ifdef WIN32
    #define PLUGIN_EXPORT __declspec(dllexport)
         
#else
    #define PLUGIN_EXPORT
        
#endif 
