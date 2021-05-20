#
# Makefile to build the PCNReader   dynamic library.
#


# Load the settings

PM_CMN_BUILD_DIR := ../../../../../powrmart/pmbuild/bin
PM_BUILD_DIR := ../build



include $(PM_CMN_BUILD_DIR)/settings.mk

# The main target of this makefile
all: lib
DYNLIB_ROOT := PCNReader

COMPILE_DEFINES += PCNREADER_EXPORTS SQLWCHARSHORT UNICODE _UNICODE

ifeq ($(PMBLD64), y)
    COMPILE_DEFINES += ODBC64
endif


DYNLIB_LINK_OPTIONS += 

# Source file list: please keep this list sorted and one per line
CPP_FILES := \
	msgcatalog \
    pcnreader_dsq \
	pcnreader_init \
	pcnreader_metaextension \
	pcnreader_partition \
	pcnreader_plugin \
	pcnreader_sessionattributes \
	pcnreader_tconnection \
	slogger \
	utils
	
	
# Grab files from respective directories
vpath msgcatalog.cpp ../../cmn
vpath pcnreader_dsq.cpp ..
vpath pcnreader_init.cpp ..
vpath pcnreader_metaextension.cpp ..
vpath pcnreader_partition.cpp ..
vpath pcnreader_plugin.cpp ..
vpath pcnreader_sessionattributes.cpp ..
vpath pcnreader_tconnection.cpp ..
vpath slogger.cpp ../../cmn
vpath utils.cpp ..

# Our includes relative to current directory: please keep this list one per line

PM_INC_DIRS := \
	.. \
	../../../../../powrmart/sdk/include \
	$(DD_ODBCHOME)/include \
    ../../../../../powrmart/common/cmnmisc \
	../../cmn \
	../../../../../powrmart/server/include
        
DYNLIB_PATH := $(GLOBAL_OUTDIR) \
		$(DD_ODBCHOME)/lib

DYNLIBS := odbc

# Load the rules
include $(PM_CMN_BUILD_DIR)/server_rules.mk

	
