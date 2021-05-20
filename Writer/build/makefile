#
# Makefile to build the PCNWriter dynamic library.
#


# Load the settings

PM_CMN_BUILD_DIR := ../../../../../powrmart/pmbuild/bin
PM_BUILD_DIR := ../build



include $(PM_CMN_BUILD_DIR)/settings.mk

# The main target of this makefile
all: lib
DYNLIB_ROOT := PCNWriter

COMPILE_DEFINES += PCNWRITER_EXPORTS SQLWCHARSHORT UNICODE _UNICODE

ifeq ($(PMBLD64), y)
    COMPILE_DEFINES += ODBC64
endif


DYNLIB_LINK_OPTIONS += 

# Source file list: please keep this list sorted and one per line
CPP_FILES := \
	msgcatalog \
        pcnwriter_group \
        pcnwriter_init \
        pcnwriter_partition \
	pcnwriter_plugin \
	pcnwriter_target \
	pcnwriter_connection \
	slogger \
	utils
	
# Grab files from respective directories
vpath msgcatalog.cpp ../../cmn
vpath pcnwriter_group.cpp ..
vpath pcnwriter_init.cpp ..
vpath pcnwriter_partition.cpp ..
vpath pcnwriter_plugin.cpp ..
vpath pcnwriter_target.cpp ..
vpath pcnwriter_connection.cpp ..
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

	
