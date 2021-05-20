#Makefile for generating the Netezza message catalog header files.
#
# Copyright (c) 2008 Informatica Corporation.  This file contains material
# proprietary to Informatica Corporation and may not be copied or distributed
# in any form without the written permission of Informatica Corporation.
#


# Load the settings
PM_CMN_BUILD_DIR := ../../../../powrmart/pmbuild/bin
PM_BUILD_DIR     := ../../server/build
PM_TOOLS_DIR     := ../../../../powrmart/tools
include $(PM_CMN_BUILD_DIR)/settings.mk

# Build message catalog headers
all: msghdr
	test -f ./rdr_msg.h && mv -f ./rdr_msg.h ../Reader/. || true
	test -f ./wrt_msg.h && mv -f ./wrt_msg.h ../Writer/. || true
	test -f ./cmn_msg.h && mv -f ./cmn_msg.h ../cmn/. || true

# Generate the catalog files in this directory
PM_MSGHDR_DIR := .

# All the server message files in the $(PM_MSG_LANG) directory: please keep
# this list sorted and one per line
PM_MSGFILES := \
    rdr \
    wrt \
    cmn \

# Load the rules
include $(PM_CMN_BUILD_DIR)/server_rules.mk


