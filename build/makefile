#
# Master makefile for Unix builds
#

# Load the settings
PM_CMN_BUILD_DIR := ../../../../powrmart/pmbuild/bin
PM_BUILD_DIR := .
#TIBRV_HOME := $(TIBCO_HOME)/tibrv
include $(PM_CMN_BUILD_DIR)/settings.mk

all: msghdr catfiles lib

# Make in these directories, in this order: please keep this list one per line
PM_MAKE_DIRS := \
	../Reader/build \
	../Writer/build
	
	
	
# Message catalog file and header file generation
NETEZZA_MSGDIR := ../msg

# Generate catalog files in all supported languages
# (Need to set MAKELEVEL to work around problem (bug?) in GNU make on AIX)
PM_MSG_LANGS := en ja
CATFILES := $(foreach l, $(PM_MSG_LANGS), \
	      $(GLOBAL_OUTDIR)/pmnetezza_$(shell echo $(l) | tr '[a-z]' '[A-Z]').cat)

catfiles: $(CATFILES)
$(GLOBAL_OUTDIR)/pmnetezza_%.cat:
	MAKELEVEL=0 \
	PM_MSG_LANG=$(shell echo $* | tr '[A-Z]' '[a-z]') \
	$(MAKE) catfile

# The current language catalog file
CATFILE := $(GLOBAL_OUTDIR)/pmnetezza_$(GC_MSG_LANG)$(CAT_EXT)
catfile: $(CATFILE)

# Build paths to common and server message files
MSG_FILES := $(foreach f, $(shell cd $(NETEZZA_MSGDIR) && ($(MAKE) listmsg | fgrep -v directory) ), $(NETEZZA_MSGDIR)/$(f)) 

# Build the cat file
$(CATFILE): $(MSG_FILES)
	$(SHELL) ./lsdupmsgid $(PM_MSG_LANG)
	$(SHELL) ./lsmsgdiff $(PM_MSG_LANG) en
	-$(MSG_COMPILER) $(MSG_CAT_OPT) $@ $^

msghdr: 
	(cd $(NETEZZA_MSGDIR) && $(MAKE) $(MFLAGS) all)

# Additional files to be cleaned
CLEAN_FILES += $(CATFILES) \
               $(NETEZZA_MSGDIR)/*.h

# Load the rules
include $(PM_CMN_BUILD_DIR)/rules.mk

