# Copyright (C) 2011,2013,2014, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: all standalone clean 

STREAMS_MSG_TOOLKIT ?=../../com.ibm.streamsx.json
SPLC_FLAGS ?= -a -t $(STREAMS_MSG_TOOLKIT)
SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_CMD_ARGS ?= 
SPL_NS=com.ibm.streamsx.json.sample.jtot

all: standalone


standalone:
	$(SPLC) $(SPLC_FLAGS) --output-dir output/SimpleJsonConvert -T -M $(SPL_NS)::SimpleJsonConvert $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) --output-dir output/JsonArrayConvert  -T -M $(SPL_NS)::JsonArrayConvert $(SPL_CMD_ARGS)

clean: 
	$(SPLC) -C --output-dir output/SimpleJsonConvert -M $(SPL_NS)::SimpleJsonConvert
	$(SPLC) -C --output-dir output/JsonArrayConvert  -M $(SPL_NS)::JsonArrayConvert 
	rm -rf output