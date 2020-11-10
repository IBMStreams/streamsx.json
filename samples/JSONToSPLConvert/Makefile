# Copyright (C) 2011,2013,2016, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: all build clean 


STREAMS_JSON_TOOLKIT ?=../../com.ibm.streamsx.json:$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json
ifdef STREAMS_BUILDSERVICE
    STREAMS_JSON_TOOLKIT = ../toolkits:$(STREAMS_INSTALL)/toolkits
endif
SPLC_FLAGS ?= -a -t $(STREAMS_JSON_TOOLKIT)
SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_CMD_ARGS ?= 
SPL_NS=com.ibm.streamsx.json.sample.jtos

all: build


build:
	$(SPLC) $(SPLC_FLAGS) --output-dir output/ExtractFromJSON  -M $(SPL_NS)::ExtractFromJSON $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) --output-dir output/QueryJSONBasic  -M $(SPL_NS)::QueryJSONBasic $(SPL_CMD_ARGS)

clean: 
	$(SPLC) -C --output-dir output/ExtractFromJSON -M $(SPL_NS)::ExtractFromJSON
	$(SPLC) -C --output-dir output/QueryJSONBasic  -M $(SPL_NS)::QueryJSONBasic 
	rm -rf output
