# Copyright (C) 2016, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: all build clean 


STREAMS_JSON_TOOLKIT ?=../../com.ibm.streamsx.json:$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json
ifdef STREAMS_BUILDSERVICE
    STREAMS_JSON_TOOLKIT = ../toolkits:$(STREAMS_INSTALL)/toolkits
endif
SPLC_FLAGS ?= -a -t $(STREAMS_JSON_TOOLKIT)
SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_CMD_ARGS ?= 
SPL_NS=com.ibm.streamsx.json.sample.stoj

all: build


build:
	$(SPLC) $(SPLC_FLAGS) --output-dir output/SPLToJSONSample  -M $(SPL_NS)::SPLToJSONSample $(SPL_CMD_ARGS)

clean: 
	$(SPLC) -C --output-dir output/SPLToJSONSample -M $(SPL_NS)::SPLToJSONSample
	rm -rf output
