# Copyright (C) 2011,2013,2016, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: all build clean 

STREAMS_JSON_TOOLKIT ?=../../com.ibm.streamsx.json:$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json
SPLC_FLAGS ?= -a -t $(STREAMS_JSON_TOOLKIT)
SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_CMD_ARGS ?= 
SPL_NS=com.ibm.streamsx.json.sample.ttoj

all: build


build:
	$(SPLC) $(SPLC_FLAGS) --output-dir output/SimpleTupleConvert  -M $(SPL_NS)::SimpleTupleConvert $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) --output-dir output/JsonArrayConvert  -M $(SPL_NS)::JsonArrayConvert $(SPL_CMD_ARGS)

clean: 
	$(SPLC) -C --output-dir output/SimpleTupleConvert -M $(SPL_NS)::SimpleTupleConvert
	$(SPLC) -C --output-dir output/JsonArrayConvert  -M $(SPL_NS)::JsonArrayConvert 
	rm -rf output