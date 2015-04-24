#!/bin/bash
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************

# This is a helper utility to create SPL types based on JSON data
# Note that in cases where the type cannot be determined due to lack of data, 
# an UNKNOWN_TYPE value will be generated


fil=$@

if [ -z "$fil" ]; then
		echo "$0 jsonFile"
		exit 1
fi

if [ -z "${STREAMS_INSTALL}" ]; then
		echo "STREAMS_INSTALL environment variable not set"
		exit 1
fi

if [ ! -e $fil ]; then
		echo "ERROR: File $file does not exist"
		exit 1
fi


jspath=$(dirname $0)/../

if [ ! -e ${jspath}/impl/lib/com.ibm.streamsx.json.jar ]; then
		echo "ERROR: Unable to find ${jspath}/impl/lib/com.ibm.streamsx.json.jar. Perhaps the toolkit needs to be compiled?"
		exit 1
fi

cp=${jspath}/impl/lib/com.ibm.streamsx.json.jar:${STREAMS_INSTALL}/ext/lib/JSON4J.jar

java -cp $cp com.ibm.streamsx.json.JSONMain $fil
