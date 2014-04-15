#
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************
#

sc=${STREAMS_INSTALL}/bin/sc
args=-t ../com.ibm.streamsx.json
ns=com.ibm.streamsx.json.tests
outputdir=./output

tests=BasicTest ListTest SetOfListTest

rntest=./scripts/testRunner.sh

all: BasicTest ListTest SetOfListTest NullBasicTest


compile: ${tests}

%Test:
	${sc} -T ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@
	${rntest} ${outputdir}/$@/bin/standalone

clean:
	rm -rf ${outputdir}
