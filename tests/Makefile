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
ftest=./scripts/expectFail.sh

all: BasicTest ListTest SetOfListTest NullBasicTest CompileFailtest
	@echo "Tests Passed"

compile: ${tests}

%Test:
	${sc} -T ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@
	${rntest} ${outputdir}/$@/bin/standalone


%Failtest:
	${ftest} ${sc} -T ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@ 

clean:
	rm -rf ${outputdir}
