#
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************
#

sc=${STREAMS_INSTALL}/bin/sc
args=-t ../com.ibm.streamsx.json --data-directory=data
ns=com.ibm.streamsx.json.tests
outputdir=./output

tests=BasicTest ListTest SetOfListTest InputSpecificationTest RootAttributeTest

rntest=./scripts/testRunner.sh
ftest=./scripts/expectFail.sh

all: BasicTest ListTest SetOfListTest NullBasicTest RecordArrayListTest InputSpecificationTest RootAttributeTest CompileFailtest EmptyStringTest
	@echo "Tests Passed"

compile: ${tests}

%Test:
	${sc} -T ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@
###	${outputdir}/$@/bin/standalone -t 5
	${rntest} ${outputdir}/$@/bin/standalone 


%Failtest:
	${ftest} ${sc} -T ${args} --output-directory ${outputdir}/$@ -M ${ns}::$@ 

clean:
	rm -rf ${outputdir}
