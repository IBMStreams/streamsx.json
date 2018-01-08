#
# *******************************************************************************
# * Copyright (C)2014, International Business Machines Corporation and *
# * others. All Rights Reserved. *
# *******************************************************************************
#

sc=${STREAMS_INSTALL}/bin/sc
args=-t ../com.ibm.streamsx.json --data-directory=data
#args=-t ${STREAMS_INSTALL}/toolkits --data-directory=data
ns=com.ibm.streamsx.json.tests
outputdir=./output

tests=BasicTest ListTest SetOfListTest InputSpecificationTest RootAttributeTest ReservedKeywordTest

rntest=./scripts/testRunner.sh
ftest=./scripts/expectFail.sh


all: BasicTest ListTest SetOfListTest NullBasicTest RecordArrayListTest InputSpecificationTest RootAttributeTest CompileFailtest EmptyStringTest ReservedKeywordTest Optional_BasicTest Optional_OptionalSetTest Optional_OptionalListTest Optional_ListOfOptionalTest Optional_SetOfOptionalTest Optional_BasicTupleTest Optional_OptionalListOfTupleTest Optional_NF_tupleToJSON_BasicTest Optional_NF_tupleToJSON_OptionalSetTest Optional_NF_tupleToJSON_OptionalListTest Optional_NF_tupleToJSON_ListOfOptionalTest Optional_NF_tupleToJSON_SetOfOptionalTest Optional_NF_tupleToJSON_BasicTupleTest Optional_NF_tupleToJSON_OptionalListOfTupleTest Optional_NF_tupleToJSON_OptionalSetOfTupleTest Optional_NF_toJSON_CombinedTest Optional_NF_mapToJSON_CombinedTest Optional_NF_extractFromJSON_BasicTest Optional_NF_extractFromJSON_OptionalListTest Optional_NF_extractFromJSON_OptionalSetTest Optional_NF_extractFromJSON_BasicTupleTest Optional_NF_extractFromJSON_OptionalListOfTupleTest Optional_NF_extractFromJSON_ListOfOptionalTest  Optional_NF_extractFromJSON_OptionalMapTest Optional_NF_extractFromJSON_MapOfOptionalTest

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
