//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.TupleAttribute;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;
import com.ibm.streamsx.json.converters.JSONToTupleConverter;
import com.ibm.streamsx.json.converters.TupleTypeVerifier;

@InputPorts(@InputPortSet(cardinality=1, optional=false))
@OutputPorts({
	@OutputPortSet(cardinality=1, optional=false), 
	@OutputPortSet(cardinality=1, optional=true)})
@PrimitiveOperator(name="JSONToTuple", description=JSONToTuple.DESC)
public class JSONToTuple extends AbstractOperator  
{
	private String jsonStringAttribute = null;
	private static final String INPUT_JSON_ATTRIBUTE_PARAM="inputAttribute";
	private static final String defaultJsonStringAttribute = "jsonString";
	private Logger l = Logger.getLogger(JSONToTuple.class.getCanonicalName());
	boolean ignoreParsingError = false;
	private String jsonStringOutputAttribute = null;
	private String targetAttribute = null;
	private Type targetAttrType;
	private boolean wasTargetSpecified = false;
	private boolean hasOptionalOut = false;
	private TupleAttribute<Tuple,String> inputJsonAttribute = null;
	private JSONToTupleConverter converter;
	
	@Parameter(name=INPUT_JSON_ATTRIBUTE_PARAM,optional=true, description="The input stream attribute (not the name of the attribute) which contains the input JSON string.  Replaces jsonStringAttribute.")
	public void setInputJson(TupleAttribute<Tuple,String> in) {
		inputJsonAttribute = in;
	}
	
	@Parameter(optional=true, description="Deprecated.  Use "+INPUT_JSON_ATTRIBUTE_PARAM+" instead. Name of the input stream attribute which contains the JSON string. " +
			"This attribute must be of USTRING or RSTRING type. Default is jsonString")
	public void setJsonStringAttribute(String value) {
		this.jsonStringAttribute = value;
	}
	@Parameter(optional=true, description="Name of the output stream attribute which should be populated with the incoming JSON string. " +
			"This attribute must be of USTRING or RSTRING type. Default is to ignore.")
	public void setJsonStringOutputAttribute(String value) {
		this.jsonStringOutputAttribute = value;
	}
	@Parameter(optional=true, 
			description="Name of the output stream attribute which should be considered as the root of the JSON tuple to be populated. " +
		    "Note that this can only point to a tuple type or collection type attributes (list, set etc). " +
		    "If it points to a list, set etc type attribute, the input JSON is expected to be an array. " +
			"Default is the output tuple root.")
	public void setTargetAttribute(String value) {
		this.targetAttribute = value;
		wasTargetSpecified=true;
	}
	@Parameter(optional=true, description=
			"Ignore any JSON parsing errors." +
			"If the optional output port is enabled, then this parameter is ignored. " +
			"JSON that cannot be parsed is sent on the optional output port. " +
			"Default is false where the operator will fail if the JSON cannot be parsed.")
	public void setIgnoreParsingError(boolean value) {
		ignoreParsingError = value;
	}
	
	@ContextCheck
	public static boolean checkOptionalPortSchema(OperatorContextChecker checker) {
		if(checker.getOperatorContext().getNumberOfStreamingOutputs() == 2) {
			return checker.checkMatchingSchemas(
					checker.getOperatorContext().getStreamingInputs().get(0), 
					checker.getOperatorContext().getStreamingOutputs().get(1));
		}
		checker.checkExcludedParameters(INPUT_JSON_ATTRIBUTE_PARAM, "jsonStringAttribute");
		checker.checkExcludedParameters("jsonStringAttribute", INPUT_JSON_ATTRIBUTE_PARAM);
		return true;
	}

	@Override
	public void initialize(OperatorContext op) throws Exception {
		super.initialize(op);
		converter = new JSONToTupleConverter();

		StreamSchema ssOp0 = getOutput(0).getStreamSchema();
		StreamSchema ssIp0 = getInput(0).getStreamSchema();
		hasOptionalOut = op.getStreamingOutputs().size() > 1;

		List<MetaType> types  = Arrays.asList(MetaType.RSTRING, MetaType.USTRING);
		if (inputJsonAttribute == null && jsonStringAttribute == null) {
			if (ssIp0.getAttributeCount() == 1) {
				jsonStringAttribute = ssIp0.getAttribute(0).getName();
			}
			else {
				jsonStringAttribute = defaultJsonStringAttribute;
			}
		}
		if (inputJsonAttribute == null) {
			TupleTypeVerifier.verifyAttributeType(op,  ssIp0, jsonStringAttribute, types).getMetaType();
		}
		if(jsonStringOutputAttribute!=null) {
			TupleTypeVerifier.verifyAttributeType(op, ssOp0, jsonStringOutputAttribute, types).getMetaType();
		}

		if(wasTargetSpecified) {
			targetAttrType = TupleTypeVerifier.verifyAttributeType(op, ssOp0, targetAttribute, 
	 					Arrays.asList(MetaType.TUPLE, MetaType.LIST, MetaType.BLIST, MetaType.SET, MetaType.BSET));
			l.log(TraceLevel.INFO, "Will populate target field: " + targetAttribute);
		}
	}

	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception {
		String jsonInput;
		if (inputJsonAttribute != null) {
			jsonInput = inputJsonAttribute.getValue(tuple);
		}
		else {
			jsonInput = tuple.getString(jsonStringAttribute);
		}
		StreamingOutput<OutputTuple> ops = getOutput(0);
		OutputTuple op = ops.newTuple();
		op.assign(tuple);//copy over any relevant attributes

		if(l.isLoggable(TraceLevel.DEBUG))
			l.log(TraceLevel.DEBUG, "Converting JSON: " + jsonInput);

		try {
			if( targetAttribute != null &&  targetAttrType.getMetaType() != MetaType.TUPLE) {
				//in this mode, the incoming json string is expected to be an array
				JSONArray jsonArr = JSONArray.parse(jsonInput);
				Object collectionObj = converter.jsonToAttribute(targetAttribute, targetAttrType, jsonArr, null);
				if(collectionObj != null)
					op.setObject(targetAttribute, collectionObj);
			}
			else {
				JSONObject jsonObj = JSONObject.parse(jsonInput);
				if(targetAttribute == null) {
					final Map<String, Object> attributeMap = converter.jsonToAtributeMap(jsonObj, op.getStreamSchema());
					if (!attributeMap.isEmpty())
					    for (String name : attributeMap.keySet())
					    	op.setObject(name, attributeMap.get(name));
				}
				else {
					Tuple tup = converter.jsonToTuple(jsonObj, ((TupleType)targetAttrType).getTupleSchema());
					if(tup!=null)
						op.setTuple(targetAttribute, tup);
				}
			}
			
			
			if(jsonStringOutputAttribute!= null) {
				op.setString(jsonStringOutputAttribute, jsonInput);
			}
			ops.submit(op);

			
		} catch(Exception e) {
			l.log(TraceLevel.ERROR, "Error Converting String: " + jsonInput, e);
			if(!hasOptionalOut && !ignoreParsingError)
				throw e;
			if(hasOptionalOut) {
				StreamingOutput<OutputTuple> op1 = getOutput(1);
				op1.submit(tuple);
			}
		}
	}

	static final String DESC = 
			"This operator converts JSON strings into SPL Tuples. The tuple structure is expected to match the JSON schema." +
					" A subset of the attributes can be specified as well. " +
					" Only those attributes that are present in the Tuple schema and JSON input will be converted. All other attributes will be ignored." +
					" If an invalid JSON string is found in the input, the operator will fail. " +
					" This behavior can be overridden by specifying the optional output port or by specifying the \\\"ignoreParsingError\\\" parameter." +
					" Attributes from the input stream that match those in the output stream will be automatically copied over. " +
					" However, if they also exist in the JSON input, their assigned value will be of that specified in the JSON." +
					" Null values in JSON arrays are ignored. Null values for all other attributes will result in default initializled output attributes. " +
					" Limitations:" +
					" BLOB, MAP and COMPLEX attribute types are not supported in the output tuple schema at this time and will be ignored."
					;
}
