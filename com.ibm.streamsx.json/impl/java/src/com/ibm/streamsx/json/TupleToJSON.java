//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.json;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.encoding.EncodingFactory;
import com.ibm.streams.operator.encoding.JSONEncoding;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@InputPorts(@InputPortSet(cardinality=1, optional=false))
@OutputPorts(@OutputPortSet(cardinality=1, optional=false))
@PrimitiveOperator(name="TupleToJSON", description=TupleToJSON.DESC)
public class TupleToJSON extends AbstractOperator {

	private String jsonStringAttribute = null;
	private static final String defaultJsonStringAttribute = "jsonString";

	private String rootAttribute = null;
	private Type rootAttributeType =null;

	private static Logger l = Logger.getLogger(TupleToJSON.class.getCanonicalName());


	@Parameter(optional=true, 
			description="Name of the output stream attribute where the JSON string will be populated. Default is jsonString")
	public void setJsonStringAttribute(String value) {
		this.jsonStringAttribute = value;
	}
	@Parameter(optional=true, 
			description="Name of the input stream attribute to be used as the root of the JSON object. Default is the input tuple.")
	public void setRootAttribute(String value) {
		this.rootAttribute = value;
	}

	@Override
	public void initialize(OperatorContext op) throws Exception {
		super.initialize(op);

		StreamSchema ssop = getOutput(0).getStreamSchema();
		if (jsonStringAttribute == null) {
			// If we haven't set it using an argument, then...
			if (ssop.getAttributeCount() > 1) {
				// when there's more than one attribute, use the default jsonString
				jsonStringAttribute = defaultJsonStringAttribute;
			}
			else {
				// Only one attribute; so it's clear what to use.
				jsonStringAttribute = ssop.getAttribute(0).getName();
			}
		}
		JSONToTuple.verifyAttributeType(getOperatorContext(), ssop, jsonStringAttribute, 
				Arrays.asList(MetaType.RSTRING, MetaType.USTRING));

		StreamSchema ssip = getInput(0).getStreamSchema();
		if(rootAttribute!=null) {
			rootAttributeType = JSONToTuple.verifyAttributeType(getOperatorContext(), ssip, rootAttribute, 
					Arrays.asList(MetaType.TUPLE, MetaType.LIST, MetaType.BLIST, MetaType.SET, MetaType.BSET));
			l.log(TraceLevel.INFO, "Will use source attribute: " + rootAttribute);
		}
	}

	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception 	{
		StreamingOutput<OutputTuple> ops = getOutput(0);
		final String jsonData;
		if(rootAttribute == null) 
			jsonData = convertTuple(tuple);
		else {
			if(rootAttributeType.getMetaType() == MetaType.TUPLE)
				jsonData = convertTuple(tuple.getTuple(rootAttribute));
			else 
				jsonData = convertArray(tuple, rootAttribute);
		}
		OutputTuple op = ops.newTuple();
		op.assign(tuple);//copy over all relevant attributes form the source tuple
        op.setString(jsonStringAttribute, jsonData);

		ops.submit(op);
	}

	protected String convertTuple(Tuple tuple) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return je.encodeAsString(tuple);
	}
	protected String convertArray(Tuple tuple, String attrName) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return ((JSONArray)je.getAttributeObject(tuple, attrName)).serialize();
	}

	static final String DESC = 
			"This operator converts incoming tuples to JSON String." +
			" Note that any matching attributes from the input stream will be copied over to the output." +
			" If an attribute, with the same name as the JSON string output attribute exists in the input stream, " +
			"it will be overwritten by the JSON String that is generated." ;
}
