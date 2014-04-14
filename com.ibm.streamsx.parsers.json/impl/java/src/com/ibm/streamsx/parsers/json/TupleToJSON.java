//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.parsers.json;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.StreamingData.Punctuation;
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
import com.ibm.streams.operator.types.RString;

@InputPorts(@InputPortSet(cardinality=1, optional=false))
@OutputPorts(@OutputPortSet(cardinality=1, optional=false))
@PrimitiveOperator(name="TupleToJSON", description=TupleToJSON.DESC)
public class TupleToJSON extends AbstractOperator {

	private String dataParamName = "jsonString";
	private Type dtype = null;

	String sourceAttr = null;
	HashSet<String> copyFields = new HashSet<String>();

	private static Logger l = Logger.getLogger(TupleToJSON.class.getCanonicalName());


	@Parameter(optional=true, description="Name of the output stream attribute where the JSON string will be populated. Default is jsonString")
	public void setJsonStringAttribute(String value) {
		this.dataParamName = value;
	}
	@Parameter(optional=true, description="Name of the input stream attribute to be used as the root of the JSON object. Default is the input tuple.")
	public void setRootAttribute(String value) {
		this.sourceAttr = value;
	}
	@Parameter(optional=true, cardinality=-1, description="Name of the input stream attributes to copy over to the output stream. ")
	public void setCopyAttribute(List<String> value) {
		copyFields.addAll(value);
	}


	@Override
	public void initialize(OperatorContext op) throws Exception {
		super.initialize(op);


		StreamSchema ssop = op.getStreamingOutputs().get(0).getStreamSchema();

		if(ssop.getAttribute(dataParamName) == null ||
				(ssop.getAttribute(dataParamName).getType().getMetaType() != MetaType.RSTRING &&
				ssop.getAttribute(dataParamName).getType().getMetaType() != MetaType.USTRING
						)) {
			throw new Exception(
					"Attribute \"" + dataParamName + "\" of type \"" + MetaType.RSTRING + "\" or \"" + MetaType.USTRING + 
					"\" must be specified.");
		}
		dtype = ssop.getAttribute(dataParamName).getType();



		StreamSchema ssip = op.getStreamingInputs().get(0).getStreamSchema();
		if(sourceAttr!=null) {
			if(ssip.getAttribute(sourceAttr) == null || 
					ssop.getAttribute(sourceAttr).getType().getMetaType() != MetaType.TUPLE)
				throw new Exception("Attribute \"" + sourceAttr + "\" of type TUPLE must be specified in input stream.");

			if(copyFields.size()==0) {
				for(int i=0;i<ssip.getAttributeCount();i++) {
					Attribute ipat = ssip.getAttribute(i);
					if(ipat.getName().equals(dataParamName)) continue;
					Attribute opat = ssop.getAttribute(ipat.getName());
					if(JSONToTuple.isCompatible(ipat.getName(), ipat, opat,l)) {
						copyFields.add(ipat.getName());
					}
				}
			}
			l.log(TraceLevel.INFO, "Will use source field attribute: " + sourceAttr);

		}

		if(copyFields.size()>0) {
			for(String f : copyFields) {
				JSONToTuple.isCompatibleExcep(f, ssip.getAttribute(f), ssop.getAttribute(f), l);
			}
			l.log(TraceLevel.INFO, "Copy Fields: " + copyFields);
		}

	}

	public synchronized void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception 	{
		StreamingOutput<OutputTuple> ops = getOutput(0);
		String jsonData = null;
		if(sourceAttr == null) 
			jsonData = convertTuple(tuple);
		else 
			jsonData = convertTuple((Tuple)tuple.getObject(sourceAttr));

		OutputTuple op = ops.newTuple();
		if(dtype.getMetaType() == MetaType.RSTRING)
			op.setObject(dataParamName, new RString(jsonData.getBytes()));
		else 
			op.setObject(dataParamName, jsonData);

		if(copyFields.size() > 0) {
			for(String f : copyFields)
				op.setObject(f, tuple.getObject(f));
		}

		ops.submit(op);
	}

	protected String convertTuple(Tuple tuple) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return je.encodeAsString(tuple);
	}

	@Override
	public synchronized void processPunctuation(StreamingInput<Tuple> stream,
			Punctuation mark) throws Exception {
		getOperatorContext().getStreamingOutputs().get(0).punctuate(mark);
	}
	
	static final String DESC = 
			"This operator converts incoming tuples to JSON String." ;
}
