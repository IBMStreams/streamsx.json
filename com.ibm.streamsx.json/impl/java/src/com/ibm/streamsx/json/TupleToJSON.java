//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.json;

import java.io.IOException;
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
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.RString;

@InputPorts(@InputPortSet(cardinality=1, optional=false))
@OutputPorts(@OutputPortSet(cardinality=1, optional=false,windowPunctuationOutputMode=WindowPunctuationOutputMode.Preserving))
@PrimitiveOperator(name="TupleToJSON", description=TupleToJSON.DESC)
public class TupleToJSON extends AbstractOperator {

	private String dataParamName = "jsonString";
	private Type dtype = null;

	private String sourceAttr = null;

	private static Logger l = Logger.getLogger(TupleToJSON.class.getCanonicalName());


	@Parameter(optional=true, description="Name of the output stream attribute where the JSON string will be populated. Default is jsonString")
	public void setJsonStringAttribute(String value) {
		this.dataParamName = value;
	}
	@Parameter(optional=true, description="Name of the input stream attribute to be used as the root of the JSON object. Default is the input tuple.")
	public void setRootAttribute(String value) {
		this.sourceAttr = value;
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
				throw new Exception("Input stream attribute \"" + sourceAttr + "\" must be of type TUPLE.");

			l.log(TraceLevel.INFO, "Will use source field attribute: " + sourceAttr);

		}

	}

	public synchronized void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception 	{
		if(l.isLoggable(TraceLevel.DEBUG))
			l.log(TraceLevel.DEBUG, "Input Tuple: " + tuple.toString());
		StreamingOutput<OutputTuple> ops = getOutput(0);
		String jsonData = null;
		if(sourceAttr == null) 
			jsonData = convertTuple(tuple);
		else 
			jsonData = convertTuple((Tuple)tuple.getObject(sourceAttr));

		OutputTuple op = ops.newTuple();
		op.assign(tuple);//copy over all relevant attributes form the source tuple
		
		if(dtype.getMetaType() == MetaType.RSTRING)
			op.setObject(dataParamName, new RString(jsonData.getBytes()));
		else 
			op.setObject(dataParamName, jsonData);
		
		if(l.isLoggable(TraceLevel.DEBUG))
			l.log(TraceLevel.DEBUG, "Output Tuple: " + op.toString());

		ops.submit(op);
	}

	protected String convertTuple(Tuple tuple) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return je.encodeAsString(tuple);
	}


	static final String DESC = 
			"This operator converts incoming tuples to JSON String." +
			"Note that any matching attributes from the input stream will be copied over to the output. " +
			"If an attribute, with the same name as the JSON string output attribute, exists in the input stream then it will be overwritten by the JSON String that is generated." ;
}
