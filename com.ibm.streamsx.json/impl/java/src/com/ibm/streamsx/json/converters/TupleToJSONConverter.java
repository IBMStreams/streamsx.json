package com.ibm.streamsx.json.converters;

import java.io.IOException;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.EncodingFactory;
import com.ibm.streams.operator.encoding.JSONEncoding;

public class TupleToJSONConverter {


	public String convertTuple(Tuple tuple) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return je.encodeAsString(tuple);
	}
	
	public String convertArray(Tuple tuple, String attrName) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return ((JSONArray)je.getAttributeObject(tuple, attrName)).serialize();
	}
	
}
