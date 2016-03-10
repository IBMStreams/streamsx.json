package com.ibm.streamsx.json.converters;

import java.io.IOException;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.EncodingFactory;
import com.ibm.streams.operator.encoding.JSONEncoding;

public class TupleToJSONConverter {


	/**
	 * Converts an SPL tuple to a String representation of a JSONObject 
	 * @param tuple Tuple to be converted
	 * @return String representation of a JSON object
	 * @throws IOException 
	 */
	public static String convertTuple(Tuple tuple) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return je.encodeAsString(tuple);
	}
	
	/**
	 * Converts an SPL tuple to a String representation of a JSONArray
	 * @param tuple Tuple to be converted
	 * @param attrName Name of the attribute to convert
	 * @return String representation of a JSON object
	 * @throws IOException
	 */
	public static String convertArray(Tuple tuple, String attrName) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return ((JSONArray)je.getAttributeObject(tuple, attrName)).serialize();
	}
	
}
