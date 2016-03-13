package com.ibm.streamsx.json.converters;

import java.io.IOException;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.encoding.EncodingFactory;
import com.ibm.streams.operator.encoding.JSONEncoding;

/**
 * Converts SPL tuples and SPL tuple attributes to String representations of JSON values.  
 */
public class TupleToJSONConverter { 


	/**
	 * Converts an SPL tuple to a String representation of a JSONObject 
	 * @param tuple Tuple to be converted
	 * @return String representation of a JSONObject
	 * @throws IOException If there was a problem converting the SPL tuple
	 */
	public static String convertTuple(Tuple tuple) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return je.encodeAsString(tuple);
	}
	
	/**
	 * Converts an SPL tuple attribute (that must be a list) to a String representation of a JSONArray
	 * @param tuple Tuple containing the attribute to be converted
	 * @param attrName Name of the attribute to convert
	 * @return String representation of a JSON array
	 * @throws IOException If there was a problem converting the SPL tuple attribute
	 */
	public static String convertArray(Tuple tuple, String attrName) throws IOException  {	
		JSONEncoding<JSONObject, JSONArray> je = EncodingFactory.getJSONEncoding();
		return ((JSONArray)je.getAttributeObject(tuple, attrName)).serialize();
	}
	
}
