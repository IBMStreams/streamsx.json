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

@InputPorts(@InputPortSet(cardinality=1, optional=false))
@OutputPorts({
	@OutputPortSet(cardinality=1, optional=false), 
	@OutputPortSet(cardinality=1, optional=true)})
@Libraries(value="@STREAMS_INSTALL@/ext/lib/JSON4J.jar")
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
		     verifyAttributeType(op,  ssIp0, jsonStringAttribute, types).getMetaType();
		}
		if(jsonStringOutputAttribute!=null) {
			verifyAttributeType(op, ssOp0, jsonStringOutputAttribute, types).getMetaType();
		}

		if(wasTargetSpecified) {
			targetAttrType = verifyAttributeType(op, ssOp0, targetAttribute, 
	 					Arrays.asList(MetaType.TUPLE, MetaType.LIST, MetaType.BLIST, MetaType.SET, MetaType.BSET));
			l.log(TraceLevel.INFO, "Will populate target field: " + targetAttribute);
		}
	}

	static Type verifyAttributeType(OperatorContext op, StreamSchema ss, String attributeName, List<MetaType> types) 
			throws Exception {
		Attribute attr = ss.getAttribute(attributeName);
		if(attr == null) {
			throw new Exception("Attribute \""+attributeName+"\" must be specified");
		}
		Type t = attr.getType();
		MetaType rtype = attr.getType().getMetaType();
		if(types != null && types.size() > 0) {
			for(MetaType mt : types ) {
				if(mt == rtype) {
					return t;
				}
			}
			throw new Exception("Attribute \""+attributeName+"\" must be one of the following types: " + types.toString());
		}
		return t;
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
				Object collectionObj = jsonToAttribute(targetAttribute, targetAttrType, jsonArr, null);
				if(collectionObj != null)
					op.setObject(targetAttribute, collectionObj);
			}
			else {
				JSONObject jsonObj = JSONObject.parse(jsonInput);
				if(targetAttribute == null) {
					final Map<String, Object> attributeMap = jsonToAtributeMap(jsonObj, op.getStreamSchema());
					if (!attributeMap.isEmpty())
					    for (String name : attributeMap.keySet())
					    	op.setObject(name, attributeMap.get(name));
				}
				else {
					Tuple tup = jsonToTuple(jsonObj, ((TupleType)targetAttrType).getTupleSchema());
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

	private Object jsonToAttribute (String name, Type type, Object jsonObj, Type parentType) throws Exception {

		if(l.isLoggable(TraceLevel.DEBUG)) {
			l.log(TraceLevel.DEBUG, "Converting: " + name + " -- " + type.toString());
		}
		if(jsonObj == null) return null;
		try {
			switch(type.getMetaType()) {
			case INT8:
			case UINT8:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).byteValue();
				return Byte.parseByte(jsonObj.toString());
			case INT16:
			case UINT16:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).shortValue();
				return Short.parseShort(jsonObj.toString());
			case INT32:
			case UINT32:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).intValue();
				return Integer.parseInt(jsonObj.toString());
			case INT64:
			case UINT64:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).longValue();
				return Long.parseLong(jsonObj.toString());
			case BOOLEAN:
				if(jsonObj instanceof Boolean)
					return (Boolean)jsonObj;
				return Boolean.parseBoolean(jsonObj.toString());
			case FLOAT32:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).floatValue();
				return Float.parseFloat(jsonObj.toString());
			case FLOAT64:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).doubleValue();
				return Double.parseDouble(jsonObj.toString());
			case DECIMAL32:
			case DECIMAL64:
			case DECIMAL128:
				return new java.math.BigDecimal(jsonObj.toString());

			case USTRING:
				return jsonObj.toString();
			case BSTRING:
			case RSTRING:
				return new RString(jsonObj.toString());

			case LIST:
			case BLIST:
			{
				//depending on the case, the java types for lists can be arrays or collections. 
				if(!(((CollectionType)type).getElementType()).getMetaType().isCollectionType() &&
						(parentType == null || !parentType.getMetaType().isCollectionType())) {
					return arrayToSPLArray(name, (JSONArray) jsonObj, type);
				}
				else {
					List<Object> lst = new ArrayList<Object>();
					arrayToCollection(name, lst, (JSONArray) jsonObj, type);
					return lst;
				}
			}

			case SET:
			case BSET:
			{
				Set<Object> lst = new HashSet<Object>();
				arrayToCollection(name, lst, (JSONArray) jsonObj, type);
				return lst;
			}

			case TUPLE:
				return jsonToTuple((JSONObject)jsonObj, ((TupleType)type).getTupleSchema());

			case TIMESTAMP:
				if(jsonObj instanceof Number)
					return Timestamp.getTimestamp(((Number)jsonObj).doubleValue());
				return Timestamp.getTimestamp(Double.parseDouble(jsonObj.toString()));

				//TODO -- not yet supported types
			case BLOB:
			case MAP:
			case BMAP:
			case COMPLEX32:
			case COMPLEX64:
			default:
				if(l.isLoggable(TraceLevel.DEBUG))
					l.log(TraceLevel.DEBUG, "Ignoring unsupported field: " + name + ", of type: " + type);
				break;
			}
		}catch(Exception e) {
			l.log(TraceLevel.ERROR, "Error converting attribute: Exception: " + e);
			throw e;
		}
		return null;
	}

	//this is used when a JSON array maps to a SPL collection 
	private void arrayToCollection(String name, Collection<Object>lst, JSONArray jarr, Type ptype) throws Exception {
		CollectionType ctype = (CollectionType) ptype;
		String cname = lst.getClass().getSimpleName() + ": " + name;
		for(Object jsonObj : jarr) {
			Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
			if(obj!=null)
				lst.add(obj);
		}
	}


	//this is used when a JSON array maps to a Java array 
	private Object arrayToSPLArray(String name, JSONArray jarr, Type ptype) throws Exception {
		if(l.isLoggable(TraceLevel.DEBUG)) {
			l.log(TraceLevel.DEBUG, "Creating Array: " + name);
		}
		CollectionType ctype = (CollectionType) ptype;
		int cnt=0;
		String cname = "List: " + name;

		switch(ctype.getElementType().getMetaType()) {
		case INT8:
		case UINT8: 
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) lst.add(obj);
			}

			byte[] arr= new byte[lst.size()];
			for(Object val : lst)
				arr[cnt++] = (Byte)val;
			return arr;
		} 
		case INT16:
		case UINT16:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) lst.add(obj);
			}

			short[] arr= new short[lst.size()];
			for(Object val : lst)
				arr[cnt++] = (Short)val;
			return arr;
		} 
		case INT32:
		case UINT32:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) lst.add(obj);
			}

			int[] arr= new int[lst.size()];
			for(Object val : lst)
				arr[cnt++] = (Integer)val;
			return arr;
		} 

		case INT64:
		case UINT64:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) lst.add(obj);
			}

			long[] arr= new long[lst.size()];
			for(Object val : lst)
				arr[cnt++] = (Long)val;
			return arr;
		} 

		case BOOLEAN:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) lst.add(obj);
			}

			boolean[] arr= new boolean[lst.size()];
			for(Object val : lst)
				arr[cnt++] = (Boolean)val;
			return arr;
		} 

		case FLOAT32:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) lst.add(obj);
			}

			float[] arr= new float[lst.size()];
			for(Object val : lst)
				arr[cnt++] = (Float)val;
			return arr;
		} 

		case FLOAT64:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) lst.add(obj);
			}

			double[] arr= new double[lst.size()];
			for(Object val : lst)
				arr[cnt++] = (Double)val;
			return arr;
		} 

		case USTRING:
		{
			List<String> lst =  new ArrayList<String>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null) 
					lst.add((String)obj);
			}
			return lst.toArray(new String[lst.size()]);
		} 

		case BSTRING:
		case RSTRING:
		{
			List<RString> lst = new ArrayList<RString>(); 
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null) 
					lst.add((RString)obj);
			}
			return lst;
		}

		case TUPLE:
		{
			List<Tuple> lst = new ArrayList<Tuple>(); 
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null) 
					lst.add((Tuple)obj);
			}
			return lst;
		}

		case LIST:
		case BLIST:
		{
			List<Object> lst = new ArrayList<Object>(); 
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null) 
					lst.add(obj);
			}
			return lst;
		}
		case SET:
		case BSET:
		{
			Set<Object> lst = new HashSet<Object>(); 
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null) 
					lst.add(obj);
			}
			return lst;
		}
		case DECIMAL32:
		case DECIMAL64:
		case DECIMAL128:
		{
			List<BigDecimal> lst = new ArrayList<BigDecimal>(); 
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null) 
					lst.add((BigDecimal)obj);
			}
			return lst;
		}
		case TIMESTAMP:
		{
			List<Timestamp> lst = new ArrayList<Timestamp>(); 
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null) 
					lst.add((Timestamp)obj);
			}
			return lst;
		}


		//TODO -- not yet supported types
		case BLOB:
		case MAP:
		case BMAP:
		case COMPLEX32:
		case COMPLEX64:
		default:
			throw new Exception("Unhandled array type: " + ctype.getElementType().getMetaType());
		}

	}

	private Tuple jsonToTuple(JSONObject jbase, StreamSchema schema) throws Exception {		
		return schema.getTuple(jsonToAtributeMap(jbase, schema));
	}
	
	private Map<String, Object> jsonToAtributeMap(JSONObject jbase, StreamSchema schema) throws Exception {
		Map<String, Object> attrmap = new HashMap<String, Object>();
		for(Attribute attr : schema) {
			String name = attr.getName();
			try {
				if(l.isLoggable(TraceLevel.DEBUG)) {
					l.log(TraceLevel.DEBUG, "Checking for: " + name);
				}
				Object childobj = jbase.get(name);
				if(childobj==null) {
					if(l.isLoggable(TraceLevel.DEBUG)) {
						l.log(TraceLevel.DEBUG, "Not Found: " + name);
					}
					continue;
				}
				Object obj = jsonToAttribute(name, attr.getType(), childobj, null);
				if(obj!=null)
					attrmap.put(name, obj);
			}catch(Exception e) {
				l.log(TraceLevel.ERROR, "Error converting object: " + name, e);
				throw e;
			}

		}
		return attrmap;
	}

	static final String DESC = 
			"This operator converts JSON strings into SPL Tuples. The tuple structure is expected to match the JSON schema." +
					" A subset of the attributes can be specified as well. " +
					" Only those attributes that are present in the Tuple schema and JSON input will be converted. All other attributes will be ignored." +
					" If an invalid JSON string is found in the input, the operator will fail. " +
					" This behavior can be overridden by specifying the optional output port or by specifying the \\\"ignoreParsingError\\\" parameter." +
					" Atributes from the input stream that match those in the output stream will be automaticall copied over. " +
					" However, if they also exist in the JSON input, their assigned value will be of that specified in the JSON." +
					" Null values in JSON arrays are ignored. Null values for all other attributes will result in default initializled output attributes. " +
					" Limitations:" +
					" BLOB, MAP and COMPLEX attribute types are not supported in the output tuple schema at this time and will be ignored."
					;
}
