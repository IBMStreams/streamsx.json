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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.ibm.streams.operator.Type.MetaType;
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
	private String jsonStringAttribute = "jsonString";
	private Logger l = Logger.getLogger(JSONToTuple.class.getCanonicalName());
	boolean ignoreParsingError = false;
	private String jsonStringOutputAttribute = null;
	private String targetAttribute = null;
	private Type targetAttrType;
	private boolean wasTargetSpecified = false;
	private boolean hasOptionalOut = false;

	@Parameter(optional=true, description="Name of the input stream attribute which contains the JSON string. " +
			"This attribute must be of USTRING or RSTRING type. Default is jsonString")
	public void setJsonStringAttribute(String value) {
		this.jsonStringAttribute = value;
	}
	@Parameter(optional=true, description="Name of the output stream attribute which should be populated with the incoming JSON string. " +
			"This attribute must be of USTRING or RSTRING type. Default is to ignore.")
	public void setJsonStringOutputAttribute(String value) {
		this.jsonStringOutputAttribute = value;
	}
	@Parameter(optional=true, description="Name of the output stream attribute which should be considered as the root of the JSON tuple to be populated. " +
			"Default is the entite tuple root.")
	public void setTargetAttribute(String value) {
		this.targetAttribute = value;
		wasTargetSpecified=true;
	}
	@Parameter(optional=true, description=
			"Ignore any JSON parsing errors." +
			"If the optional output port is enabled, then this parameter is ignored. JSON that cannot be parsed is sent on the optional output port." +
			"Default is false where the operator will fail if the JSON cannot be parsed.")
	public void ignoreParsingError(boolean value) {
		ignoreParsingError = value;
	}

	@Override
	public void initialize(OperatorContext op) throws Exception {
		super.initialize(op);


		StreamSchema ssOp0 = getOutput(0).getStreamSchema();
		StreamSchema ssIp0 = getInput(0).getStreamSchema();
		hasOptionalOut = op.getStreamingOutputs().size() > 1;
		
		List<MetaType> types  = Arrays.asList(MetaType.RSTRING, MetaType.USTRING);
		verifyAttributeType(op,  ssIp0, jsonStringAttribute, types).getMetaType();

		if(jsonStringOutputAttribute!=null) {
			verifyAttributeType(op, ssOp0, jsonStringOutputAttribute, types).getMetaType();
		}

		if(wasTargetSpecified) {
			targetAttrType = verifyAttributeType(op, ssOp0, targetAttribute, Arrays.asList(MetaType.TUPLE));
			l.log(TraceLevel.INFO, "Will populate target field: " + targetAttribute);
		}

		if(hasOptionalOut) {
			if(!ssIp0.equals(op.getStreamingOutputs().get(1).getStreamSchema()))
				throw new Exception("Schemas of input port 0 and output port 1 do not match.");
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

	public void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception 	{

		StreamingOutput<OutputTuple> ops = getOutput(0);
		OutputTuple op = ops.newTuple();
		op.assign(tuple);//copy over any relevant attributes

		String str = tuple.getString(jsonStringAttribute);
		if(str.length()>0) {
			if(l.isLoggable(TraceLevel.DEBUG))
				l.log(TraceLevel.DEBUG, "Converting JSON: " + str);
			try {
				JSONObject obj = JSONObject.parse(str);
				if(targetAttribute == null) {
					Tuple tup = jsonToTuple(obj, op.getStreamSchema());
					if(tup!=null) 
						op.assign(tup);
				}
				else {
					Tuple tup = jsonToTuple(obj, ((TupleType)targetAttrType).getTupleSchema());
					if(tup!=null)
						op.setTuple(targetAttribute, tup);
				}
			}catch(Exception e) {
				l.log(TraceLevel.ERROR, "Error Converting String: " + str, e);
				if(!hasOptionalOut && !ignoreParsingError)
					throw e;
				if(hasOptionalOut) {
					StreamingOutput<OutputTuple> op1 = getOutput(1);
					OutputTuple otupErr = op1.newTuple();
					otupErr.assign(tuple);
					op1.submit(otupErr);
				}
				return;
			}

			if(jsonStringOutputAttribute!= null) {
				op.setString(jsonStringOutputAttribute, str);
			}
		}
		else {
			if(l.isLoggable(TraceLevel.INFO)) 
				l.log(TraceLevel.INFO, "No JSON data found");
		}
		
		ops.submit(op);
	}

	private Object jsonToAttribute (String name, Type type, Object obj, Type parentType) throws Exception {

		if(l.isLoggable(TraceLevel.DEBUG)) {
			l.log(TraceLevel.DEBUG, "Converting: " + name + " -- " + type.toString());
		}
		if(obj == null) return null;
		try {
			switch(type.getMetaType()) {
			case INT8:
			case UINT8:
				if(obj instanceof Number)
					return ((Number)obj).byteValue();
				return Byte.parseByte(obj.toString());
			case INT16:
			case UINT16:
				if(obj instanceof Number)
					return ((Number)obj).shortValue();
				return Short.parseShort(obj.toString());
			case INT32:
			case UINT32:
				if(obj instanceof Number)
					return ((Number)obj).intValue();
				return Integer.parseInt(obj.toString());
			case INT64:
			case UINT64:
				if(obj instanceof Number)
					return ((Number)obj).longValue();
				return Long.parseLong(obj.toString());
			case BOOLEAN:
				if(obj instanceof Boolean)
					return (Boolean)obj;
				return Boolean.parseBoolean(obj.toString());
			case FLOAT32:
				if(obj instanceof Number)
					return ((Number)obj).floatValue();
				return Float.parseFloat(obj.toString());
			case FLOAT64:
				if(obj instanceof Number)
					return ((Number)obj).doubleValue();
				return Double.parseDouble(obj.toString());
			case DECIMAL32:
			case DECIMAL64:
			case DECIMAL128:
				return new java.math.BigDecimal(obj.toString());

			case USTRING:
				return obj.toString();
			case BSTRING:
			case RSTRING:
				return new RString(obj.toString());

			case LIST:
			case BLIST:
			{
				//depending on the case, the java types for lists can be arrays or collections. 
				if(!(((CollectionType)type).getElementType()).getMetaType().isCollectionType() &&
					(parentType == null || !parentType.getMetaType().isCollectionType())) {
					return arrayToSPLArray(name, (JSONArray) obj, type);
				}
				else {
					List<Object> lst = new ArrayList<Object>();
					arrayToCollection(name, lst, (JSONArray) obj, type);
					return lst;
				}
			}

			case SET:
			case BSET:
			{
				Set<Object> lst = new HashSet<Object>();
				arrayToCollection(name, lst, (JSONArray) obj, type);
				return lst;
			}

			case TUPLE:
				return jsonToTuple((JSONObject)obj, ((TupleType)type).getTupleSchema());
			
			case TIMESTAMP:
				if(obj instanceof Number)
					return Timestamp.getTimestamp(((Number)obj).doubleValue());
				return Timestamp.getTimestamp(Double.parseDouble(obj.toString()));
				
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
		@SuppressWarnings("unchecked")
		Iterator<Object> jsonit = jarr.iterator();
		String cname = lst.getClass().getSimpleName() + ": " + name;
		while(jsonit.hasNext()) {
			Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
		int arrsize = jarr.size();
		@SuppressWarnings("unchecked")
		Iterator<Object> jsonit = jarr.iterator();
		int cnt=0;
		String cname = "List: " + name;

		switch(ctype.getElementType().getMetaType()) {
		case INT8:
		case UINT8: 
		{
			List<Object> lst = new ArrayList<Object>();
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
				if(obj != null) 
					lst.add((String)obj);
			}
			return lst.toArray(new String[lst.size()]);
		} 

		case BSTRING:
		case RSTRING:
		{
			List<RString> lst = new ArrayList<RString>(); 
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
				if(obj != null) 
					lst.add((RString)obj);
			}
			return lst;
		}

		case TUPLE:
		{
			List<Tuple> lst = new ArrayList<Tuple>(); 
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
				if(obj != null) 
					lst.add((Tuple)obj);
			}
			return lst;
		}

		case LIST:
		case BLIST:
		{
			List<Object> lst = new ArrayList<Object>(); 
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
				if(obj != null) 
					lst.add(obj);
			}
			return lst;
		}
		case SET:
		case BSET:
		{
			Set<Object> lst = new HashSet<Object>(); 
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
				if(obj != null) 
					lst.add((BigDecimal)obj);
			}
			return lst;
		}
		case TIMESTAMP:
		{
			List<Timestamp> lst = new ArrayList<Timestamp>(); 
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ptype);
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
		Map<String, Object> attrmap = new HashMap<String, Object>();
		Iterator<Attribute> iter = schema.iterator();
		while(iter.hasNext()) {
			Attribute attr = iter.next();
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
		return schema.getTuple(attrmap);
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
