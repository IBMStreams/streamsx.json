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
import com.ibm.streams.operator.StreamingData.Punctuation;
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
@OutputPorts({@OutputPortSet(cardinality=1, optional=false), @OutputPortSet(cardinality=1, optional=true)})
@Libraries(value="@STREAMS_INSTALL@/ext/lib/JSON4J.jar")
@PrimitiveOperator(name="JSONToTuple", description=JSONToTuple.DESC)
public class JSONToTuple extends AbstractOperator  
{
	private String dataParamName = "jsonString";
	private MetaType dataParamAttrType=null;
	private Logger l = Logger.getLogger(JSONToTuple.class.getCanonicalName());
	boolean continueOnError = false;
	private String jsonStringAttr = null;
	private MetaType jsonStringAttrType;
	private HashSet<String> copyFields = new HashSet<String>();
	private String targetAttr = null;
	private Type targetAttrType;
	private boolean wasTargetSpecified = false;
	private boolean hasOptionalOut = false;

	@Parameter(optional=true, description="Name of the input stream attribute which contains the JSON string. " +
			"This attribute must be of USTRING or RSTRING type. Default is jsonString")
	public void setJsonStringAttribute(String value) {
		this.dataParamName = value;
	}
	@Parameter(optional=true, description="Name of the output stream attribute which should be populated with the incoming JSON string. " +
			"This attribute must be of USTRING or RSTRING type. Default is to ignore.")
	public void setJsonStringOutputAttribute(String value) {
		this.jsonStringAttr = value;
	}
	@Parameter(optional=true, description="Name of the output stream attribute which should be considered as the root of the JSON tuple to be populated. " +
			"Default is the entite tuple root.")
	public void setTargetAttribute(String value) {
		this.targetAttr = value;
		wasTargetSpecified=true;
	}
	@Parameter(optional=true, cardinality=-1, description="Name of the input stream attributes to copy over to the output stream")
	public void setCopyAttribute(List<String> value) {
		copyFields.addAll(value);
	}
	@Parameter(optional=true, description=
			"Ignore any JSON parsing errors." +
			"If the optional output port is enabled, then this parameter is ignored. JSON that cannot be parsed is sent on the optional output port." +
			"Default is false where the operator will fail if it cannot convert any JSON values properly.")
	public void ignoreParsingError(boolean value) {
		continueOnError = value;
	}

	@Override
	public void initialize(OperatorContext op) throws Exception {
		super.initialize(op);


		StreamSchema ssOp0 = op.getStreamingOutputs().get(0).getStreamSchema();
		StreamSchema ssIp0 = op.getStreamingInputs().get(0).getStreamSchema();
		hasOptionalOut = op.getStreamingOutputs().size() > 1;
		
		List<MetaType> types  = Arrays.asList(MetaType.RSTRING, MetaType.USTRING);
		dataParamAttrType =verifyAttributeType(op,  ssIp0, dataParamName, types);

		if(jsonStringAttr!=null) {
			jsonStringAttrType  = verifyAttributeType(op, ssOp0, jsonStringAttr, types);
		}

		if(wasTargetSpecified) {
			verifyAttributeType(op, ssOp0, targetAttr, Arrays.asList(MetaType.TUPLE));
			targetAttrType = ssOp0.getAttribute(targetAttr).getType();

			if(copyFields.size()==0) {
				for(int i=0;i<ssIp0.getAttributeCount();i++) {
					Attribute ipat = ssIp0.getAttribute(i);
					if(ipat.getName().equals(targetAttr)) continue;
					Attribute opat = ssOp0.getAttribute(ipat.getName());
					if(isCompatible(ipat.getName(), ipat, opat,l)) {
						copyFields.add(ipat.getName());
					}
				}
			}
			l.log(TraceLevel.INFO, "Will populate target field: " + targetAttr);
		}

		if(copyFields.size()>0) {
			for(String f : copyFields) {
				if(targetAttr!=null && f.equals(targetAttr)) continue;
				isCompatibleExcep(f, ssIp0.getAttribute(f), ssOp0.getAttribute(f),l);
			}
		}
		if(hasOptionalOut) {
			if(!ssIp0.equals(op.getStreamingOutputs().get(1).getStreamSchema()))
				throw new Exception("Schemas of input port 0 and output port 1 do not match.");
		}

	}

	static void isCompatibleExcep(String aname, Attribute ipat, Attribute opat, Logger l) throws Exception {
		l.log(TraceLevel.DEBUG, "Checking attribute \"" + aname + "\" for compatibility");
		if(opat == null)
			throw new Exception("Attribute \""+  aname + "\" not found in schema for output port 0");
		if(ipat == null)
			throw new Exception("Attribute \""+  aname + "\" not found in schema for input port 0");
		if(opat.getType().getMetaType() != ipat.getType().getMetaType()) 
			throw new Exception("Attribute \"" + aname + "\" does not have the same type in input port 0 and output port 0");
		if(opat.getType().getMetaType() == MetaType.TUPLE) {
			if(! ((TupleType)opat.getType()).getTupleSchema().equals(((TupleType)ipat.getType()).getTupleSchema()))
				throw new Exception("Attribute \"" + aname + "\" does not have the same tuple schema in input port 0 and output port 0. \nInput Schema=" 
			+ ((TupleType)ipat.getType()).getTupleSchema() 
						+ "\nOutput Schema= " + ((TupleType)opat.getType()).getTupleSchema());
		}
	}
	static boolean isCompatible(String aname, Attribute ipat, Attribute opat, Logger l) {

		try {
			isCompatibleExcep(aname, ipat, opat,l);
			return true;
		}catch(Exception e) {
			//doesnt match
		}
		return false;
	}


	static MetaType verifyAttributeType(OperatorContext op, StreamSchema ss, String attributeName, List<MetaType> types) 
			throws Exception {
		Attribute attr = ss.getAttribute(attributeName);
		if(attr == null) {
			throw new Exception("Attribute \""+attributeName+"\" must be specified");
		}
		MetaType rtype = attr.getType().getMetaType();
		if(types != null && types.size() > 0) {
			for(MetaType mt : types ) {
				if(mt == rtype) {
					return rtype;
				}
			}
			throw new Exception("Attribute \""+attributeName+"\" must be one of the following types: " + types.toString());
		}
		return rtype;
	}

	public synchronized void process(StreamingInput<Tuple> stream, Tuple tuple) throws Exception 	{

		StreamingOutput<OutputTuple> ops = getOutput(0);

		String str = null;
		if(dataParamAttrType == MetaType.RSTRING) {
			str = ((RString)tuple.getObject(dataParamName)).getString();
		}
		else {
			str = tuple.getString(dataParamName);
		} 
		OutputTuple op = ops.newTuple();
		if(str!=null && str.length()>0) {
			if(l.isLoggable(TraceLevel.INFO))
				l.log(TraceLevel.INFO, "Converting Data Size= " + str.length());
			try {
				JSONObject obj = JSONObject.parse(str);
				if(targetAttr == null) {
					Tuple tup = jsonToTuple(obj, op.getStreamSchema(), true);
					if(tup!=null) 
						op.assign(tup);
				}
				else {
					Tuple tup = jsonToTuple(obj, ((TupleType)targetAttrType).getTupleSchema(), false);
					if(tup!=null)
						op.setObject(targetAttr, tup);
				}
			}catch(Exception e) {
				l.log(TraceLevel.ERROR, "Error Converting String: " + str, e);
				if(!hasOptionalOut && !continueOnError)
					throw e;
				if(hasOptionalOut) {
					StreamingOutput<OutputTuple> op1 = getOutput(1);
					OutputTuple otup = op1.newTuple();
					otup.assign(tuple);
					op1.submit(otup);
				}
			}
			if(copyFields.size() > 0) {
				for(String f : copyFields)
					op.setObject(f, tuple.getObject(f));
			}

			if(jsonStringAttr!= null) {
				if(jsonStringAttrType == MetaType.RSTRING) {
					op.setObject(jsonStringAttr, new RString(str));
				}
				else {
					op.setObject(jsonStringAttr, str);
				}
			}
		}
		else {
			if(l.isLoggable(TraceLevel.INFO)) 
				l.log(TraceLevel.INFO, "No JSON data found");
		}
		if(l.isLoggable(TraceLevel.INFO)) 
			l.log(TraceLevel.INFO, "Sending Tuple");
		ops.submit(op);
	}

	private Object jsonToAttribute (String name, Type type, Object obj, Type parentType) throws Exception {

		if(l.isLoggable(TraceLevel.DEBUG)) {
			l.log(TraceLevel.DEBUG, "Converting: " + name + " -- " + type.toString());
		}

		try {
			switch(type.getMetaType()) {
			case INT8:
			case UINT8:
				return Byte.parseByte(obj.toString());
			case INT16:
			case UINT16:
				return Short.parseShort(obj.toString());
			case INT32:
			case UINT32:
				return Integer.parseInt(obj.toString());
			case INT64:
			case UINT64:
				return Long.parseLong(obj.toString());
			case USTRING:
				return obj.toString();
			case BOOLEAN:
				return Boolean.parseBoolean(obj.toString());
			case FLOAT32:
				return Float.parseFloat(obj.toString());
			case FLOAT64:
				return Double.parseDouble(obj.toString());
			case DECIMAL32:
			case DECIMAL64:
			case DECIMAL128:
				return new java.math.BigDecimal(obj.toString());

			case BSTRING:
			case RSTRING:
				return new RString(obj.toString());

			case LIST:
			case BLIST:
			{
				if(!(((CollectionType)type).getElementType()).getMetaType().isCollectionType() &&
					(parentType == null || !parentType.getMetaType().isCollectionType())) {
					return arrayToCollection(name, (JSONArray) obj, type);
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
				return jsonToTuple((JSONObject)obj, ((TupleType)type).getTupleSchema(), false);
			
			case TIMESTAMP:
				return Timestamp.getTimestamp(Double.parseDouble(obj.toString()));
				
			//TODO -- not yet supported types
			case BLOB:
			case MAP:
			case BMAP:
			case COMPLEX32:
			case COMPLEX64:
			default:
				if(l.isLoggable(TraceLevel.INFO))
					l.log(TraceLevel.INFO, "Ignoring unsupported field: " + name + ", of type: " + type);
				break;
			}
		}catch(Exception e) {
			l.log(TraceLevel.ERROR, "Error converting attribute: Exception: " + e);
			throw e;
		}
		return null;
	}

	private void arrayToCollection(String name, Collection<Object>lst, JSONArray jarr, Type ptype) throws Exception 	{
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

	private Object arrayToCollection(String name, JSONArray jarr, Type ptype) throws Exception {
		if(l.isLoggable(TraceLevel.DEBUG)) {
			l.log(TraceLevel.DEBUG, "Creating Array: " + name);
		}
		CollectionType ctype = (CollectionType) ptype;
		int arrsize = jarr.size();
		@SuppressWarnings("unchecked")
		Iterator<Object> jsonit = jarr.iterator();
		int cnt=0;
		String cname = "List: " + name;

		//    Collection lst = null;
		switch(ctype.getElementType().getMetaType()) {
		case INT8:
		case UINT8: 
		{
			byte[] arr= new byte[arrsize];
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				if(obj!=null)
					arr[cnt++] = (Byte)obj;
			}
			return arr;
		} 
		case INT16:
		case UINT16:
		{
			short[] arr= new short[arrsize];
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				arr[cnt++] = obj==null ? 0 :  (Short)obj;
			}
			return arr;
		} 
		case INT32:
		case UINT32:
		{
			int[] arr= new int[arrsize];
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				arr[cnt++] = obj==null ? 0 :  (Integer)obj;
			}
			return arr;
		} 

		case INT64:
		case UINT64:
		{
			long[] arr= new long[arrsize];
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				arr[cnt++] = obj==null ? 0 :  (Long)obj;
			}
			return arr;
		} 

		case BOOLEAN:
		{
			boolean[] arr= new boolean[arrsize];
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				arr[cnt++] = obj==null ? false :  (Boolean)obj;
			}
			return arr;
		} 

		case FLOAT32:
		{
			float[] arr= new float[arrsize];
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				arr[cnt++] = obj==null ? 0 :  (Float)obj;
			}
			return arr;
		} 

		case FLOAT64:
		{
			double[] arr= new double[arrsize];
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				arr[cnt++] = obj==null ? 0 :  (Double)obj;
			}
			return arr;
		} 

		case USTRING:
		{
			String[] arr= new String[arrsize];
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				if(obj!=null)
					arr[cnt++] = (String)obj;
			}
			return arr;
		} 

		case BSTRING:
		case RSTRING:
		{
			List<RString> lst = new ArrayList<RString>(); 
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				if(obj != null)
					lst.add((RString)obj);
			}
			return lst;
		}

		case TUPLE:
		{
			List<Tuple> lst = new ArrayList<Tuple>(); 
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
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
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
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
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
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
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
				if(obj != null)
					lst.add((BigDecimal)obj);
			}
			return lst;
		}
		case TIMESTAMP:
		{
			List<Timestamp> lst = new ArrayList<Timestamp>(); 
			while(jsonit.hasNext()) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonit.next(), ctype.getElementType());
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

	private Tuple jsonToTuple(JSONObject jbase, StreamSchema schema, boolean isMain) throws Exception 
	{
		Map<String, Object> attrmap = new HashMap<String, Object>();
		Iterator<Attribute> iter = schema.iterator();
		while(iter.hasNext()) {
			Attribute attr = iter.next();
			String name = attr.getName();
			if(isMain && copyFields.contains(name)) continue;//dont populate copy fields
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
				l.log(TraceLevel.ERROR, "Error converting object: "  
						+ name + ", Exception: " + e);
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
			" Limitations:" +
			" BLOB, MAP and COMPLEX attribute types are not supported in the output tuple schema and will be ignored."
			;
}
