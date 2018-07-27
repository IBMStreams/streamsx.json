package com.ibm.streamsx.json.converters;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streamsx.json.converters.Messages;

/**
 * Converts JSON values to SPL tuples and SPL tuple attributes. 
 */
public class JSONToTupleConverter {

	private static Logger l = Logger.getLogger(JSONToTupleConverter.class.getCanonicalName());
	
	private static String prefixToIgnore = null; // null means prefixToIgnore is disabled
	
	public static void setPrefixToIgnore (String value) {
		prefixToIgnore = value;
	}
	
	/**
	 * Convert JSON value to an SPL tuple attribute value. 
	 * 
	 * @param name The name of the attribute being converted (used for logging purposes).
	 * @param type SPL attribute type that the JSON value is being converted to. 
	 * @param jsonObj JSON value that is being converted (can be either JSONObject or JSONArray).
	 * @param parentType This parameter is used when converting arrays or maps to an SPL tuple attribute. For all other JSON types, can be set to null.
	 * @return Value converted to SPL representation with type {@code type}.
	 * @throws Exception If there was a problem converting the JSON.
	 */
	public static Object jsonToAttribute (String name, Type type, Object jsonObj, Type parentType) throws Exception {

		if (jsonObj == null) return null;
		if (l.isLoggable(TraceLevel.DEBUG)) {
          l.log(TraceLevel.DEBUG, "Converting Java value '" + jsonObj.toString() + "' of type " + jsonObj.getClass().getSimpleName() + " to SPL attribute " + name + " of type " + type.toString()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
		}
		try {
			switch(type.getMetaType()) {
			case BOOLEAN:
				if(jsonObj instanceof Boolean)
					return (Boolean)jsonObj;
				return Boolean.parseBoolean(jsonObj.toString());
			case INT8:
			case UINT8:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).byteValue();
				return jsonObj.toString().isEmpty() ? 0 : Byte.parseByte(jsonObj.toString());
			case INT16:
			case UINT16:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).shortValue();
				return jsonObj.toString().isEmpty() ? 0 : Short.parseShort(jsonObj.toString());
			case INT32:
			case UINT32:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).intValue();
				return jsonObj.toString().isEmpty() ? 0 : Integer.parseInt(jsonObj.toString());
			case INT64:
			case UINT64:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).longValue();
				return jsonObj.toString().isEmpty() ? 0 : Long.parseLong(jsonObj.toString());
			case FLOAT32:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).floatValue();
				return jsonObj.toString().isEmpty() ? 0 : Float.parseFloat(jsonObj.toString());
			case FLOAT64:
				if(jsonObj instanceof Number)
					return ((Number)jsonObj).doubleValue();
				return jsonObj.toString().isEmpty() ? 0 : Double.parseDouble(jsonObj.toString());
			case DECIMAL32:
			case DECIMAL64:
			case DECIMAL128:
                return jsonObj.toString().isEmpty() ? new java.math.BigDecimal(0) : new java.math.BigDecimal(jsonObj.toString());

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
				return Timestamp.getTimestamp(jsonObj.toString().isEmpty() ? 0 : Double.parseDouble(jsonObj.toString()));

				//TODO -- not yet supported types
			case BLOB:
			case MAP:
			case BMAP:
			case COMPLEX32:
			case COMPLEX64:
			default:
				if(l.isLoggable(TraceLevel.DEBUG))
					l.log(TraceLevel.DEBUG, "Ignoring unsupported field: " + name + ", of type: " + type); //$NON-NLS-1$ //$NON-NLS-2$
				break;
			}
		}catch(Exception e) {
			l.log(TraceLevel.ERROR, "Error converting attribute: Exception: " + e); //$NON-NLS-1$
			throw e;
		}
		return null;
	}

	//this is used when a JSON array maps to a SPL collection 
	private static void arrayToCollection(String name, Collection<Object>lst, JSONArray jarr, Type ptype) throws Exception {
		CollectionType ctype = (CollectionType) ptype;
		String cname = lst.getClass().getSimpleName() + ": " + name; //$NON-NLS-1$
		for(Object jsonObj : jarr) {
			Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
			if(obj!=null)
				lst.add(obj);
		}
	}


	//this is used when a JSON array maps to a Java array 
	private static Object arrayToSPLArray(String name, JSONArray jarr, Type ptype) throws Exception {
		if(l.isLoggable(TraceLevel.DEBUG)) {
			l.log(TraceLevel.DEBUG, "Creating Array: " + name); //$NON-NLS-1$
		}
		CollectionType ctype = (CollectionType) ptype;
		int cnt=0;
		String cname = "List: " + name; //$NON-NLS-1$

		MetaType collectionElementMetaType = ctype.getElementType().getMetaType();
		
		switch(collectionElementMetaType) {
		case INT8:
		case UINT8: 
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) lst.add(obj);
			}

			/* SPL list<intxx> attributes
			 * expects assignment of java array type int[]
			 * 
			 * SPL list<uintxx> attributes
			 * expects assignment of java collection type arrayList<Object>
			 * were Object is expected to be java.lang.Byte object
			 * which is returned from jsonToAttribute()
			 * 
			 */
			if (collectionElementMetaType == MetaType.UINT8){
				return lst;
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

			/* list<uintxx> case */
			if (collectionElementMetaType == MetaType.UINT16){
				return lst;
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

			/* list<uintxx> case */
			if (collectionElementMetaType == MetaType.UINT32){
				return lst;
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

			/* list<uintxx> case */
			if (collectionElementMetaType == MetaType.UINT64){
				return lst;
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
		{
			l.log(LogLevel.ERROR, Messages.getString("UNHANDLED_ARRAY_TYPE"), new Object[]{ctype.getElementType().getMetaType()});	//$NON-NLS-1$
			throw new Exception("CDIST0953E Unhandled array type: " + ctype.getElementType().getMetaType()); //$NON-NLS-1$
		}
		}

	}

	/**
	 * Convert a JSONObject to an SPL tuple with the specified schema. The SPL schema must 
	 * contain attribute names that match the JSON key names in the JSONObject. 
	 * 
	 * @param jbase JSON value that is being converted
	 * @param schema Schema of the SPL tuple
	 * @return Value converted to SPL tuple with the specified {@code schema} 
	 * @throws Exception If there was a problem converting the JSONObject.
	 */
	public static Tuple jsonToTuple(JSONObject jbase, StreamSchema schema) throws Exception {		
		return schema.getTuple(jsonToAtributeMap(jbase, schema));
	}
	
	/**
	 * Convert a JSONObject to an Map. The specified {@code schema} must 
	 * contain attribute names that match the JSON key names in the JSONObject. 
	 * The key values of the returned map will match the names of the attributes
	 * contained in the {@code schema}.  
	 * 
	 * @param jbase JSON value that is being converted.
	 * @param schema Schema containing attribute names that will be used as the key values of the map. 
	 * @return Value converted to a {@code Map<String, Object>}.
	 * @throws Exception If there was a problem converting the JSONObject.
	 */
	public static Map<String, Object> jsonToAtributeMap(JSONObject jbase, StreamSchema schema) throws Exception {
		Map<String, Object> attrmap = new HashMap<String, Object>();
		for(Attribute attr : schema) {
			String nameToSearch = attr.getName();
			if ((prefixToIgnore != null) && (attr.getName().startsWith(prefixToIgnore))) {
				nameToSearch = attr.getName().substring(prefixToIgnore.length());
			}
			try {
				if(l.isLoggable(TraceLevel.DEBUG)) {
					l.log(TraceLevel.DEBUG, "Checking for: " + nameToSearch); //$NON-NLS-1$
				}
				Object childobj = jbase.get(nameToSearch);
				if(childobj==null) {
					if(l.isLoggable(TraceLevel.DEBUG)) {
						l.log(TraceLevel.DEBUG, "Not Found: " + nameToSearch); //$NON-NLS-1$
					}
					continue;
				}
				Object obj = jsonToAttribute(attr.getName(), attr.getType(), childobj, null);
				if(obj!=null)
					attrmap.put(attr.getName(), obj);
			} catch(Exception e) {
				l.log(TraceLevel.ERROR, "Error converting object: " + attr.getName(), e); //$NON-NLS-1$
				throw e;
			}
		}
		return attrmap;
	}
	
}
