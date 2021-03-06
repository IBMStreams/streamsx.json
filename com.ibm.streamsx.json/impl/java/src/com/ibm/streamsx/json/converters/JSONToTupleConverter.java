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
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streams.operator.logging.TraceLevel;
import com.ibm.streams.operator.meta.CollectionType;
import com.ibm.streams.operator.meta.OptionalType;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;
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
			/* as elements with 'null' values in JSON input are not existing 
			 * in the resulting parsed JSON object, which is source for all objects passed to this function,
			 * we never reach here with jsonObj of 'null' value.
			 * We handle the valid value of an optional element is for the 
			 * base type. Assignment of base types to optional SPL type is supported implicit. */
			MetaType attributeMetaType = type.getMetaType();
			Boolean isOptional = false;
			if (attributeMetaType == MetaType.OPTIONAL ) {
				attributeMetaType = ((OptionalType)type).getValueType().getMetaType();
				isOptional = true;
			}
			switch(attributeMetaType) {
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
				/*
				 * In case of optional LIST type we have first to get the ValueType of the optional before
				 * getting its List BaseType.
				 */
				if (!isOptional) {
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
				else {
					Type optionalBaseType = ((OptionalType)type).getValueType();
					if (!((CollectionType)optionalBaseType).getElementType().getMetaType().isCollectionType() &&
							(parentType == null || !parentType.getMetaType().isCollectionType())) {
						return arrayToSPLArray(name, (JSONArray) jsonObj, optionalBaseType);
					}
					else {
						List<Object> lst = new ArrayList<Object>();
						arrayToCollection(name, lst, (JSONArray) jsonObj, optionalBaseType);
						return lst;
					}
				}
			}
			case SET:
			case BSET:
			{
				Set<Object> lst = new HashSet<Object>();
				/*
				 * In case of optional LIST type we have first to get the ValueType of the optional before
				 * getting its List BaseType.
				 */
				if (!isOptional) {
					arrayToCollection(name, lst, (JSONArray) jsonObj, type);
				}
				else {
					Type optionalBaseType = ((OptionalType)type).getValueType();
					arrayToCollection(name, lst, (JSONArray) jsonObj, optionalBaseType);
				}
				return lst;
			}

			case TUPLE:
				if (!isOptional) {
					return jsonToTuple((JSONObject)jsonObj, ((TupleType)type).getTupleSchema());
				}
				else {
					Type optionalBaseType = ((OptionalType)type).getValueType();
					return jsonToTuple((JSONObject)jsonObj, ((TupleType)optionalBaseType).getTupleSchema());
				}

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
			case XML:
			case ENUM:
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

		/* at this point we get anytime a collection type (no optionalType) 
		 * but the element may be optional 
		 * so element type has to be handled regarding optionalType */
		Type collectionElementType = ctype.getElementType();
		MetaType collectionElementMetaType = collectionElementType.getMetaType();
		Boolean isOptional = false;
		if (collectionElementMetaType  == MetaType.OPTIONAL ) {
			collectionElementMetaType = ((OptionalType)collectionElementType).getValueType().getMetaType();
			isOptional = true;
		}

		switch(collectionElementMetaType) {
		case INT8:
		case UINT8: 
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) 
					lst.add(obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
			}
			/* It's not quite obvious, but is is as it is:
			 * 
			 * SPL list<int16> attribute
			 * expects assignment of java array type int[]
			 * 
			 * SPL list<uint16> attribute
			 * expects assignment of java collection type arrayList<Object>
			 * were Object is expected to be java.lang.Byte object
			 * which is returned from jsonToAttribute()
			 * 
			 * When having List of OptionalType we need anyway to return
			 * a ArrayList<Object>
			 * */
			if (collectionElementMetaType == MetaType.INT8 && !isOptional){
				byte[] arr= new byte[lst.size()];
				for(Object val : lst)
					arr[cnt++] = (Byte)val;
				return arr;
				}
			else {
				return lst;
			}
		} 
		case INT16:
		case UINT16:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) 
					lst.add(obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
				
			}
			/* It's not quite obvious, but is is as it is:
			 * 
			 * SPL list<int16> attribute
			 * expects assignment of java array type int[]
			 * 
			 * SPL list<uint16> attribute
			 * expects assignment of java collection type arrayList<Object>
			 * were Object is expected to be java.lang.Short object
			 * which is returned from jsonToAttribute()
			 * 
			 * When having List of OptionalType we need anyway to return
			 * a ArrayList<Object>
			 * */
			if (collectionElementMetaType == MetaType.INT16 && !isOptional){
				short[] arr= new short[lst.size()];
				for(Object val : lst)
					arr[cnt++] = (Short)val;
				return arr;
				}
			else {
				return lst;
			}
		} 
		case INT32:
		case UINT32:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) 
					lst.add(obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
			}
			/* It's not quite obvious, but is is as it is:
			 * 
			 * SPL list<int32> attribute
			 * expects assignment of java array int[]
			 * 
			 * SPL list<uint32> attribute
			 * expects assignment of java collection type arrayList<Object>
			 * were Object is expected to be java.lang.Integer object
			 * which is returned from jsonToAttribute()
			 * 
			 * When having List of OptionalType we need anyway to return
			 * a ArrayList<Object>
			 * */
			if (collectionElementMetaType == MetaType.INT32 && !isOptional){
				int[] arr= new int[lst.size()];
				for(Object val : lst)
					arr[cnt++] = (Integer)val;
				return arr;
				}
			else {
				return lst;
			}
		} 

		case INT64:
		case UINT64:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) 
					lst.add(obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);

			}
			/* It's not quite obvious, but is is as it is:
			 * 
			 * SPL list<int16> attribute
			 * expects assignment of java array type int[]
			 * 
			 * SPL list<uint16> attribute
			 * expects assignment of java collection type arrayList<Object>
			 * were Object is expected to be java.lang.Byte object
			 * which is returned from jsonToAttribute()
			 * 
			 * When having List of OptionalType we need anyway to return
			 * a ArrayList<Object>
			 * */
			if (collectionElementMetaType == MetaType.INT64 && !isOptional){
				long[] arr= new long[lst.size()];
				for(Object val : lst)
					arr[cnt++] = (Long)val;
				return arr;
				}
			else {
				return lst;
			}
		} 

		case BOOLEAN:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) 
					lst.add(obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
			}

			if (!isOptional){
				boolean[] arr= new boolean[lst.size()];
				for(Object val : lst)
					arr[cnt++] = (Boolean)val;
				return arr;
				}
			else {
				return lst;
			}

		} 

		case FLOAT32:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) 
					lst.add(obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
			}

			/*
			 * When having List of OptionalType we need anyway to return
			 * a ArrayList<Object>
			 */
			if (!isOptional){
				float[] arr= new float[lst.size()];
				for(Object val : lst)
					arr[cnt++] = (Float)val;
				return arr;
				}
			else {
				return lst;
			}
		} 

		case FLOAT64:
		{
			List<Object> lst = new ArrayList<Object>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj!=null) 
					lst.add(obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
			}

			/*
			 * When having List of OptionalType we need anyway to return
			 * a ArrayList<Object>
			 */
			if (!isOptional){
				double[] arr= new double[lst.size()];
				for(Object val : lst)
					arr[cnt++] = (Double)val;
				return arr;
			} else {
				return lst;
			}
		} 

		case USTRING:
		{
			List<String> lst =  new ArrayList<String>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null) 
					lst.add((String)obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
			}
			/*
			 * When having List of OptionalType we need anyway to return
			 * a ArrayList<Object>
			 */
			if (!isOptional){
				return lst.toArray(new String[lst.size()]);
			} else {
				return lst;
			}
		} 

		case BSTRING:
		case RSTRING:
		{
			List<RString> lst = new ArrayList<RString>();
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);
				if(obj != null)  
					lst.add((RString)obj);
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
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
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
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
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
			}
			return lst;
		}
		case SET:
		case BSET:
		{
			Set<Object> lst = new HashSet<Object>(); 
			for(Object jsonObj : jarr) {
				Object obj =jsonToAttribute(cname, ctype.getElementType(), jsonObj, ptype);

				/*
				 * For Set it doesn't make sense to add null values as they are unordered.
				 * There is no information deriveable from a NULL in a set, in contrast 
				 * to a List which is an ordered collection where even the position of a
				 * NULL can give an information about just the element expected on this position
				 * in the List. 
				 * So we don't add a null value to the Set on logical reason. 
				 * But even the Java HashSet wouldn't support adding NULL.
				 * 
				 * So a JSON array mapped to a SPL Set attribute will loose any NULL values
				 * and the Set may be empty if the was only NULL in the JSON array.  
				 * */
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
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
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
				else 
					/* as JSON array has to be seen as ordered elements
					 * existing null values in array have to be kept and not
					 * silently suppressed as without optionalType  
					 */
					if (isOptional)
						lst.add(null);
			}
			return lst;
		}


		//TODO -- not yet supported types
		case BLOB:
		case MAP:
		case BMAP:
		case COMPLEX32:
		case COMPLEX64:
		case ENUM:
		case XML:
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
