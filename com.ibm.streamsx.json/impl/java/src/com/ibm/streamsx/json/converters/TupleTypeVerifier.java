package com.ibm.streamsx.json.converters;

import java.util.List;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;

/**
 * Verifies that the type of an attribute in a given schema matches a list
 * of acceptable types.
 */
public class TupleTypeVerifier {

	/**
	 * Verifies that the type of the attribute in the specified schema matches one of the
	 * types in the {@code list}. 
	 * 
	 * @param schema The schema that contains the specified {@code attributeName}.
	 * @param attributeName The name of the attribute to verify.
	 * @param types The list of acceptable types.
	 * @return The type of the attribute if it matches one of the types in the {@code list}.
	 * @throws Exception Thrown if the attribute type is not in the {@code list}.
	 */
	public static Type verifyAttributeType(StreamSchema schema, String attributeName, List<MetaType> types) 
			throws Exception {
		Attribute attr = schema.getAttribute(attributeName);
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
	
}
