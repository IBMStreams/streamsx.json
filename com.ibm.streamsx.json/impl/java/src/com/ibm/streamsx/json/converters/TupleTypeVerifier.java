package com.ibm.streamsx.json.converters;

import java.util.List;
import java.util.logging.Logger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;
import com.ibm.streams.operator.logging.LogLevel;
import com.ibm.streamsx.json.converters.Messages;

/**
 * Verifies that the type of an attribute in a given schema matches a list
 * of acceptable types.
 */
public class TupleTypeVerifier {
	
	static private Logger l = Logger.getLogger(TupleTypeVerifier.class.getCanonicalName());

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
			l.log(LogLevel.ERROR, Messages.getString("ATTRIBUTE_MUST_BE_SPECIFIED"), new Object[]{attributeName});	//$NON-NLS-1$
			throw new Exception("CDIST0954E Attribute " + attributeName +" must be specified."); //$NON-NLS-1$ //$NON-NLS-2$
		}
		Type t = attr.getType();
		MetaType rtype = attr.getType().getMetaType();
		if(types != null && types.size() > 0) {
			for(MetaType mt : types ) {
				if(mt == rtype) {
					return t;
				}
			}
			l.log(LogLevel.ERROR, Messages.getString("ATTRIBUTE_MUST_BE_OF_TYPES"), new Object[]{attributeName, types.toString()});	//$NON-NLS-1$
			throw new Exception("CDIST0955E Attribute " + attributeName + " must be one of the following types: " + types.toString()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		return t;
	}
	
}
