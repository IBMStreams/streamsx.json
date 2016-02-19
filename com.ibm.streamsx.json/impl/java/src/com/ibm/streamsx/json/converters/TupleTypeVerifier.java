package com.ibm.streamsx.json.converters;

import java.util.List;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.Type.MetaType;

public class TupleTypeVerifier {

	public static Type verifyAttributeType(OperatorContext op, StreamSchema ss, String attributeName, List<MetaType> types) 
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
	
}
