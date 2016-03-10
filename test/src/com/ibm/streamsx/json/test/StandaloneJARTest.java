package com.ibm.streamsx.json.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.json.converters.JSONToTupleConverter;
import com.ibm.streamsx.json.converters.TupleToJSONConverter;

public class StandaloneJARTest {
	
	public static final String jsonStr = "{\n" + 
			"	\"a\": \"xyz\",\n" + 
			"	\"b\": 10,\n" + 
			"	\"c\": 3.3,\n" + 
			"	\"d\": [\"x\", \"y\", \"z\"],\n" + 
			"	\"e\": {\n" + 
			"		\"s\": \"mmm\",\n" + 
			"		\"t\": 10\n" + 
			"	},\n" + 
			"	\"f\": [1, 2, 3, 4, 5]\n" + 
			"}";
	public static final StreamSchema schema = Type.Factory.getStreamSchema("tuple<rstring a, int32 b, float32 c, list<rstring> d, tuple<rstring s, int32 t> e, set<int32> f>");
		
	@Test
	public void toTuple() throws Exception {

		JSONObject jsonObj = JSONObject.parse(jsonStr);
		
		Tuple tuple = JSONToTupleConverter.jsonToTuple(jsonObj, schema);
		Assert.assertNotNull(tuple);
		Assert.assertEquals(tuple.getString("a"), "xyz");
		Assert.assertEquals(tuple.getInt("b"), 10);
		Assert.assertEquals(tuple.getDouble("c"), 3.3, 0.0001);
		
		Object[] dArr = {new RString("x"), new RString("y"), new RString("z")};
		Assert.assertArrayEquals(tuple.getList("d").toArray(), dArr);
		
		Tuple eTuple = tuple.getTuple("e");
		Assert.assertEquals(eTuple.getString("s"), "mmm");
		Assert.assertEquals(eTuple.getInt("t"), 10);
		
		Set<Integer> fSet = new HashSet<>();
		fSet.addAll(Arrays.asList(1, 2, 3, 4, 5));
		Assert.assertEquals(tuple.getSet("f"), fSet);
	}
	
	@Test
	public void toJSON() throws Exception {
		Map<String, Object> map = new HashMap<>();
		
		map.put("a", new RString("xyz"));
		map.put("b", 10);
		map.put("c", (float)3.3);
		map.put("d", Arrays.asList(new RString("x"), new RString("y"), new RString("z")));
		
		Map<String, Object> eMap = new HashMap<>();
		eMap.put("s", new RString("mmm"));
		eMap.put("t", 10);
		Tuple eTuple = Type.Factory.getStreamSchema("tuple<rstring s, int32 t>").getTuple(eMap);
		map.put("e", eTuple);
		
		Set<Integer> fSet = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5));
		map.put("f", fSet);
		Tuple tuple = schema.getTuple(map);
		
		String json = TupleToJSONConverter.convertTuple(tuple);
		
		String rawExpected = jsonStr
				.trim()
				.replace("\n", "")
				.replace("\t", "")
				.replace(" ", "");
		
		Assert.assertEquals(rawExpected, json);
	}
	
}
