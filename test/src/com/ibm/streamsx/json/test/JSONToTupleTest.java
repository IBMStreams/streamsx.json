package com.ibm.streamsx.json.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.flow.declare.InputPortDeclaration;
import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.declare.OperatorGraphFactory;
import com.ibm.streams.flow.declare.OperatorInvocation;
import com.ibm.streams.flow.declare.OutputPortDeclaration;
import com.ibm.streams.flow.handlers.MostRecent;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.json.JSONToTuple;

public class JSONToTupleTest {
	
	/**
	 * Test auto assignment of input attributes to output attributes.
	 */
	@Test
	public void testAutoAssignment() throws Exception {

		OperatorGraph graph = OperatorGraphFactory.newGraph();

		StreamSchema inSchema = Type.Factory.getStreamSchema("tuple<int32 a, int64 b, rstring jsonString>");
		StreamSchema outSchema = Type.Factory.getStreamSchema("tuple<int32 a, int64 b, float64 c, boolean d>");

		// Declare a beacon operator
		OperatorInvocation<JSONToTuple> tuple2Json = graph.addOperator(JSONToTuple.class);

		// Declare an input port connected to to the beacon's output
		InputPortDeclaration input = tuple2Json.addInput(inSchema);
		OutputPortDeclaration output = tuple2Json.addOutput(outSchema);
		
		assertTrue(graph.compileChecks());
		
		// Create the testable version of the graph
		JavaTestableGraph testableGraph = new JavaOperatorTester().executable(graph);
		
		StreamingOutput<OutputTuple> testInput = testableGraph.getInputTester(input);
		
		MostRecent<Tuple> mr = new MostRecent<>();
		testableGraph.registerStreamHandler(output, mr);

		// Get read to test tuples.
		testableGraph.initialize().get().allPortsReady().get();
		
		JSONObject j = new JSONObject();
		j.put("c", 22.5d);
		j.put("d", false);
		
		// Test attributes not in the JSON are set from the input attributes
		testInput.submitAsTuple(3, 81L, new RString(j.serialize()));
		Tuple outTuple = mr.getMostRecentTuple();
		assertEquals(3, outTuple.getInt("a"));
		assertEquals(81L, outTuple.getLong("b"));
		assertEquals(22.5d, outTuple.getDouble("c"), 0.01);
		assertFalse(outTuple.getBoolean("d"));
		
		// Test attributes in the JSON are set from the JSON
		j.put("a", 6216);
		testInput.submitAsTuple(3, 81L, new RString(j.serialize()));
		outTuple = mr.getMostRecentTuple();
		assertEquals(6216, outTuple.getInt("a"));
		assertEquals(81L, outTuple.getLong("b"));
		assertEquals(22.5d, outTuple.getDouble("c"), 0.01);
		assertFalse(outTuple.getBoolean("d"));
		
		// Test attributes in the JSON are set from the JSON
		j.put("a", 436346);
		j.put("b", 325235L);
		j.put("d", true);
		testInput.submitAsTuple(3, 81L, new RString(j.serialize()));
		outTuple = mr.getMostRecentTuple();
		assertEquals(436346, outTuple.getInt("a"));
		assertEquals(325235L, outTuple.getLong("b"));
		assertEquals(22.5d, outTuple.getDouble("c"), 0.01);
		assertTrue(outTuple.getBoolean("d"));
		
		testableGraph.shutdown().get();
		
	}
}
