---
title: "Toolkit Introduction"
permalink: /docs/user/introduction/
excerpt: "Toolkit introduction"
last_modified_at: 2018-01-09T11:00:00-00:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}


## Introducing the JSON Toolkit

With the increasing popularity of cloud-based solutions, the JSON format is becoming ever more ingrained in our applications. Take for example IBM Bluemix. Many of the services in Bluemix have REST APIs that use JSON as the common exchange format when sending or receiving data.
With that being said, it is no surprise that Streams v4.2 now packages the com.ibm.streamsx.json toolkit. This toolkit provides a simple and straight-forward way to convert from SPL-to-JSON and JSON-to-SPL. The toolkit contains only 2 operators, appropriately named: TupleToJSON and JSONToTuple. There are also native functions available to allow for converting tuples to JSON when using Custom operators.
The remainder of this article will provide an overview of the operators and functions.

### TupleToJSON Operator

This operator converts incoming tuples to a JSON string. The schema of the incoming tuple is used to create the JSON string. The name of each attribute in the schema becomes the **name** portion of a name/value in the JSON string, while the value of each attribute is assigned to the **value** portion. Here is an example what the conversion between an SPL tuple and a JSON string might look like:

*SPL Tuple*

```
tuple<rstring firstname, rstring lastname, int32 age>

firstname=John
lastname=Smith
age=25
```

*JSON string*

```
{
  "fistname" : "John",
  "lastname" : "Smith",
  "age" : 25
}
```

#### SPL-to-JSON Mappings

The following is a mapping of SPL attribute types to JSON types.


SPL Type |	JSON Type
-------- | -------- 
boolean	| true, false
int8, int16, int32, int64, float32, float64, decimal32, decimal64, decimal128	| number
uint8, uint16, uint32, uint64	| number (as the unsigned value)
rstring, ustring, rstring[n]	| string
timestamp	| number (in seconds)
enum	| string
tuple	| object
map<K,V>, map<K,V>[n]	| object
list, list[n], set, set[n]	| array
blob, xml, complex32, complex64	|not supported

(Table referenced from [com.ibm.streams.operator.encoding.JSONEncoding](https://www.ibm.com/support/knowledgecenter/SSCRJU_4.3.0/com.ibm.streams.spl-java-operators.doc/api/com/ibm/streams/operator/encoding/JSONEncoding.html) Javadoc)

### JSONToTuple Operator

This operator converts a JSON string to an SPL tuple. The schema of the SPL tuple is expected to match the JSON schema. Only attributes that are present in both the tuple schema and the JSON input will be converted. Here is an example of what the conversion between a JSON string and an SPL tuple might look like:

*JSON string*

```
{
  "name" : "My App",
  "version" : 1.0,
  "tags" : ["blue", "red", "green"]
}
```

*SPL Tuple*

```
tuple<rstring name, float32 version, list<rstring> tags>

name=My App
version=1.0
tags=[blue, red, green]
```

#### JSON-to-SPL Mappings

The mapping from JSON type to SPL attribute type is the same as listed in the table above, with one exception: map<K,V> and map<K,V>[n] attribute types are NOT supported in the output tuple schema and will be ignored.

#### Type Helper

Packaged in the scripts/ directory of the toolkit is a script called `createTypes.sh`. This is a helper utility to assist in creating SPL tuples from JSON strings. The script can be executed as follows:

```
$STREAMS_INSTALL$/toolkits/com.ibm.streamsx.json/scripts/createTypes.sh <json_file>
```

<json_file> is a file that can contain one or more JSON strings to be converted. Each JSON string must appear on a single line in the file. The output of this utility will print SPL types that closely match the JSON structure. Here is an example of running this script against a file containing a single JSON string:

**myfile.json:**
```
{"src" : "helloworld.jpg", "width" : 500, "height" : 500, "alignment" : "center"}
```

Running the command:
```
$ ./createTypes.sh myfile.json
  type MainType = rstring src, int64 width, rstring alignment, int64 height;
```

### C++ Native functions

The toolkit also contains functions that allow you to convert SPL types to JSON from within Custom operators. Some of the  available functions include:

Function |	Description
-------- | -------- 
public rstring mapToJSON(map<S, T> m)	| Convert a map to JSON object encoded as a serialized JSON string
public rstring toJSON(S key, T value)	 |Convert a value to JSON object with a single key encoded as a serialized JSON string
public rstring tupleToJSON(T t)	 | Convert a tuple to JSON object encoded as a serialized JSON String
public T extractFromJSON(rstring jsonString, T value)	 | Convert a JSON string to a given tuple.

More information on these functions can be found on the (Toolkit documentation)[https://ibmstreams.github.io/streamsx.json/com.ibm.streamsx.json/doc/spldoc/html/index.html].




