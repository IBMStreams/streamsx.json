/*
 * Json.h
 *
 *  Created on: Nov 23, 2015
 *      Author: leonid.gorelik
 */

#ifndef JSON_H_
#define JSON_H_

// Define SPL types and functions.
#include "SPL/Runtime/Function/SPLCast.h"
#include "SPL/Runtime/Function/TimeFunctions.h"
#include <SPL/Runtime/Type/Tuple.h>

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

using namespace rapidjson;
using namespace SPL;

namespace json {

	template<typename String>
	char const* convToChars(String const& str) { return str.c_str(); }

	template<>
	char const* convToChars<ustring>(ustring const& str) { return spl_cast<rstring,ustring>::cast(str).c_str(); }

	template<>
	char const* convToChars<ConstValueHandle>(const ConstValueHandle& valueHandle) {
		switch(valueHandle.getMetaType()) {
			case Meta::Type::BSTRING : {
				const BString & str = valueHandle;
				return str.getCString();
			}
			case Meta::Type::USTRING : {
				const ustring & str = valueHandle;
				return spl_cast<rstring,ustring>::cast(str).c_str();
			}
			default: {
				const rstring & str = valueHandle;
				return str.c_str();
			}
		}
	}

	template<typename Container, typename Iterator>
	void writeArray(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle);

	template<typename Container, typename Iterator>
	void writeMap(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle);

	void writeTuple(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle);

	void writePrimitive(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle);


	void writeAny(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle) {

		switch (valueHandle.getMetaType()) {
			case Meta::Type::LIST : {
				writeArray<List,ConstListIterator>(writer, valueHandle);
				break;
			}
			case Meta::Type::BLIST : {
				writeArray<BList,ConstListIterator>(writer, valueHandle);
				break;
			}
			case Meta::Type::SET : {
				writeArray<Set,ConstSetIterator>(writer, valueHandle);
				break;
			}
			case Meta::Type::BSET : {
				writeArray<BSet,ConstSetIterator>(writer, valueHandle);
				break;
			}
			case Meta::Type::MAP :
				writeMap<Map,ConstMapIterator>(writer, valueHandle);
				break;
			case Meta::Type::BMAP : {
				writeMap<BMap,ConstMapIterator>(writer, valueHandle);
				break;
			}
			case Meta::Type::TUPLE : {
				writeTuple(writer, valueHandle);
				break;
			}
			default:
				writePrimitive(writer, valueHandle);
		}
	}

	template<typename Container, typename Iterator>
	void writeArray(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle) {

		writer.StartArray();

		const Container & array = valueHandle;
		for(Iterator arrayIter = array.getBeginIterator(); arrayIter != array.getEndIterator(); arrayIter++) {

			const ConstValueHandle & arrayValueHandle = *arrayIter;
			writeAny(writer, arrayValueHandle);
		}

		writer.EndArray();
	}

	template<typename Container, typename Iterator>
	void writeMap(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle) {

		writer.StartObject();

		const Container & map = valueHandle;
		for(Iterator mapIter = map.getBeginIterator(); mapIter != map.getEndIterator(); mapIter++) {

			const std::pair<ConstValueHandle,ConstValueHandle> & mapHandle = *mapIter;
			const ConstValueHandle & mapValueHandle = mapHandle.second;

			writer.String(convToChars(mapHandle.first));
			writeAny(writer, mapValueHandle);
		}

		writer.EndObject();
	}

	void writeTuple(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle) {

		writer.StartObject();

		const Tuple & tuple = valueHandle;
		for(ConstTupleIterator tupleIter = tuple.getBeginIterator(); tupleIter != tuple.getEndIterator(); tupleIter++) {

			const std::string & attrName = (*tupleIter).getName();
			const ConstValueHandle & attrValueHandle = static_cast<ConstTupleAttribute>(*tupleIter).getValue();

			writer.String(convToChars(attrName));
			writeAny(writer, attrValueHandle);
		}

		writer.EndObject();
	}

	void writePrimitive(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle) {

		switch (valueHandle.getMetaType()) {
			case Meta::Type::BOOLEAN : {
				const boolean & value = valueHandle;
				writer.Bool(value);
				break;
			}
			case Meta::Type::ENUM : {
				const Enum & value = valueHandle;
				writer.String(convToChars(value.getValue()));
				break;
			}
			case Meta::Type::INT8 : {
				const int8 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case Meta::Type::INT16 : {
				const int16 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case Meta::Type::INT32 : {
				const int32 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case Meta::Type::INT64 : {
				const int64 & value = valueHandle;
				writer.Int64(value);
				break;
			}
			case Meta::Type::UINT8 : {
				const uint8 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case Meta::Type::UINT16 : {
				const uint16 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case Meta::Type::UINT32 : {
				const uint32 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case Meta::Type::UINT64 : {
				const uint64 & value = valueHandle;
				writer.Uint64(value);
				break;
			}
			case Meta::Type::FLOAT32 : {
				const float32 & value = valueHandle;
				writer.Double(value);
				break;
			}
			case Meta::Type::FLOAT64 : {
				const float64 & value = valueHandle;
				writer.Double(value);
				break;
			}
			case Meta::Type::DECIMAL32 : {
				const float64 & value = spl_cast<float64,decimal32>::cast(valueHandle);
				writer.Double(value);
				break;
			}
			case Meta::Type::DECIMAL64 : {
				const float64 & value = spl_cast<float64,decimal64>::cast(valueHandle);
				writer.Double(value);
				break;
			}
			case Meta::Type::DECIMAL128 : {
				const float64 & value = spl_cast<float64,decimal128>::cast(valueHandle);
				writer.Double(value);
				break;
			}
			case Meta::Type::COMPLEX32 : {
				writer.Null();
				break;
			}
			case Meta::Type::COMPLEX64 : {
				writer.Null();
				break;
			}
			case Meta::Type::TIMESTAMP : {
				const timestamp & value = valueHandle;
				writer.String(convToChars(Functions::Time::ctime(value)));
				break;
			}
			case Meta::Type::BSTRING :
			case Meta::Type::RSTRING :
			case Meta::Type::USTRING : {
				writer.String(convToChars(valueHandle));
				break;
			}
			case Meta::Type::BLOB : {
				writer.Null();
				break;
			}
			case Meta::Type::XML : {
				writer.Null();
				break;
			}
			default:
				writer.Null();
		}
	}


	inline SPL::rstring tupleToJSON(Tuple const& tuple) {

	    StringBuffer s;
	    Writer<StringBuffer> writer(s);

		writeAny(writer, ConstValueHandle(tuple));

		return s.GetString();
	}

	template<class MAP>
	inline SPL::rstring mapToJSON(MAP const& map) {

	    StringBuffer s;
	    Writer<StringBuffer> writer(s);

		writeAny(writer, ConstValueHandle(map));

		return s.GetString();
	}

	template<class String, class SPLAny>
	inline SPL::rstring toJSON(String const& key, SPLAny const& splAny) {


	    StringBuffer s;
	    Writer<StringBuffer> writer(s);

		writer.StartObject();

		writer.String(convToChars(key));

		writeAny(writer, ConstValueHandle(splAny));

		writer.EndObject();

		return s.GetString();
	}

}

#endif /* JSON_H_ */
