/*
 * JsonWriter.h
 *
 *  Created on: Nov 23, 2015
 *      Author: leonid.gorelik
 */

#ifndef JSON_WRITER_H_
#define JSON_WRITER_H_

// Define SPL types and functions.
#include "SPL/Runtime/Function/SPLCast.h"
#include "SPL/Runtime/Function/TimeFunctions.h"
#include <SPL/Runtime/Type/Tuple.h>

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <streams_boost/algorithm/string.hpp>

using namespace rapidjson;
using namespace SPL;

namespace com { namespace ibm { namespace streamsx { namespace json {

	template<typename String>
	inline char const* convToChars(String const& str) { return str.c_str(); }

	template<>
	inline char const* convToChars<ustring>(ustring const& str) { return spl_cast<rstring,ustring>::cast(str).c_str(); }

	template<>
	inline char const* convToChars<ConstValueHandle>(const ConstValueHandle& valueHandle) {
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
	inline void writeArray(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore);

	template<typename Container, typename Iterator>
	inline void writeMap(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore);

	inline void writeTuple(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore);

	inline void writePrimitive(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle);


	inline void writeAny(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore) {

		switch (valueHandle.getMetaType()) {
			case Meta::Type::LIST : {
				writeArray<List,ConstListIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case Meta::Type::BLIST : {
				writeArray<BList,ConstListIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case Meta::Type::SET : {
				writeArray<Set,ConstSetIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case Meta::Type::BSET : {
				writeArray<BSet,ConstSetIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case Meta::Type::MAP :
				writeMap<Map,ConstMapIterator>(writer, valueHandle, prefixToIgnore);
				break;
			case Meta::Type::BMAP : {
				writeMap<BMap,ConstMapIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case Meta::Type::TUPLE : {
				writeTuple(writer, valueHandle, prefixToIgnore);
				break;
			}
			case Meta::Type::OPTIONAL : {
				/*
				 * optional meta type needs to be checked first if a value is present
				 * if it is present the value meta type has to be detected and the value itself
				 * will be handled as it's non-optional
				 * - primitive types will be handled by writePrimitive
				 * - composite types by their appropriate write... function
				 * if it is not present a null value should be written
				 */
				if (((const SPL::Optional&)valueHandle).isPresent())
				{
					switch (((const SPL::Optional&)valueHandle).getValueMetaType()) {
						case Meta::Type::BOOLEAN :
						case Meta::Type::ENUM :
						case Meta::Type::INT8 :
						case Meta::Type::INT16 :
						case Meta::Type::INT32 :
						case Meta::Type::INT64 :
						case Meta::Type::UINT8 :
						case Meta::Type::UINT16 :
						case Meta::Type::UINT32 :
						case Meta::Type::UINT64 :
						case Meta::Type::FLOAT32 :
						case Meta::Type::FLOAT64 :
						case Meta::Type::DECIMAL32 :
						case Meta::Type::DECIMAL64 :
						case Meta::Type::DECIMAL128 :
						case Meta::Type::COMPLEX32 :
						case Meta::Type::COMPLEX64 :
						case Meta::Type::TIMESTAMP :
						case Meta::Type::BSTRING :
						case Meta::Type::RSTRING :
						case Meta::Type::USTRING :
						case Meta::Type::BLOB :
						case Meta::Type::XML : {
							writePrimitive(writer, ((const SPL::Optional&)valueHandle).getValue());
							break;
						}
						case Meta::Type::LIST : {
							writeArray<List,ConstListIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case Meta::Type::BLIST : {
							writeArray<BList,ConstListIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case Meta::Type::SET : {
							writeArray<Set,ConstSetIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case Meta::Type::BSET : {
							writeArray<BSet,ConstSetIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case Meta::Type::MAP :
							writeMap<Map,ConstMapIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						case Meta::Type::BMAP : {
							writeMap<BMap,ConstMapIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case Meta::Type::TUPLE : {
							writeTuple(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						default:
							writer.Null();
					}
				}
				else {
					// write null if optional is not present
					writer.Null();
				}
				break;
			} //end case optional
			default:
				writePrimitive(writer, valueHandle);
		}
	}

	template<typename Container, typename Iterator>
	inline void writeArray(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore) {

		writer.StartArray();

		const Container & array = valueHandle;
		for(Iterator arrayIter = array.getBeginIterator(); arrayIter != array.getEndIterator(); arrayIter++) {

			const ConstValueHandle & arrayValueHandle = *arrayIter;
			writeAny(writer, arrayValueHandle, prefixToIgnore);
		}

		writer.EndArray();
	}

	template<typename Container, typename Iterator>
	inline void writeMap(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore) {

		writer.StartObject();

		const Container & map = valueHandle;
		for(Iterator mapIter = map.getBeginIterator(); mapIter != map.getEndIterator(); mapIter++) {

			const std::pair<ConstValueHandle,ConstValueHandle> & mapHandle = *mapIter;
			const ConstValueHandle & mapValueHandle = mapHandle.second;

			writer.String(convToChars(mapHandle.first));
			writeAny(writer, mapValueHandle, prefixToIgnore);
		}

		writer.EndObject();
	}

	inline void writeTuple(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore) {
		using namespace streams_boost::algorithm;

		writer.StartObject();

		const Tuple & tuple = valueHandle;
		for(ConstTupleIterator tupleIter = tuple.getBeginIterator(); tupleIter != tuple.getEndIterator(); tupleIter++) {

			const std::string & attrName = (*tupleIter).getName();
			const ConstValueHandle & attrValueHandle = static_cast<ConstTupleAttribute>(*tupleIter).getValue();

			if(!prefixToIgnore.empty() && starts_with(attrName, prefixToIgnore)) {
				writer.String(convToChars(replace_first_copy(attrName, prefixToIgnore, "")));
			}
			else {
				writer.String(convToChars(attrName));
			}

			writeAny(writer, attrValueHandle, prefixToIgnore);
		}

		writer.EndObject();
	}

	inline void writePrimitive(Writer<StringBuffer> & writer, ConstValueHandle const & valueHandle) {

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


	inline SPL::rstring tupleToJSON(Tuple const& tuple, SPL::rstring prefixToIgnore = "") {

	    StringBuffer s;
	    Writer<StringBuffer> writer(s);

		writeAny(writer, ConstValueHandle(tuple), prefixToIgnore);

		return s.GetString();
	}

	template<class MAP>
	inline SPL::rstring mapToJSON(MAP const& map, SPL::rstring prefixToIgnore = "") {

	    StringBuffer s;
	    Writer<StringBuffer> writer(s);

		writeAny(writer, ConstValueHandle(map), prefixToIgnore);

		return s.GetString();
	}

	/*
	 * overwrite the mapToJSON for optional map types
	 *
	 * works exactly as mapToJSON except the case where the
	 * the parameter value is null. In this case an empty object
	 * represented by {} will be generated, to get a valid JSON
	 * string. It is the same as for an empty map. SO one can
	 * never reinterpret this one to a map type with NULL value.
	 */
	template<class MAP>
	inline SPL::rstring mapToJSON(SPL::optional<MAP> const& map, SPL::rstring prefixToIgnore = "") {

	    StringBuffer s;
	    Writer<StringBuffer> writer(s);

		if (((const SPL::Optional&)ConstValueHandle(map)).isPresent()) {
			writeAny(writer, ConstValueHandle(map), prefixToIgnore);
		}
		else {
			writer.StartObject();
			writer.EndObject();
		}

		return s.GetString();
	}


	template<class String, class SPLAny>
	inline SPL::rstring toJSON(String const& key, SPLAny const& splAny, SPL::rstring prefixToIgnore = "") {


	    StringBuffer s;
	    Writer<StringBuffer> writer(s);

		writer.StartObject();

		writer.String(convToChars(key));

		writeAny(writer, ConstValueHandle(splAny), prefixToIgnore);

		writer.EndObject();

		return s.GetString();
	}

}}}}

#endif /* JSON_WRITER_H_ */
