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



//using namespace SPL;

namespace com { namespace ibm { namespace streamsx { namespace json {

	template<typename String>
	inline char const* convToChars(String const& str) { return str.c_str(); }

	template<>
	inline char const* convToChars<SPL::ustring>(SPL::ustring const& str) { return SPL::spl_cast<SPL::rstring,SPL::ustring>::cast(str).c_str(); }

	template<>
	inline char const* convToChars<SPL::ConstValueHandle>(const SPL::ConstValueHandle& valueHandle) {
		switch(valueHandle.getMetaType()) {
			case SPL::Meta::Type::BSTRING : {
				const SPL::BString & str = valueHandle;
				return str.getCString();
			}
			case SPL::Meta::Type::USTRING : {
				const SPL::ustring & str = valueHandle;
				return SPL::spl_cast<SPL::rstring,SPL::ustring>::cast(str).c_str();
			}
			default: {
				const SPL::rstring & str = valueHandle;
				return str.c_str();
			}
		}
	}


	template<typename Container, typename Iterator>
	inline void writeArray(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore);

	template<typename Container, typename Iterator>
	inline void writeMap(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore);

	inline void writeTuple(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore);

	inline void writePrimitive(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle);


	inline void writeAny(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore) {

		switch (valueHandle.getMetaType()) {
			case SPL::Meta::Type::LIST : {
				writeArray<SPL::List,SPL::ConstListIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case SPL::Meta::Type::BLIST : {
				writeArray<SPL::BList,SPL::ConstListIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case SPL::Meta::Type::SET : {
				writeArray<SPL::Set,SPL::ConstSetIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case SPL::Meta::Type::BSET : {
				writeArray<SPL::BSet,SPL::ConstSetIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case SPL::Meta::Type::MAP :
				writeMap<SPL::Map,SPL::ConstMapIterator>(writer, valueHandle, prefixToIgnore);
				break;
			case SPL::Meta::Type::BMAP : {
				writeMap<SPL::BMap,SPL::ConstMapIterator>(writer, valueHandle, prefixToIgnore);
				break;
			}
			case SPL::Meta::Type::TUPLE : {
				writeTuple(writer, valueHandle, prefixToIgnore);
				break;
			}
			case SPL::Meta::Type::OPTIONAL : {
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
						case SPL::Meta::Type::BOOLEAN :
						case SPL::Meta::Type::ENUM :
						case SPL::Meta::Type::INT8 :
						case SPL::Meta::Type::INT16 :
						case SPL::Meta::Type::INT32 :
						case SPL::Meta::Type::INT64 :
						case SPL::Meta::Type::UINT8 :
						case SPL::Meta::Type::UINT16 :
						case SPL::Meta::Type::UINT32 :
						case SPL::Meta::Type::UINT64 :
						case SPL::Meta::Type::FLOAT32 :
						case SPL::Meta::Type::FLOAT64 :
						case SPL::Meta::Type::DECIMAL32 :
						case SPL::Meta::Type::DECIMAL64 :
						case SPL::Meta::Type::DECIMAL128 :
						case SPL::Meta::Type::COMPLEX32 :
						case SPL::Meta::Type::COMPLEX64 :
						case SPL::Meta::Type::TIMESTAMP :
						case SPL::Meta::Type::BSTRING :
						case SPL::Meta::Type::RSTRING :
						case SPL::Meta::Type::USTRING :
						case SPL::Meta::Type::BLOB :
						case SPL::Meta::Type::XML : {
							writePrimitive(writer, ((const SPL::Optional&)valueHandle).getValue());
							break;
						}
						case SPL::Meta::Type::LIST : {
							writeArray<SPL::List,SPL::ConstListIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case SPL::Meta::Type::BLIST : {
							writeArray<SPL::BList,SPL::ConstListIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case SPL::Meta::Type::SET : {
							writeArray<SPL::Set,SPL::ConstSetIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case SPL::Meta::Type::BSET : {
							writeArray<SPL::BSet,SPL::ConstSetIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case SPL::Meta::Type::MAP :
							writeMap<SPL::Map,SPL::ConstMapIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						case SPL::Meta::Type::BMAP : {
							writeMap<SPL::BMap,SPL::ConstMapIterator>(writer, ((const SPL::Optional&)valueHandle).getValue(), prefixToIgnore);
							break;
						}
						case SPL::Meta::Type::TUPLE : {
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
	inline void writeArray(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore) {

		writer.StartArray();

		const Container & array = valueHandle;
		for(Iterator arrayIter = array.getBeginIterator(); arrayIter != array.getEndIterator(); arrayIter++) {

			const SPL::ConstValueHandle & arrayValueHandle = *arrayIter;
			writeAny(writer, arrayValueHandle, prefixToIgnore);
		}

		writer.EndArray();
	}

	template<typename Container, typename Iterator>
	inline void writeMap(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore) {

		writer.StartObject();

		const Container & map = valueHandle;
		for(Iterator mapIter = map.getBeginIterator(); mapIter != map.getEndIterator(); mapIter++) {

			const std::pair<SPL::ConstValueHandle,SPL::ConstValueHandle> & mapHandle = *mapIter;
			const SPL::ConstValueHandle & mapValueHandle = mapHandle.second;

			writer.String(convToChars(mapHandle.first));
			writeAny(writer, mapValueHandle, prefixToIgnore);
		}

		writer.EndObject();
	}

	inline void writeTuple(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle, SPL::rstring const& prefixToIgnore) {
		using namespace streams_boost::algorithm;

		writer.StartObject();

		const SPL::Tuple & tuple = valueHandle;
		for(SPL::ConstTupleIterator tupleIter = tuple.getBeginIterator(); tupleIter != tuple.getEndIterator(); tupleIter++) {

			const std::string & attrName = (*tupleIter).getName();
			const SPL::ConstValueHandle & attrValueHandle = static_cast<SPL::ConstTupleAttribute>(*tupleIter).getValue();

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

	inline void writePrimitive(rapidjson::Writer<rapidjson::StringBuffer> & writer, SPL::ConstValueHandle const & valueHandle) {

		switch (valueHandle.getMetaType()) {
			case SPL::Meta::Type::BOOLEAN : {
				const SPL::boolean & value = valueHandle;
				writer.Bool(value);
				break;
			}
			case SPL::Meta::Type::ENUM : {
				const SPL::Enum & value = valueHandle;
				writer.String(convToChars(value.getValue()));
				break;
			}
			case SPL::Meta::Type::INT8 : {
				const SPL::int8 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case SPL::Meta::Type::INT16 : {
				const SPL::int16 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case SPL::Meta::Type::INT32 : {
				const SPL::int32 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case SPL::Meta::Type::INT64 : {
				const SPL::int64 & value = valueHandle;
				writer.Int64(value);
				break;
			}
			case SPL::Meta::Type::UINT8 : {
				const SPL::uint8 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case SPL::Meta::Type::UINT16 : {
				const SPL::uint16 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case SPL::Meta::Type::UINT32 : {
				const SPL::uint32 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case SPL::Meta::Type::UINT64 : {
				const SPL::uint64 & value = valueHandle;
				writer.Uint64(value);
				break;
			}
			case SPL::Meta::Type::FLOAT32 : {
				const SPL::float32 & value = valueHandle;
				writer.Double(value);
				break;
			}
			case SPL::Meta::Type::FLOAT64 : {
				const SPL::float64 & value = valueHandle;
				writer.Double(value);
				break;
			}
			case SPL::Meta::Type::DECIMAL32 : {
				const SPL::float64 & value = SPL::spl_cast<SPL::float64,SPL::decimal32>::cast(valueHandle);
				writer.Double(value);
				break;
			}
			case SPL::Meta::Type::DECIMAL64 : {
				const SPL::float64 & value = SPL::spl_cast<SPL::float64,SPL::decimal64>::cast(valueHandle);
				writer.Double(value);
				break;
			}
			case SPL::Meta::Type::DECIMAL128 : {
				const SPL::float64 & value = SPL::spl_cast<SPL::float64,SPL::decimal128>::cast(valueHandle);
				writer.Double(value);
				break;
			}
			case SPL::Meta::Type::COMPLEX32 : {
				writer.Null();
				break;
			}
			case SPL::Meta::Type::COMPLEX64 : {
				writer.Null();
				break;
			}
			case SPL::Meta::Type::TIMESTAMP : {
				const SPL::timestamp & value = valueHandle;
				writer.String(convToChars(SPL::Functions::Time::ctime(value)));
				break;
			}
			case SPL::Meta::Type::BSTRING :
			case SPL::Meta::Type::RSTRING :
			case SPL::Meta::Type::USTRING : {
				writer.String(convToChars(valueHandle));
				break;
			}
			case SPL::Meta::Type::BLOB : {
				writer.Null();
				break;
			}
			case SPL::Meta::Type::XML : {
				writer.Null();
				break;
			}
			default:
				writer.Null();
		}
	}


	inline SPL::rstring tupleToJSON(SPL::Tuple const& tuple, SPL::rstring prefixToIgnore = "") {

	    rapidjson::StringBuffer s;
	    rapidjson::Writer<rapidjson::StringBuffer> writer(s);

		writeAny(writer, SPL::ConstValueHandle(tuple), prefixToIgnore);

		return s.GetString();
	}

	template<class MAP>
	inline SPL::rstring mapToJSON(MAP const& map, SPL::rstring prefixToIgnore = "") {

	    rapidjson::StringBuffer s;
	    rapidjson::Writer<rapidjson::StringBuffer> writer(s);

		writeAny(writer, SPL::ConstValueHandle(map), prefixToIgnore);

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

	    rapidjson::StringBuffer s;
	    rapidjson::Writer<rapidjson::StringBuffer> writer(s);

		if (((const SPL::Optional&)SPL::ConstValueHandle(map)).isPresent()) {
			writeAny(writer, SPL::ConstValueHandle(map), prefixToIgnore);
		}
		else {
			writer.StartObject();
			writer.EndObject();
		}

		return s.GetString();
	}


	template<class String, class SPLAny>
	inline SPL::rstring toJSON(String const& key, SPLAny const& splAny, SPL::rstring prefixToIgnore = "") {


	    rapidjson::StringBuffer s;
	    rapidjson::Writer<rapidjson::StringBuffer> writer(s);

		writer.StartObject();

		writer.String(convToChars(key));

		writeAny(writer, SPL::ConstValueHandle(splAny), prefixToIgnore);

		writer.EndObject();

		return s.GetString();
	}

}}}}

#endif /* JSON_WRITER_H_ */
