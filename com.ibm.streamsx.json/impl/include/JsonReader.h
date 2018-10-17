/*
 * JsonReader.h
 *
 *  Created on: Nov 23, 2015
 *      Author: leonid.gorelik
 */

#ifndef JSON_READER_H_
#define JSON_READER_H_

#define STREAMS_BOOST_LEXICAL_CAST_ASSUME_C_LOCALE

#include "rapidjson/error/en.h"
#include "rapidjson/document.h"
#include "rapidjson/pointer.h"
#include "rapidjson/reader.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include <stack>
#include <streams_boost/lexical_cast.hpp>
#include <streams_boost/mpl/or.hpp>
#include <streams_boost/thread/tss.hpp>
#include <streams_boost/type_traits.hpp>
#include <streams_boost/utility/enable_if.hpp>

#include <SPL/Runtime/Type/Tuple.h>



namespace com { namespace ibm { namespace streamsx { namespace json {

	typedef enum{ NO, LIST, MAP } InCollection;

	struct TupleState {

		TupleState(SPL::Tuple & _tuple) : tuple(_tuple), attrIter(_tuple.getEndIterator()), inCollection(NO), objectCount(0) {}

		SPL::Tuple & tuple;
		SPL::TupleIterator attrIter;
		InCollection inCollection;
		int objectCount;
		SPL::set<SPL::rstring> foundKeys;
	};

	struct EventHandler : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, EventHandler> {

		EventHandler(SPL::Tuple & _tuple) {
			objectStack.push(TupleState(_tuple));
		}

		bool Key(const char* jsonKey, rapidjson::SizeType length, bool copy) {
			SPLAPPTRC(L_DEBUG, "extracted key: " << jsonKey, "EXTRACT_FROM_JSON");

			TupleState & state = objectStack.top();
			SPL::TupleIterator const& endIter = state.tuple.getEndIterator();

			if(state.inCollection == MAP) {
				lastKey = jsonKey;
			}
			else {
				if(state.foundKeys.getSize() >= state.tuple.getNumberOfAttributes()) {

					if(objectStack.size() > 1) {
						objectStack.pop();
						objectStack.top().objectCount++;
					}
					else {
						return false;
					}
				}

				TupleState & newState = objectStack.top();
				newState.attrIter = newState.tuple.findAttribute(jsonKey);

				if(newState.attrIter == endIter)
					SPLAPPTRC(L_DEBUG, "not matched, dropped key: " << jsonKey, "EXTRACT_FROM_JSON");
				else if(!newState.foundKeys.insert(jsonKey).second) {
					SPLAPPTRC(L_DEBUG, "duplicate, dropped key: " << jsonKey, "EXTRACT_FROM_JSON");
					newState.attrIter = endIter;
				}
			}

			return true;
		}

		bool Null() { return true; }

		bool Bool(bool b) {

			TupleState & state = objectStack.top();

			if(state.attrIter == state.tuple.getEndIterator()) {
				SPLAPPTRC(L_DEBUG, "not matched, dropped value: " << std::boolalpha << b, "EXTRACT_FROM_JSON");
			}
			else {
				SPLAPPTRC(L_DEBUG, "extracted value: " << std::boolalpha << b, "EXTRACT_FROM_JSON");

				SPL::ValueHandle valueHandle = (*state.attrIter).getValue();

				if(state.inCollection == NO && valueHandle.getMetaType() == SPL::Meta::Type::BOOLEAN)
					static_cast<SPL::boolean&>(valueHandle) = b;
				else if(state.inCollection != NO && valueType == SPL::Meta::Type::BOOLEAN)
					InsertValue(valueHandle, SPL::ConstValueHandle(SPL::boolean(b)));
				else
					SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
			}
			return true;
		}

		template <typename T>
		bool Num(T num) {
			TupleState & state = objectStack.top();

			if(state.attrIter == state.tuple.getEndIterator()) {
				SPLAPPTRC(L_DEBUG, "not matched, dropped value: " << num, "EXTRACT_FROM_JSON");
			}
			else {
				SPLAPPTRC(L_DEBUG, "extracted value: " << num, "EXTRACT_FROM_JSON");

				SPL::ValueHandle valueHandle = (*state.attrIter).getValue();

				if(state.inCollection == NO) {
					switch(valueHandle.getMetaType()) {
						case SPL::Meta::Type::INT8 : { static_cast<SPL::int8&>(valueHandle) = num; break; }
						case SPL::Meta::Type::INT16 : { static_cast<SPL::int16&>(valueHandle) = num; break; }
						case SPL::Meta::Type::INT32 : { static_cast<SPL::int32&>(valueHandle) = num; break; }
						case SPL::Meta::Type::INT64 : { static_cast<SPL::int64&>(valueHandle) = num; break; }
						case SPL::Meta::Type::UINT8 : { static_cast<SPL::uint8&>(valueHandle) = num; break; }
						case SPL::Meta::Type::UINT16 : { static_cast<SPL::uint16&>(valueHandle) = num; break; }
						case SPL::Meta::Type::UINT32 : { static_cast<SPL::uint32&>(valueHandle) = num; break; }
						case SPL::Meta::Type::UINT64 : { static_cast<SPL::uint64&>(valueHandle) = num; break; }
						case SPL::Meta::Type::FLOAT32 : { static_cast<SPL::float32&>(valueHandle) = num; break; }
						case SPL::Meta::Type::FLOAT64 : { static_cast<SPL::float64&>(valueHandle) = num; break; }
						default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
				}
				else {
					switch(valueType) {
						case SPL::Meta::Type::INT8 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::int8>(num))); break; }
						case SPL::Meta::Type::INT16 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::int16>(num))); break; }
						case SPL::Meta::Type::INT32 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::int32>(num))); break; }
						case SPL::Meta::Type::INT64 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::int64>(num))); break; }
						case SPL::Meta::Type::UINT8 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::uint8>(num))); break; }
						case SPL::Meta::Type::UINT16 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::uint16>(num))); break; }
						case SPL::Meta::Type::UINT32 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::uint32>(num))); break; }
						case SPL::Meta::Type::UINT64 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::uint64>(num))); break; }
						case SPL::Meta::Type::FLOAT32 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::float32>(num))); break; }
						case SPL::Meta::Type::FLOAT64 : { InsertValue(valueHandle, SPL::ConstValueHandle(static_cast<SPL::float64>(num))); break; }
						default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
				}
			}
			return true;
		}

		bool Int(int32_t i) { return Num(i); }
		bool Uint(uint32_t u) { return Num(u); }
		bool Int64(int64_t ii) { return Num(ii); }
		bool Uint64(uint64_t uu) { return Num(uu); }
		bool Double(double d) { return Num(d); }

		bool String(const char* s, rapidjson::SizeType length, bool copy) {
			TupleState & state = objectStack.top();

			if(state.attrIter == state.tuple.getEndIterator()) {
				SPLAPPTRC(L_DEBUG, "not matched, dropped value: " << s, "EXTRACT_FROM_JSON");
			}
			else {
				SPLAPPTRC(L_DEBUG, "extracted value: " << s, "EXTRACT_FROM_JSON");

				SPL::ValueHandle valueHandle = (*state.attrIter).getValue();

				if(state.inCollection == NO) {
					switch(valueHandle.getMetaType()) {
						case SPL::Meta::Type::BSTRING : { static_cast<SPL::BString&>(valueHandle) = SPL::rstring(s, length); break; }
						case SPL::Meta::Type::RSTRING : { static_cast<SPL::rstring&>(valueHandle) = s; break; }
						case SPL::Meta::Type::USTRING : { static_cast<SPL::ustring&>(valueHandle) = s; break; }
						default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
				}
				else {
					switch(valueType) {
						case SPL::Meta::Type::BSTRING : { InsertValue(valueHandle, SPL::ConstValueHandle(SPL::bstring<1024>(s, length))); break; }
						case SPL::Meta::Type::RSTRING : { InsertValue(valueHandle, SPL::ConstValueHandle(SPL::rstring(s, length))); break; }
						case SPL::Meta::Type::USTRING : { InsertValue(valueHandle, SPL::ConstValueHandle(SPL::ustring(s, length))); break; }
						default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
				}
			}

			return true;
		}

		bool StartObject() {
			SPLAPPTRC(L_DEBUG, "object started", "EXTRACT_FROM_JSON");

			TupleState & state = objectStack.top();
			SPL::TupleIterator const& endIter = state.tuple.getEndIterator();

			if(state.attrIter == endIter) {
				state.objectCount++;
			}
			else {
				SPL::ValueHandle valueHandle = (*state.attrIter).getValue();

				if(state.inCollection == NO) {
					switch(valueHandle.getMetaType()) {
						case SPL::Meta::Type::MAP : {
							SPLAPPTRC(L_DEBUG, "matched to map", "EXTRACT_FROM_JSON");
							state.inCollection = MAP;

							switch (static_cast<SPL::Map&>(valueHandle).getKeyMetaType()) {
								case SPL::Meta::Type::RSTRING :;
								case SPL::Meta::Type::USTRING : {
									valueType = static_cast<SPL::Map&>(valueHandle).getValueMetaType();
									break;
								}
								default : {
									SPLAPPTRC(L_DEBUG, "key type not matched", "EXTRACT_FROM_JSON");
									state.attrIter = endIter;
								}
							}

							break;
						}
						case SPL::Meta::Type::BMAP : {
							SPLAPPTRC(L_DEBUG, "matched to bounded map", "EXTRACT_FROM_JSON");
							state.inCollection = MAP;

							switch (static_cast<SPL::BMap&>(valueHandle).getKeyMetaType()) {
								case SPL::Meta::Type::RSTRING :;
								case SPL::Meta::Type::USTRING : {
									valueType = static_cast<SPL::BMap&>(valueHandle).getValueMetaType();
									break;
								}
								default : {
									SPLAPPTRC(L_DEBUG, "key type not matched", "EXTRACT_FROM_JSON");
									state.attrIter = endIter;
								}
							}

							break;
						}
						case SPL::Meta::Type::TUPLE : {
							SPLAPPTRC(L_DEBUG, "matched to tuple", "EXTRACT_FROM_JSON");

							SPL::Tuple & tuple = valueHandle;
							objectStack.push(TupleState(tuple));

							break;
						}
						default : {
							SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
							state.attrIter = endIter;
						}
					}
				}
				else {
					if(valueType != SPL::Meta::Type::TUPLE) {
						state.objectCount++;
					}
					else {
						switch(valueHandle.getMetaType()) {
							case SPL::Meta::Type::LIST : {
								SPL::List & listAttr = valueHandle;
								SPL::ValueHandle valueElemHandle = listAttr.createElement();
								listAttr.pushBack(valueElemHandle);
								valueElemHandle.deleteValue();

								SPL::Tuple & tuple = listAttr.getElement(listAttr.getSize()-1);
								objectStack.push(TupleState(tuple));

								break;
							}
							case SPL::Meta::Type::MAP : {
								SPL::Map & mapAttr = valueHandle;
								SPL::ValueHandle valueElemHandle = mapAttr.createValue();

								SPL::ConstValueHandle key(lastKey);
								if(mapAttr.getKeyMetaType() == SPL::Meta::Type::USTRING)
									key = SPL::ustring(lastKey.data(), lastKey.length());

								mapAttr.insertElement(key, valueElemHandle);
								valueElemHandle.deleteValue();

								SPL::Tuple & tuple = (*(mapAttr.findElement(key))).second;
								objectStack.push(TupleState(tuple));

								break;
							}
							default : {
								SPLAPPTRC(L_DEBUG, "Set and bounded collection types with tuple value not supported", "EXTRACT_FROM_JSON");
								state.attrIter = endIter;
							}
						}
					}
				}
			}

			return true;
		}

		bool EndObject(rapidjson::SizeType memberCount) {
			SPLAPPTRC(L_DEBUG, "object ended", "EXTRACT_FROM_JSON");

			TupleState & state = objectStack.top();

			if(state.inCollection == MAP) {
				state.inCollection = NO;
			}
			else {
				if(state.objectCount > 0)
					state.objectCount--;
				else
					objectStack.pop();
			}

			return true;
		}

		bool StartArray() {
			SPLAPPTRC(L_DEBUG, "array started", "EXTRACT_FROM_JSON");

			TupleState & state = objectStack.top();
			SPL::TupleIterator const& endIter = state.tuple.getEndIterator();

			if(state.attrIter != endIter) {
				SPL::ValueHandle valueHandle = (*state.attrIter).getValue();

				switch (valueHandle.getMetaType()) {
					case SPL::Meta::Type::LIST : {
						SPLAPPTRC(L_DEBUG, "matched to list", "EXTRACT_FROM_JSON");

						valueType = static_cast<SPL::List&>(valueHandle).getElementMetaType();
						state.inCollection = LIST;
						break;
					}
					case SPL::Meta::Type::BLIST : {
						SPLAPPTRC(L_DEBUG, "matched to bounded list", "EXTRACT_FROM_JSON");

						valueType = static_cast<SPL::BList&>(valueHandle).getElementMetaType();
						state.inCollection = LIST;
						break;
					}
					case SPL::Meta::Type::SET : {
						SPLAPPTRC(L_DEBUG, "matched to set", "EXTRACT_FROM_JSON");

						valueType = static_cast<SPL::Set&>(valueHandle).getElementMetaType();
						state.inCollection = LIST;
						break;
					}
					case SPL::Meta::Type::BSET : {
						SPLAPPTRC(L_DEBUG, "matched to bounded set", "EXTRACT_FROM_JSON");

						valueType = static_cast<SPL::BSet&>(valueHandle).getElementMetaType();
						state.inCollection = LIST;
						break;
					}
					default : {
						SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
						state.attrIter = endIter;
					}
				}
			}

			return true;
		}

		bool EndArray(rapidjson::SizeType elementCount) {
			SPLAPPTRC(L_DEBUG, "array ended", "EXTRACT_FROM_JSON");

			objectStack.top().inCollection = NO;
			return true;
		}

		inline void InsertValue(SPL::ValueHandle & valueHandle, SPL::ConstValueHandle const& valueElemHandle) {

			switch (valueHandle.getMetaType()) {
				case SPL::Meta::Type::LIST : {
					static_cast<SPL::List&>(valueHandle).pushBack(valueElemHandle);
					break;
				}
				case SPL::Meta::Type::BLIST : {
					static_cast<SPL::BList&>(valueHandle).pushBack(valueElemHandle);
					break;
				}
				case SPL::Meta::Type::SET : {
					static_cast<SPL::Set&>(valueHandle).insertElement(valueElemHandle);
					break;
				}
				case SPL::Meta::Type::BSET : {
					static_cast<SPL::BSet&>(valueHandle).insertElement(valueElemHandle);
					break;
				}
				case SPL::Meta::Type::MAP : {
					SPL::Map & mapAttr = valueHandle;
					if(mapAttr.getKeyMetaType() == SPL::Meta::Type::RSTRING)
						mapAttr.insertElement(SPL::ConstValueHandle(lastKey), valueElemHandle);
					else
						mapAttr.insertElement(SPL::ConstValueHandle(SPL::ustring(lastKey.data(), lastKey.length())), valueElemHandle);

					break;
				}
				case SPL::Meta::Type::BMAP : {
					SPL::BMap & mapAttr = valueHandle;
					if(mapAttr.getKeyMetaType() == SPL::Meta::Type::RSTRING)
						mapAttr.insertElement(SPL::ConstValueHandle(lastKey), valueElemHandle);
					else
						mapAttr.insertElement(SPL::ConstValueHandle(SPL::ustring(lastKey.data(), lastKey.length())), valueElemHandle);

					break;
				}
				default:;
			}
		}

	private:
		SPL::rstring lastKey;
		SPL::Meta::Type valueType;
		std::stack<TupleState> objectStack;
	};

	inline SPL::Tuple& extractFromJSON(SPL::rstring const& jsonString, SPL::Tuple & tuple) {

	    EventHandler handler(tuple);
	    rapidjson::Reader reader;
	    rapidjson::StringStream jsonStringStream(jsonString.c_str());
	    reader.Parse(jsonStringStream, handler);

		return tuple;
	}


	template<typename T>
	inline T parseNumber(rapidjson::Value * value) {
		rapidjson::StringBuffer str;
		rapidjson::Writer<rapidjson::StringBuffer> writer(str);
		value->Accept(writer);
		return SPL::spl_cast<T,SPL::rstring>::cast( SPL::rstring(str.GetString()));
	}

	template<typename Status>
	inline SPL::rstring getParseError(Status const& status) {
		return GetParseError_En((rapidjson::ParseErrorCode)status.getIndex());
	}

	template<typename Status, typename Index>
	inline SPL::boolean getJSONValue(rapidjson::Value * value, SPL::boolean defaultVal, Status & status, Index const& jsonIndex) {

		if(!value)					status = 4;
		else if(value->IsNull())	status = 3;
		else {
			try {
				if(value->IsBool())		{ status = 0; return static_cast<SPL::boolean>(value->GetBool()); }
				if(value->IsString())	{ status = 1; return streams_boost::lexical_cast<SPL::boolean>(value->GetString()); }
			}
			catch(streams_boost::bad_lexical_cast const&) {}

			status = 2;
		}

		return defaultVal;
	}

	template<typename T, typename Status, typename Index>
	inline T getJSONValue(rapidjson::Value * value, T defaultVal, Status & status, Index const& jsonIndex,
					   typename streams_boost::enable_if< typename streams_boost::mpl::or_<
					   	   streams_boost::mpl::bool_< streams_boost::is_arithmetic<T>::value>,
						   streams_boost::mpl::bool_< streams_boost::is_same<SPL::decimal32, T>::value>,
						   streams_boost::mpl::bool_< streams_boost::is_same<SPL::decimal64, T>::value>,
						   streams_boost::mpl::bool_< streams_boost::is_same<SPL::decimal128, T>::value>
					   >::type, void*>::type t = NULL) {

		if(!value)
			status = 4;
		else if(value->IsNull())
			status = 3;
		else if(value->IsNumber())	{
			status = 0;

			if( streams_boost::is_same<SPL::int8, T>::value)		return static_cast<T>(value->GetInt());
			if( streams_boost::is_same<SPL::int16, T>::value)		return static_cast<T>(value->GetInt());
			if( streams_boost::is_same<SPL::int32, T>::value)		return static_cast<T>(value->GetInt());
			if( streams_boost::is_same<SPL::int64, T>::value)		return static_cast<T>(value->GetInt64());
			if( streams_boost::is_same<SPL::uint8, T>::value)		return static_cast<T>(value->GetUint());
			if( streams_boost::is_same<SPL::uint16, T>::value)		return static_cast<T>(value->GetUint());
			if( streams_boost::is_same<SPL::uint32, T>::value)		return static_cast<T>(value->GetUint());
			if( streams_boost::is_same<SPL::uint64, T>::value)		return static_cast<T>(value->GetUint64());
			if( streams_boost::is_same<SPL::float32, T>::value)		return static_cast<T>(value->GetFloat());
			if( streams_boost::is_same<SPL::float64, T>::value)		return static_cast<T>(value->GetDouble());
			if( streams_boost::is_same<SPL::decimal32, T>::value)	return parseNumber<T>(value);
			if( streams_boost::is_same<SPL::decimal64, T>::value)	return parseNumber<T>(value);
			if( streams_boost::is_same<SPL::decimal128, T>::value)	return parseNumber<T>(value);
		}
		else if(value->IsString())	{
			status = 1;

			try {
				return streams_boost::lexical_cast<T>(value->GetString());
			}
			catch(streams_boost::bad_lexical_cast const&) {
				status = 2;
			}
		}
		else
			status = 2;

		return defaultVal;
	}

	template<typename T, typename Status, typename Index>
	inline T getJSONValue(rapidjson::Value * value, T const& defaultVal, Status & status, Index const& jsonIndex,
					   typename streams_boost::enable_if< typename streams_boost::mpl::or_<
					   	   streams_boost::mpl::bool_< streams_boost::is_base_of<SPL::RString, T>::value>,
						   streams_boost::mpl::bool_< streams_boost::is_same<SPL::ustring, T>::value>
					   >::type, void*>::type t = NULL) {

		status = 0;

		if(!value)					status = 4;
		else if(value->IsNull())	status = 3;
		else {
			try {
				switch (value->GetType()) {
					case rapidjson::kStringType: {
						status = 0;
						return value->GetString();
					}
					case rapidjson::kFalseType: {
						status = 1;
						return "false";
					}
					case rapidjson::kTrueType: {
						status = 1;
						return "true";
					}
					case rapidjson::kNumberType: {
						status = 1;
						return parseNumber<T>(value);
					}
					default:;
				}
			}
			catch(streams_boost::bad_lexical_cast const&) {}

			status = 2;
		}

		return defaultVal;
	}

	template<typename T, typename Status, typename Index>
	inline SPL::list<T> getJSONValue(rapidjson::Value * value, SPL::list<T> const& defaultVal, Status & status, Index const& jsonIndex) {

		if(!value)					status = 4;
		else if(value->IsNull())	status = 3;
		else if(!value->IsArray())	status = 2;
		else						status = 0;

		if(status == 0) {
			rapidjson::Value::Array arr = value->GetArray();
			SPL::list<T> result;
			result.reserve(arr.Size());
			Status valueStatus = 0;

			for (rapidjson::Value::Array::ValueIterator it = arr.Begin(); it != arr.End(); ++it) {
				T val = getJSONValue(it, T(), valueStatus, jsonIndex);

				if(valueStatus == 0)
					result.push_back(val);
				else if(valueStatus > status)
					status = valueStatus;
			}

			return result;
		}

		return defaultVal;
	}
}}}}

#endif

namespace com { namespace ibm { namespace streamsx { namespace json {

	namespace { // this anonymous namespace will be defined for each operator separately

		template<typename Index>
		inline rapidjson::Document& getDocument() {
			static streams_boost::thread_specific_ptr<rapidjson::Document> jsonPtr_;

			rapidjson::Document * jsonPtr = jsonPtr_.get();
			if(!jsonPtr) {
				jsonPtr_.reset(new rapidjson::Document());
				jsonPtr = jsonPtr_.get();
			}

			return *jsonPtr;
		}

		template<typename Status, typename Index>
		inline bool parseJSON(SPL::rstring const& jsonString, Status & status, uint32_t & offset, const Index & jsonIndex) {
			rapidjson::Document & json = getDocument<Index>();
			rapidjson::Document(rapidjson::kObjectType).Swap(json);

			if(json.Parse<rapidjson::kParseStopWhenDoneFlag>(jsonString.c_str()).HasParseError()) {
				json.SetObject();
				status = json.GetParseError();
				offset = json.GetErrorOffset();

				return false;
			}
			return true;
		}

		template<typename Index>
		inline uint32_t  parseJSON(SPL::rstring const& jsonString, const Index & jsonIndex) {

			rapidjson::ParseErrorCode status = rapidjson::kParseErrorNone;
			uint32_t offset = 0;

			if(!parseJSON(jsonString, status, offset, jsonIndex))
				SPLAPPTRC(L_ERROR, GetParseError_En(status), "PARSE_JSON");

			return (uint32_t)status;
		}

		template<typename T, typename Status, typename Index>
		inline T queryJSON(SPL::rstring const& jsonPath, T const& defaultVal, Status & status, Index const& jsonIndex) {

			rapidjson::Document & json = getDocument<Index>();
			if(json.IsNull())
				THROW(SPL::SPLRuntimeOperator, "Invalid usage of 'queryJSON' function, 'parseJSON' function must be used before.");

			const rapidjson::Pointer & pointer = rapidjson::Pointer(jsonPath.c_str());
			rapidjson::PointerParseErrorCode ec = pointer.GetParseErrorCode();

			if(pointer.IsValid()) {
				rapidjson::Value * value = pointer.Get(json);
				return getJSONValue(value, defaultVal, status, jsonIndex);
			}
			else {
				status = ec + 4; // Pointer error codes in SPL enum should be shifted by 4
				return defaultVal;
			}
		}

		template<typename T, typename Index>
		inline T queryJSON(SPL::rstring const& jsonPath, T const& defaultVal, Index const& jsonIndex) {

			 int status = 0;
			 return queryJSON(jsonPath, defaultVal, status, jsonIndex);
		}
	}
}}}}

/* JSON_READER_H_ */

