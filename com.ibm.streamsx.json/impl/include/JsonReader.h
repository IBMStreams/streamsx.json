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

using namespace rapidjson;
using namespace streams_boost;
using namespace SPL;

namespace com { namespace ibm { namespace streamsx { namespace json {

	namespace {
		struct OperatorInstance {};
	}

	typedef enum{ NO, LIST, MAP } InCollection;

	struct TupleState {

		TupleState(Tuple & _tuple) : tuple(_tuple), attrIter(_tuple.getEndIterator()), inCollection(NO), objectCount(0) {}

		Tuple & tuple;
		TupleIterator attrIter;
		InCollection inCollection;
		int objectCount;
		set<rstring> foundKeys;
	};

	struct EventHandler : public BaseReaderHandler<UTF8<>, EventHandler> {

		EventHandler(Tuple & _tuple) {
			objectStack.push(TupleState(_tuple));
		}

		bool Key(const char* jsonKey, SizeType length, bool copy) {
			SPLAPPTRC(L_DEBUG, "extracted key: " << jsonKey, "EXTRACT_FROM_JSON");

			TupleState & state = objectStack.top();
			TupleIterator const& endIter = state.tuple.getEndIterator();

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

				ValueHandle valueHandle = (*state.attrIter).getValue();

				if(state.inCollection == NO && valueHandle.getMetaType() == Meta::Type::BOOLEAN)
					static_cast<boolean&>(valueHandle) = b;
				else if(state.inCollection != NO && valueType == Meta::Type::BOOLEAN)
					InsertValue(valueHandle, ConstValueHandle(boolean(b)));
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

				ValueHandle valueHandle = (*state.attrIter).getValue();

				if(state.inCollection == NO) {
					switch(valueHandle.getMetaType()) {
						case Meta::Type::INT8 : { static_cast<int8&>(valueHandle) = num; break; }
						case Meta::Type::INT16 : { static_cast<int16&>(valueHandle) = num; break; }
						case Meta::Type::INT32 : { static_cast<int32&>(valueHandle) = num; break; }
						case Meta::Type::INT64 : { static_cast<int64&>(valueHandle) = num; break; }
						case Meta::Type::UINT8 : { static_cast<uint8&>(valueHandle) = num; break; }
						case Meta::Type::UINT16 : { static_cast<uint16&>(valueHandle) = num; break; }
						case Meta::Type::UINT32 : { static_cast<uint32&>(valueHandle) = num; break; }
						case Meta::Type::UINT64 : { static_cast<uint64&>(valueHandle) = num; break; }
						case Meta::Type::FLOAT32 : { static_cast<float32&>(valueHandle) = num; break; }
						case Meta::Type::FLOAT64 : { static_cast<float64&>(valueHandle) = num; break; }
						default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
				}
				else {
					switch(valueType) {
						case Meta::Type::INT8 : { InsertValue(valueHandle, ConstValueHandle(static_cast<int8>(num))); break; }
						case Meta::Type::INT16 : { InsertValue(valueHandle, ConstValueHandle(static_cast<int16>(num))); break; }
						case Meta::Type::INT32 : { InsertValue(valueHandle, ConstValueHandle(static_cast<int32>(num))); break; }
						case Meta::Type::INT64 : { InsertValue(valueHandle, ConstValueHandle(static_cast<int64>(num))); break; }
						case Meta::Type::UINT8 : { InsertValue(valueHandle, ConstValueHandle(static_cast<uint8>(num))); break; }
						case Meta::Type::UINT16 : { InsertValue(valueHandle, ConstValueHandle(static_cast<uint16>(num))); break; }
						case Meta::Type::UINT32 : { InsertValue(valueHandle, ConstValueHandle(static_cast<uint32>(num))); break; }
						case Meta::Type::UINT64 : { InsertValue(valueHandle, ConstValueHandle(static_cast<uint64>(num))); break; }
						case Meta::Type::FLOAT32 : { InsertValue(valueHandle, ConstValueHandle(static_cast<float32>(num))); break; }
						case Meta::Type::FLOAT64 : { InsertValue(valueHandle, ConstValueHandle(static_cast<float64>(num))); break; }
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

		bool String(const char* s, SizeType length, bool copy) {
			TupleState & state = objectStack.top();

			if(state.attrIter == state.tuple.getEndIterator()) {
				SPLAPPTRC(L_DEBUG, "not matched, dropped value: " << s, "EXTRACT_FROM_JSON");
			}
			else {
				SPLAPPTRC(L_DEBUG, "extracted value: " << s, "EXTRACT_FROM_JSON");

				ValueHandle valueHandle = (*state.attrIter).getValue();

				if(state.inCollection == NO) {
					switch(valueHandle.getMetaType()) {
						case Meta::Type::BSTRING : { static_cast<BString&>(valueHandle) = rstring(s, length); break; }
						case Meta::Type::RSTRING : { static_cast<rstring&>(valueHandle) = s; break; }
						case Meta::Type::USTRING : { static_cast<ustring&>(valueHandle) = s; break; }
						default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
				}
				else {
					switch(valueType) {
						case Meta::Type::BSTRING : { InsertValue(valueHandle, ConstValueHandle(bstring<1024>(s, length))); break; }
						case Meta::Type::RSTRING : { InsertValue(valueHandle, ConstValueHandle(rstring(s, length))); break; }
						case Meta::Type::USTRING : { InsertValue(valueHandle, ConstValueHandle(ustring(s, length))); break; }
						default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
				}
			}

			return true;
		}

		bool StartObject() {
			SPLAPPTRC(L_DEBUG, "object started", "EXTRACT_FROM_JSON");

			TupleState & state = objectStack.top();
			TupleIterator const& endIter = state.tuple.getEndIterator();

			if(state.attrIter == endIter) {
				state.objectCount++;
			}
			else {
				ValueHandle valueHandle = (*state.attrIter).getValue();

				if(state.inCollection == NO) {
					switch(valueHandle.getMetaType()) {
						case Meta::Type::MAP : {
							SPLAPPTRC(L_DEBUG, "matched to map", "EXTRACT_FROM_JSON");
							state.inCollection = MAP;

							switch (static_cast<Map&>(valueHandle).getKeyMetaType()) {
								case Meta::Type::RSTRING :;
								case Meta::Type::USTRING : {
									valueType = static_cast<Map&>(valueHandle).getValueMetaType();
									break;
								}
								default : {
									SPLAPPTRC(L_DEBUG, "key type not matched", "EXTRACT_FROM_JSON");
									state.attrIter = endIter;
								}
							}

							break;
						}
						case Meta::Type::BMAP : {
							SPLAPPTRC(L_DEBUG, "matched to bounded map", "EXTRACT_FROM_JSON");
							state.inCollection = MAP;

							switch (static_cast<BMap&>(valueHandle).getKeyMetaType()) {
								case Meta::Type::RSTRING :;
								case Meta::Type::USTRING : {
									valueType = static_cast<BMap&>(valueHandle).getValueMetaType();
									break;
								}
								default : {
									SPLAPPTRC(L_DEBUG, "key type not matched", "EXTRACT_FROM_JSON");
									state.attrIter = endIter;
								}
							}

							break;
						}
						case Meta::Type::TUPLE : {
							SPLAPPTRC(L_DEBUG, "matched to tuple", "EXTRACT_FROM_JSON");

							Tuple & tuple = valueHandle;
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
					if(valueType != Meta::Type::TUPLE) {
						state.objectCount++;
					}
					else {
						switch(valueHandle.getMetaType()) {
							case Meta::Type::LIST : {
								List & listAttr = valueHandle;
								ValueHandle valueElemHandle = listAttr.createElement();
								listAttr.pushBack(valueElemHandle);
								valueElemHandle.deleteValue();

								Tuple & tuple = listAttr.getElement(listAttr.getSize()-1);
								objectStack.push(TupleState(tuple));

								break;
							}
							case Meta::Type::MAP : {
								Map & mapAttr = valueHandle;
								ValueHandle valueElemHandle = mapAttr.createValue();

								ConstValueHandle key(lastKey);
								if(mapAttr.getKeyMetaType() == Meta::Type::USTRING)
									key = ustring(lastKey.data(), lastKey.length());

								mapAttr.insertElement(key, valueElemHandle);
								valueElemHandle.deleteValue();

								Tuple & tuple = (*(mapAttr.findElement(key))).second;
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

		bool EndObject(SizeType memberCount) {
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
			TupleIterator const& endIter = state.tuple.getEndIterator();

			if(state.attrIter != endIter) {
				ValueHandle valueHandle = (*state.attrIter).getValue();

				switch (valueHandle.getMetaType()) {
					case Meta::Type::LIST : {
						SPLAPPTRC(L_DEBUG, "matched to list", "EXTRACT_FROM_JSON");

						valueType = static_cast<List&>(valueHandle).getElementMetaType();
						state.inCollection = LIST;
						break;
					}
					case Meta::Type::BLIST : {
						SPLAPPTRC(L_DEBUG, "matched to bounded list", "EXTRACT_FROM_JSON");

						valueType = static_cast<BList&>(valueHandle).getElementMetaType();
						state.inCollection = LIST;
						break;
					}
					case Meta::Type::SET : {
						SPLAPPTRC(L_DEBUG, "matched to set", "EXTRACT_FROM_JSON");

						valueType = static_cast<Set&>(valueHandle).getElementMetaType();
						state.inCollection = LIST;
						break;
					}
					case Meta::Type::BSET : {
						SPLAPPTRC(L_DEBUG, "matched to bounded set", "EXTRACT_FROM_JSON");

						valueType = static_cast<BSet&>(valueHandle).getElementMetaType();
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

		bool EndArray(SizeType elementCount) {
			SPLAPPTRC(L_DEBUG, "array ended", "EXTRACT_FROM_JSON");

			objectStack.top().inCollection = NO;
			return true;
		}

		inline void InsertValue(ValueHandle & valueHandle, ConstValueHandle const& valueElemHandle) {

			switch (valueHandle.getMetaType()) {
				case Meta::Type::LIST : {
					static_cast<List&>(valueHandle).pushBack(valueElemHandle);
					break;
				}
				case Meta::Type::BLIST : {
					static_cast<BList&>(valueHandle).pushBack(valueElemHandle);
					break;
				}
				case Meta::Type::SET : {
					static_cast<Set&>(valueHandle).insertElement(valueElemHandle);
					break;
				}
				case Meta::Type::BSET : {
					static_cast<BSet&>(valueHandle).insertElement(valueElemHandle);
					break;
				}
				case Meta::Type::MAP : {
					Map & mapAttr = valueHandle;
					if(mapAttr.getKeyMetaType() == Meta::Type::RSTRING)
						mapAttr.insertElement(ConstValueHandle(lastKey), valueElemHandle);
					else
						mapAttr.insertElement(ConstValueHandle(ustring(lastKey.data(), lastKey.length())), valueElemHandle);

					break;
				}
				case Meta::Type::BMAP : {
					BMap & mapAttr = valueHandle;
					if(mapAttr.getKeyMetaType() == Meta::Type::RSTRING)
						mapAttr.insertElement(ConstValueHandle(lastKey), valueElemHandle);
					else
						mapAttr.insertElement(ConstValueHandle(ustring(lastKey.data(), lastKey.length())), valueElemHandle);

					break;
				}
				default:;
			}
		}

	private:
		rstring lastKey;
		Meta::Type valueType;
		std::stack<TupleState> objectStack;
	};

	inline Tuple& extractFromJSON(rstring const& jsonString, Tuple & tuple) {

	    EventHandler handler(tuple);
	    Reader reader;
	    StringStream jsonStringStream(jsonString.c_str());
	    reader.Parse(jsonStringStream, handler);

		return tuple;
	}


	template<typename Index, typename OP>
	inline Document& getDocument() {
		static thread_specific_ptr<Document> jsonPtr_;

		Document * jsonPtr = jsonPtr_.get();
		if(!jsonPtr) {
			jsonPtr_.reset(new Document());
			jsonPtr = jsonPtr_.get();
		}

		return *jsonPtr;
	}

	template<typename Status, typename Index>
	inline bool parseJSON(rstring const& jsonString, Status & status, uint32_t & offset, const Index & jsonIndex) {
		Document & json = getDocument<Index, OperatorInstance>();
		Document(kObjectType).Swap(json);

		if(json.Parse<kParseStopWhenDoneFlag>(jsonString.c_str()).HasParseError()) {
			json.SetObject();
			status = json.GetParseError();
			offset = json.GetErrorOffset();

			return false;
		}
		return true;
	}

	template<typename Index>
	inline uint32_t  parseJSON(rstring const& jsonString, const Index & jsonIndex) {

		ParseErrorCode status = kParseErrorNone;
		uint32_t offset = 0;

		if(!parseJSON(jsonString, status, offset, jsonIndex))
			SPLAPPTRC(L_ERROR, GetParseError_En(status), "PARSE_JSON");

		return (uint32_t)status;
	}

	template<typename Status>
	inline rstring getParseError(Status const& status) {
		return GetParseError_En((ParseErrorCode)status.getIndex());
	}

	template<typename Status, typename Index>
	inline boolean getJSONValue(Value * value, boolean defaultVal, Status & status, Index const& jsonIndex) {

		if(!value)					status = 4;
		else if(value->IsNull())	status = 3;
		else {
			try {
				if(value->IsBool())		{ status = 0; return static_cast<boolean>(value->GetBool()); }
				if(value->IsString())	{ status = 1; return lexical_cast<boolean>(value->GetString()); }
			}
			catch(bad_lexical_cast const&) {}

			status = 2;
		}

		return defaultVal;
	}

	template<typename T, typename Status, typename Index>
	inline T getJSONValue(Value * value, T defaultVal, Status & status, Index const& jsonIndex,
					   typename enable_if< typename mpl::or_<
					   	   mpl::bool_< is_arithmetic<T>::value>,
					   	   mpl::bool_< is_same<decimal32, T>::value>,
					   	   mpl::bool_< is_same<decimal64, T>::value>,
						   mpl::bool_< is_same<decimal128, T>::value>
					   >::type, void*>::type t = NULL) {

		if(!value)
			status = 4;
		else if(value->IsNull())
			status = 3;
		else if(value->IsNumber())	{
			status = 0;

			if( is_same<SPL::int8, T>::value)		return static_cast<T>(value->GetInt());
			if( is_same<SPL::int16, T>::value)		return static_cast<T>(value->GetInt());
			if( is_same<SPL::int32, T>::value)		return static_cast<T>(value->GetInt());
			if( is_same<SPL::int64, T>::value)		return static_cast<T>(value->GetInt64());
			if( is_same<SPL::uint8, T>::value)		return static_cast<T>(value->GetUint());
			if( is_same<SPL::uint16, T>::value)		return static_cast<T>(value->GetUint());
			if( is_same<SPL::uint32, T>::value)		return static_cast<T>(value->GetUint());
			if( is_same<SPL::uint64, T>::value)		return static_cast<T>(value->GetUint64());
			if( is_same<SPL::float32, T>::value)	return static_cast<T>(value->GetFloat());
			if( is_same<SPL::float64, T>::value)	return static_cast<T>(value->GetDouble());
			if( is_same<SPL::decimal32, T>::value)	return static_cast<T>(value->GetFloat());
			if( is_same<SPL::decimal64, T>::value)	return static_cast<T>(value->GetDouble());
			if( is_same<SPL::decimal128, T>::value)	return static_cast<T>(value->GetDouble());
		}
		else if(value->IsString())	{
			status = 1;

			try {
				return lexical_cast<T>(value->GetString());
			}
			catch(bad_lexical_cast const&) {}
		}

		status = 2;
		return defaultVal;
	}

	template<typename T, typename Status, typename Index>
	inline T getJSONValue(Value * value, T const& defaultVal, Status & status, Index const& jsonIndex,
					   typename enable_if< typename mpl::or_<
					   	   mpl::bool_< is_base_of<RString, T>::value>,
						   mpl::bool_< is_same<ustring, T>::value>
					   >::type, void*>::type t = NULL) {

		status = 0;

		if(!value)					status = 4;
		else if(value->IsNull())	status = 3;
		else {
			try {
				switch (value->GetType()) {
					case kStringType: {
						status = 0;
						return value->GetString();
					}
					case kFalseType: {
						status = 1;
						return "false";
					}
					case kTrueType: {
						status = 1;
						return "true";
					}
					case kNumberType: {
						status = 1;
						StringBuffer str;
						Writer<StringBuffer> writer(str);
						value->Accept(writer);
						return str.GetString();
					}
					default:;
				}
			}
			catch(bad_lexical_cast const&) {}

			status = 2;
		}

		return defaultVal;
	}

	template<typename T, typename Status, typename Index>
	inline list<T> getJSONValue(Value * value, list<T> const& defaultVal, Status & status, Index const& jsonIndex) {

		if(!value)					status = 4;
		else if(value->IsNull())	status = 3;
		else if(!value->IsArray())	status = 2;
		else						status = 0;

		if(status == 0) {
			Value::Array arr = value->GetArray();
			list<T> result;
			result.reserve(arr.Size());
			Status valueStatus = 0;

			for (Value::Array::ValueIterator it = arr.Begin(); it != arr.End(); ++it) {
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

	template<typename T, typename Status, typename Index>
	inline T queryJSON(rstring const& jsonPath, T const& defaultVal, Status & status, Index const& jsonIndex) {

		Document & json = getDocument<Index, OperatorInstance>();
		if(json.IsNull())
			THROW(SPL::SPLRuntimeOperator, "Invalid usage of 'queryJSON' function, 'parseJSON' function must be used before.");

		const Pointer & pointer = Pointer(jsonPath.c_str());
		PointerParseErrorCode ec = pointer.GetParseErrorCode();

		if(pointer.IsValid()) {
			Value * value = pointer.Get(json);
			return getJSONValue(value, defaultVal, status, jsonIndex);
		}
		else {
			status = ec + 4; // Pointer error codes in SPL enum should be shifted by 4
			return defaultVal;
		}
	}

	template<typename T, typename Index>
	inline T queryJSON(rstring const& jsonPath, T const& defaultVal, Index const& jsonIndex) {

		 int status = 0;
		 return queryJSON(jsonPath, defaultVal, status, jsonIndex);
	}

}}}}

#endif /* JSON_READER_H_ */
