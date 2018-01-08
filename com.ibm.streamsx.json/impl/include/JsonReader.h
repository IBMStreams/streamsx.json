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


	/* Structure holding the state of the actual open tuple
	 * tuple		reference to the SPl tuple object
	 * attrIter		Iterator to access the attributes of the tuple
	 * inCollection	indicates that the attribute of attrIter is a SPL collection (MAP, LIST)
	 * 				or not
	 * objectCount	counter of JSON objects being opened (synch to objects closed)
	 * foundKeys	set holding attribute names of the tuple already read
	 */
	struct TupleState {

		TupleState(Tuple & _tuple) : tuple(_tuple), attrIter(_tuple.getEndIterator()), inCollection(NO), objectCount(0) {}

		Tuple & tuple;
		TupleIterator attrIter;
		InCollection inCollection;
		int objectCount;
		set<rstring> foundKeys;
	};


	/* EventHandler as expected by RapidJSON lib SAX parser
	 *
	 * SAX events handled
	 *
	 * 	key()
	 * 	null()
	 * 	bool()
	 *	Int()
	 *	Uint()
	 *	Int64()
	 *	Uint64()
	 *	Double()
	 *	String()
	 * 	StartObject()
	 * 	EndObject()
	 * 	StartArray()
	 * 	EndArray()
	 *
	 * 	The eventhandler holds a status stack of TupleState allowing nested object handling.
	 * 	An JSON object is mapped to a SPL tuple type or can also be mapped to a SPL map type.
	 * 	The SPL map types key is either rstring or ustring. All values of this JSON
	 * 	object are expected to be of same type.
	 * 	An JSON array is mapped to a SPL set or SPL list. Primitive as  well as tuple types
	 * 	are supported as collection elements.
	 * 	A key event tries to find the attribute of same name as the key. If found the AttrIter
	 * 	is used in following events for value assignment.
	 * 	Null events are ignored if the SPL attribute type is not optional. If it is optional
	 * 	the null value is set to the attribute or in collection of optional elements the element
	 * 	is set to null.
	 * 	Int(), Uint(), Int65(), Uint64(), Double() events are mapped to the attributes SPL
	 * 	numeric type if it matches.
	 * 	String() event is mapped to the attributes SPL string type if it matches
	 * 	(rstring,ustring,rstring<n>).
	 *
	 * 	SPL decimal types are not supported.
	 * 	SPL Set of tuple is not supported.
	 * 	SPL timestamps are not supported.
	 */
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

		/*
		 * A JSON null value is ignored by non-optional attributes, as there is no SPL equivalent.
		 * Optional attributes will be set to not present (null).
		 * List and map collection elements have to be created and added with set to not-present.
		 * Set elements will not be added as null gives no information in a set.
		 * */
		bool Null() {
			TupleState & state = objectStack.top();

			if(state.attrIter == state.tuple.getEndIterator()) {
				SPLAPPTRC(L_DEBUG, "not matched, dropped value: null", "EXTRACT_FROM_JSON");
			}
			else {
				SPLAPPTRC(L_DEBUG, "extracted value: null" , "EXTRACT_FROM_JSON");

				ValueHandle valueHandle = (*state.attrIter).getValue();

				if (state.inCollection == NO) {
					if (valueHandle.getMetaType() == Meta::Type::OPTIONAL)	{
						/* the null received in this stage is the direct value of the
						 * open attribute, regardless if being a collection attribute
						 * or primitive attribute
						 * set it just to not-present
						 */
						((Optional &)valueHandle).clear();
					}
				}
				else {
					/* the JSON null received at this stage means a collection element is
					 * null
					 * only List, Blist, Map, BMap are supported
					 * if the collection element valueType is optional
					 *   create an element of the collection (initially optionals are NULL)
					 * for non-optional collection element types the null is ignored
					 */
					ValueHandle collectionHandle = valueHandle;
					if (valueHandle.getMetaType() == Meta::Type::OPTIONAL)
						collectionHandle = ((Optional&)valueHandle).getValue();

					switch (collectionHandle.getMetaType()) {
						case Meta::Type::MAP: {
							if (((Map &)collectionHandle).getValueMetaType() == Meta::Type::OPTIONAL) {
								ValueHandle collectionElementValue = ((Map &)collectionHandle).createValue();
								InsertValue(valueHandle, collectionElementValue);
								collectionElementValue.deleteValue();
								}
							break;}
						case Meta::Type::BMAP: {
							if (((BMap &)collectionHandle).getValueMetaType() == Meta::Type::OPTIONAL) {
								ValueHandle collectionElementValue = ((BMap &)collectionHandle).createValue();
								InsertValue(valueHandle, collectionElementValue);
								collectionElementValue.deleteValue();
								}
							break;}
						case Meta::Type::LIST: {
							if (((List &)collectionHandle).getElementMetaType() == Meta::Type::OPTIONAL) {
								ValueHandle collectionElementValue = ((List &)collectionHandle).createElement();
								InsertValue(valueHandle, collectionElementValue);
								collectionElementValue.deleteValue();
								}
							break;}
						case Meta::Type::BLIST: {
							if (((BList &)collectionHandle).getElementMetaType() == Meta::Type::OPTIONAL) {
								ValueHandle collectionElementValue = ((BList &)collectionHandle).createElement();
								InsertValue(valueHandle, collectionElementValue);
								collectionElementValue.deleteValue();
								}
							break; }
						default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
				}
			}
			return true;
		}

		bool Bool(bool b) {

			TupleState & state = objectStack.top();

			if(state.attrIter == state.tuple.getEndIterator()) {
				SPLAPPTRC(L_DEBUG, "not matched, dropped value: " << std::boolalpha << b, "EXTRACT_FROM_JSON");
			}
			else {
				SPLAPPTRC(L_DEBUG, "extracted value: " << std::boolalpha << b, "EXTRACT_FROM_JSON");

				ValueHandle valueHandle = (*state.attrIter).getValue();


				if(state.inCollection == NO ) {
					if (valueHandle.getMetaType() == Meta::Type::OPTIONAL) {
						Optional & refOptional = valueHandle;
						if (refOptional.getValueMetaType() == Meta::Type::BOOLEAN)
							static_cast<optional<boolean> &>(refOptional) = boolean(b);
						else
							SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
					else if (valueHandle.getMetaType() == Meta::Type::BOOLEAN )
						static_cast<boolean&>(valueHandle) = b;
					else
						SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
				}
				else {
					if (valueHandle.getMetaType() == Meta::Type::OPTIONAL){
						Optional&  refOptional = (Optional&) valueHandle;
						if (refOptional.isPresent()) {
							valueHandle = refOptional.getValue();
							switch(valueType) {
								case Meta::Type::BOOLEAN : {InsertValue(valueHandle, ConstValueHandle(boolean(b)));break;}
								default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
							}

						}
						else
							SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
					}
					else {
						switch(valueType) {
							case Meta::Type::BOOLEAN : {InsertValue(valueHandle, ConstValueHandle(boolean(b)));break;}
							default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
						}
					}
				}
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
					if (valueHandle.getMetaType() == Meta::Type::OPTIONAL){
						Optional & refOptional = valueHandle;
						switch(((const SPL::Optional&)valueHandle).getValueMetaType()) {
							case Meta::Type::INT8 : { static_cast<optional<int8> &>(refOptional) = int8(num); break; }
							case Meta::Type::INT16 : { static_cast<optional<int16> &>(refOptional) = int16(num); break; }
							case Meta::Type::INT32 : { static_cast<optional<int32> &>(refOptional) = int32(num); break; }
							case Meta::Type::INT64 : { static_cast<optional<int64> &>(refOptional) = int64(num); break; }
							case Meta::Type::UINT8 : { static_cast<optional<uint8> &>(refOptional) = uint8(num); break; }
							case Meta::Type::UINT16 : { static_cast<optional<uint16> &>(refOptional) = uint16(num); break; }
							case Meta::Type::UINT32 : { static_cast<optional<uint32> &>(refOptional) = uint32(num); break; }
							case Meta::Type::UINT64 : { static_cast<optional<uint64> &>(refOptional) = uint64(num); break; }
							case Meta::Type::FLOAT32 : { static_cast<optional<float32> &>(refOptional) = float32(num); break; }
							case Meta::Type::FLOAT64 : { static_cast<optional<float64> &>(refOptional) = float64(num); break; }
							default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
						}
					}
					else {
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
				}
				else {
					if (valueHandle.getMetaType() == Meta::Type::OPTIONAL){
						Optional&  refOptional = (Optional&) valueHandle;
						if (refOptional.isPresent()) {
							valueHandle = refOptional.getValue();
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
						else
							SPLAPPTRC(L_DEBUG, "optional collection attribute not present, but should", "EXTRACT_FROM_JSON");
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
					if (valueHandle.getMetaType() == Meta::Type::OPTIONAL){
						Optional&  refOptional = (Optional&) valueHandle;
						switch(refOptional.getValueMetaType()) {
							case Meta::Type::BSTRING : {
								SetOptionalValueToDefault(refOptional);
								ValueHandle tmpValueHandle = refOptional.getValue();
								static_cast<BString &>(tmpValueHandle) = rstring(s,length);
								refOptional.setValue(tmpValueHandle);
								break; }
							case Meta::Type::RSTRING : {
								static_cast<optional<rstring> &>(refOptional) = rstring(s,length);
								break; }
							case Meta::Type::USTRING : {
								static_cast<optional<ustring> &>(refOptional) = ustring(s,length);
								break; }
							default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
						}
					}
					else {
						switch(valueHandle.getMetaType()) {
							case Meta::Type::BSTRING : { static_cast<BString&>(valueHandle) = rstring(s, length); break; }
							case Meta::Type::RSTRING : { static_cast<rstring&>(valueHandle) = s; break; }
							case Meta::Type::USTRING : { static_cast<ustring&>(valueHandle) = s; break; }
							default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
						}
					}
				}
				else {
					if (valueHandle.getMetaType() == Meta::Type::OPTIONAL){
						Optional&  refOptional = (Optional&) valueHandle;
						if (refOptional.isPresent()) {
							valueHandle = refOptional.getValue();
							switch(valueType) {
								case Meta::Type::BSTRING : { InsertValue(valueHandle, ConstValueHandle(bstring<1024>(s, length))); break; }
								case Meta::Type::RSTRING : { InsertValue(valueHandle, ConstValueHandle(rstring(s, length))); break; }
								case Meta::Type::USTRING : { InsertValue(valueHandle, ConstValueHandle(ustring(s, length))); break; }
								default : SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
							}

						}
						else
							SPLAPPTRC(L_DEBUG, "not matched, optional is not present", "EXTRACT_FROM_JSON");
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

				/* There is no collection open
				 * valid attribute types to which the object can be mapped
				 * 	tuple
				 * 	map
				 * 	bmap
				 * */
				if(state.inCollection == NO) {
					switch(valueHandle.getMetaType()) {
						case Meta::Type::MAP : {
							SPLAPPTRC(L_DEBUG, "matched to map", "EXTRACT_FROM_JSON");
							state.inCollection = MAP;

							switch (static_cast<Map&>(valueHandle).getKeyMetaType()) {
								case Meta::Type::RSTRING :;
								case Meta::Type::USTRING : {

									valueType = static_cast<Map&>(valueHandle).getValueMetaType();
									valueIsOptional=false;
									/* in case of optional collection type determine the optionals value type by
									 * creating a value and get the value type from this handle
									 */
									if (valueType == Meta::Type::OPTIONAL) {
										ValueHandle tmpElementValueHandle=static_cast<Map&>(valueHandle).createValue();
										valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
										tmpElementValueHandle.deleteValue();
										valueIsOptional=true;
									}

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
									valueIsOptional=false;
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
						case Meta::Type::OPTIONAL: {
							Optional & refOptional = valueHandle;
							switch(refOptional.getValueMetaType()) {
								case Meta::Type::MAP : {
									SPLAPPTRC(L_DEBUG, "matched to optional map", "EXTRACT_FROM_JSON");
									state.inCollection = MAP;

									/* make sure that a map is there */
									if (!refOptional.isPresent())
										SetOptionalValueToDefault(refOptional);

									switch (static_cast<Map&>(refOptional.getValue()).getKeyMetaType()) {
										case Meta::Type::RSTRING :;
										case Meta::Type::USTRING : {
											valueType = static_cast<Map&>(refOptional.getValue()).getValueMetaType();
											valueIsOptional=false;
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
									SPLAPPTRC(L_DEBUG, "matched to optional bounded map", "EXTRACT_FROM_JSON");
									state.inCollection = MAP;

									/* make sure that a map is there */
									if (!refOptional.isPresent())
										SetOptionalValueToDefault(refOptional);

									switch (static_cast<BMap&>(refOptional.getValue()).getKeyMetaType()) {
										case Meta::Type::RSTRING :;
										case Meta::Type::USTRING : {
											valueType = static_cast<Map&>(refOptional.getValue()).getValueMetaType();
											valueIsOptional=false;
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
									SPLAPPTRC(L_DEBUG, "matched to optional tuple", "EXTRACT_FROM_JSON");

									/* make sure that a tuple is there */
									if (!refOptional.isPresent())
										SetOptionalValueToDefault(refOptional);

									Tuple & tuple = refOptional.getValue();
									objectStack.push(TupleState(tuple));

									break;
								}
								default : {
									SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
									state.attrIter = endIter;
								}
							}
							break;
						}
						default : {
							SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
							state.attrIter = endIter;
						}
					}
				}
				/* a json object is opened and we have an actual open collection
				 * so the object is not a value to a key but new element of the actual attributes
				 * collection:
				 *     list
				 *     map */
				else {
					/* the value of a SPL collection needs to be tuple to be able to map a new JSON object
					 * otherwise just count the open objects to be in synch with EndObject() */
					if(valueType != Meta::Type::TUPLE) {
						state.objectCount++;
					}
					else {
						/* collection is list or map and collection element type is tuple
						 * create a new empty/default element(tuple) in the open attribute
						 * collection
						 * put this element on tuple stack so that it receives further
						 * SAX key/value events */
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
								/*only maps with JSON object value mapped to tuple are supported
								 * map<key,tuple<>>
								 * but not map<key,map<>>*/
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
							case Meta::Type::OPTIONAL: {
								Optional & refOptional = valueHandle;
								switch(refOptional.getValueMetaType()) {
									case Meta::Type::LIST : {
										List & listAttr = refOptional.getValue();
										ValueHandle valueElemHandle = listAttr.createElement();
										listAttr.pushBack(valueElemHandle);
										valueElemHandle.deleteValue();

										Tuple & tuple = listAttr.getElement(listAttr.getSize()-1);
										objectStack.push(TupleState(tuple));

										break;
									}
									case Meta::Type::MAP : {
										Map & mapAttr = refOptional.getValue();;
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

			/* if the tuple object containing the collection attribute (can be only map)
			 * receives an EndObject (means all possibly child tuple objects are removed
			 * from stack), than its pending open map collection is closed  */
			if(state.inCollection == MAP) {
				state.inCollection = NO;
			}
			/* only the pending open child object (tuple or map) in a list/tuple is closed
			 * and tuple object is removed from stack when
			 * object count is in synch with all StartObject and EndObject received during
			 * lifetime of open stack tuple (there may be some which are not mapped/matched)
			 * collection state remains as before.
			 * For a list it is changed in EndArray to NO
			 * For a tuple it is still NO */
			else {
				if(state.objectCount > 0)
					state.objectCount--;
				else
					objectStack.pop();
			}

			return true;
		}



		/* StartArray will match only to list/blist and set/bset attribute types
		 * hint: array of arrays resp. collection of collections is not supported
		 * */
		bool StartArray() {
			SPLAPPTRC(L_DEBUG, "array started", "EXTRACT_FROM_JSON");

			TupleState & state = objectStack.top();
			TupleIterator const& endIter = state.tuple.getEndIterator();

			/* Do we have an open attribute expecting a value?
			 * This would be the one receiving t he JSON array content
			 * and as such needs to have a SPL type being able to store values from
			 * an array (SPL list or SPL Set).*/
			if(state.attrIter != endIter) {
				ValueHandle valueHandle = (*state.attrIter).getValue();

				switch (valueHandle.getMetaType()) {
					case Meta::Type::LIST : {
						SPLAPPTRC(L_DEBUG, "matched to list", "EXTRACT_FROM_JSON");

						valueType = static_cast<List&>(valueHandle).getElementMetaType();
						valueIsOptional=false;
						if (valueType == Meta::Type::OPTIONAL) {
							ValueHandle tmpElementValueHandle=static_cast<List&>(valueHandle).createElement();
							valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
							tmpElementValueHandle.deleteValue();
							valueIsOptional=true;
						}
						state.inCollection = LIST;
						break;
					}
					case Meta::Type::BLIST : {
						SPLAPPTRC(L_DEBUG, "matched to bounded list", "EXTRACT_FROM_JSON");

						valueType = static_cast<BList&>(valueHandle).getElementMetaType();
						valueIsOptional=false;
						if (valueType == Meta::Type::OPTIONAL) {
							ValueHandle tmpElementValueHandle=static_cast<BList&>(valueHandle).createElement();
							valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
							tmpElementValueHandle.deleteValue();
							valueIsOptional=true;
						}
						state.inCollection = LIST;
						break;
					}
					case Meta::Type::SET : {
						SPLAPPTRC(L_DEBUG, "matched to set", "EXTRACT_FROM_JSON");

						valueType = static_cast<Set&>(valueHandle).getElementMetaType();
						valueIsOptional=false;
						if (valueType == Meta::Type::OPTIONAL) {
							ValueHandle tmpElementValueHandle=static_cast<Set&>(valueHandle).createElement();
							valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
							tmpElementValueHandle.deleteValue();
							valueIsOptional=true;
						}
						state.inCollection = LIST;
						break;
					}
					case Meta::Type::BSET : {
						SPLAPPTRC(L_DEBUG, "matched to bounded set", "EXTRACT_FROM_JSON");

						valueType = static_cast<BSet&>(valueHandle).getElementMetaType();
						valueIsOptional=false;
						if (valueType == Meta::Type::OPTIONAL) {
							ValueHandle tmpElementValueHandle=static_cast<BSet&>(valueHandle).createElement();
							valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
							tmpElementValueHandle.deleteValue();
							valueIsOptional=true;
						}
						state.inCollection = LIST;
						break;
					}
					case Meta::Type::OPTIONAL : {
						Optional & refOptional = valueHandle;
						switch(refOptional.getValueMetaType()) {
							case Meta::Type::LIST : {
								SPLAPPTRC(L_DEBUG, "matched to optional list", "EXTRACT_FROM_JSON");
								SetOptionalValueToDefault(refOptional);
								valueType = static_cast<List&>(refOptional.getValue()).getElementMetaType();
								valueIsOptional=false;
								if (valueType == Meta::Type::OPTIONAL) {
									ValueHandle tmpElementValueHandle=static_cast<List&>(refOptional.getValue()).createElement();
									valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
									tmpElementValueHandle.deleteValue();
									valueIsOptional=true;
								}
								state.inCollection = LIST;
								break;
							}
							case Meta::Type::BLIST : {
								SPLAPPTRC(L_DEBUG, "matched to optional bounded list", "EXTRACT_FROM_JSON");
								SetOptionalValueToDefault(refOptional);
								valueType = static_cast<BList&>(refOptional.getValue()).getElementMetaType();
								valueIsOptional=false;
								if (valueType == Meta::Type::OPTIONAL) {
									ValueHandle tmpElementValueHandle=static_cast<BList&>(refOptional.getValue()).createElement();
									valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
									tmpElementValueHandle.deleteValue();
									valueIsOptional=true;
								}
								state.inCollection = LIST;
								break;
							}
							case Meta::Type::SET : {
								SPLAPPTRC(L_DEBUG, "matched to optional set", "EXTRACT_FROM_JSON");
								SetOptionalValueToDefault(refOptional);
								valueType = static_cast<Set&>(refOptional.getValue()).getElementMetaType();
								valueIsOptional=false;
								if (valueType == Meta::Type::OPTIONAL) {
									ValueHandle tmpElementValueHandle=static_cast<Set&>(refOptional.getValue()).createElement();
									valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
									tmpElementValueHandle.deleteValue();
									valueIsOptional=true;
								}
								state.inCollection = LIST;
								break;
							}
							case Meta::Type::BSET : {
								SPLAPPTRC(L_DEBUG, "matched to optional bounded set", "EXTRACT_FROM_JSON");
								SetOptionalValueToDefault(refOptional);
								valueType = static_cast<BSet&>(refOptional.getValue()).getElementMetaType();
								valueIsOptional=false;
								if (valueType == Meta::Type::OPTIONAL) {
									ValueHandle tmpElementValueHandle=static_cast<BSet&>(refOptional.getValue()).createElement();
									valueType = static_cast<Optional&> (tmpElementValueHandle).getValueMetaType();
									tmpElementValueHandle.deleteValue();
									valueIsOptional=true;
								}
								state.inCollection = LIST;
								break;
							}
							default : {
								SPLAPPTRC(L_DEBUG, "not matched", "EXTRACT_FROM_JSON");
								state.attrIter = endIter;
							}
						}
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


		/* Inserting a value in a collection type attribute
		 * MAP/BMAP
		 * LIST/BLIST
		 * SET/BSET
		 * parameter:
		 * 		valueHandle - handle to the collection attribute
		 * 		valueElemHandle - handle to the eleemnt to be inserted
		 *
		 * The collection attribute may be optional
		 * The collections elements may be optional:
		 * 		list elements
		 * 		map Value elements (map Key type can't be optional)
		 * 		set elements can't be optional, doesn't make sense
		 *
		 * Member varibale
		 * 		valueIsOptional - indicates that the collection element value type is optional
		 * 		valueType - contains the collection element value type, if collection element value type
		 * 		            is an 'optional'  valueType is set to the  ValueType of the 'optional' and
		 * 		            valueIsOptional is set.
		 * 		lastKey - holds the name of the last read key from SAX key(), used for maps to add
		 * 				  a JSON key:value pair into a SPL map
		 * 		            */
		inline void InsertValue(ValueHandle & valueHandle, ConstValueHandle const& valueElemHandle) {
			switch (valueHandle.getMetaType()) {
				case Meta::Type::LIST : {
					InsertListElement(static_cast<List&>(valueHandle),valueIsOptional, valueElemHandle);
					break;
				}
				case Meta::Type::BLIST : {
					InsertListElement(static_cast<BList&>(valueHandle),valueIsOptional, valueElemHandle);
					break;
				}
				case Meta::Type::SET : {
					InsertSetElement(static_cast<Set&>(valueHandle),valueIsOptional, valueElemHandle);
					break;
				}
				case Meta::Type::BSET : {
					InsertSetElement(static_cast<BSet&>(valueHandle),valueIsOptional, valueElemHandle);
					break;
				}
				case Meta::Type::MAP : {
					if(static_cast<Map&>(valueHandle).getKeyMetaType() == Meta::Type::RSTRING)
						InsertMapElement(static_cast<Map&>(valueHandle),ConstValueHandle(lastKey),valueIsOptional, valueElemHandle);
					else
						InsertMapElement(static_cast<Map&>(valueHandle),ConstValueHandle(ustring(lastKey.data(), lastKey.length())),valueIsOptional, valueElemHandle);
					break;
				}
				case Meta::Type::BMAP : {
					if(static_cast<BMap&>(valueHandle).getKeyMetaType() == Meta::Type::RSTRING)
						InsertMapElement(static_cast<BMap&>(valueHandle),ConstValueHandle(lastKey),valueIsOptional, valueElemHandle);
					else
						InsertMapElement(static_cast<BMap&>(valueHandle),ConstValueHandle(ustring(lastKey.data(), lastKey.length())),valueIsOptional, valueElemHandle);
					break;
				}
				default:;
			}
		}

		/* Functions to insert elements into collections
		 * if it is a collection of optional elements
		 * - create a new optional element
		 * - set the element value
		 * - add the optional element to collection
		 * - delete the created element
		 * otherwise insert the element directly
		*/
		template <typename T>
		void InsertListElement(T & collection,bool& isOptional, ConstValueHandle const& elementHandle ) {
			if (isOptional){
				ValueHandle tmpElementValueHandle = collection.createElement();
				static_cast<Optional&>(tmpElementValueHandle).setValue(elementHandle);
				collection.pushBack(tmpElementValueHandle);
				tmpElementValueHandle.deleteValue();
			}
			else
				collection.pushBack(elementHandle);
		}

		template <typename T>
		void InsertSetElement(T& collection,bool& isOptional, ConstValueHandle const& elementHandle) {
			if (isOptional){
				ValueHandle tmpElementValueHandle = collection.createElement();
				static_cast<Optional&>(tmpElementValueHandle).setValue(elementHandle);
				collection.insertElement(tmpElementValueHandle);
				tmpElementValueHandle.deleteValue();
			}
			else
				collection.insertElement(elementHandle);
		}

		template <typename T>
		void InsertMapElement(T & collection,ConstValueHandle const& key, bool& isOptional, ConstValueHandle const& elementHandle ) {
			if (isOptional){
				ValueHandle tmpElementValueHandle = collection.createValue();
				static_cast<Optional&>(tmpElementValueHandle).setValue(elementHandle);
				collection.insertElement(key, tmpElementValueHandle);
				tmpElementValueHandle.deleteValue();
			}
			else
				collection.insertElement(key, elementHandle);
		}


		/* Function to set an Optional to present with its value
		 * default initialization,
		 * necessary e.g. to set an optional collection to present and empty
		 * */
		inline void SetOptionalValueToDefault(Optional & refOptional) {
			ValueHandle value = refOptional.createValue();
			refOptional.setValue(value);
			value.deleteValue();
		}

	private:
		// store last JSON key for creating map-collection (key,value) pairs with next JSON value event
		rstring lastKey;
		// store the element type of the collection on attribute represents, for maps it is the value type
		// of the (key,value) pair
		// for optionals it is the base type of the optional<>
		Meta::Type valueType;
		// indicate that the collection element is optional<>
		bool valueIsOptional;
		// store the stack of nested tuples, the top is the one which is open/in-work
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

	template<typename T>
	inline T parseNumber(Value * value) {
		StringBuffer str;
		Writer<StringBuffer> writer(str);
		value->Accept(writer);
		return spl_cast<T,SPL::rstring>::cast( SPL::rstring(str.GetString()));
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
			if( is_same<SPL::decimal32, T>::value)	return parseNumber<T>(value);
			if( is_same<SPL::decimal64, T>::value)	return parseNumber<T>(value);
			if( is_same<SPL::decimal128, T>::value)	return parseNumber<T>(value);
		}
		else if(value->IsString())	{
			status = 1;

			try {
				return lexical_cast<T>(value->GetString());
			}
			catch(bad_lexical_cast const&) {
				status = 2;
			}
		}
		else
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
						return parseNumber<T>(value);
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
