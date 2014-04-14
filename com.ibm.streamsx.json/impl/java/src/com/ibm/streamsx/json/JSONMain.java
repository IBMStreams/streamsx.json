//
// *******************************************************************************
// * Copyright (C)2014, International Business Machines Corporation and *
// * others. All Rights Reserved. *
// *******************************************************************************
//
package com.ibm.streamsx.json;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;

//this is a utility class
//give it a filename containing json object as an arg
//it will print the structure of the json object to help you write a tuple/object with the same structure
public class JSONMain {

  static String TAB = "  ";
  public static void main(String[] args) throws Exception {
    if(args.length!=1) {
      System.err.println("Please specify a file containing json string.");
      System.exit(1);
    }
    JSONObject obj = JSONObject.parse(readFile(args[0]));
    //    print(obj, "");
    List<String> typeList = new ArrayList<String>();
    print(obj, typeList, "", "Main");
    for(String s : typeList) {
      System.out.println(s+"\n");
    }
  }
  
  static String readFile(String fname) throws IOException {
    StringBuffer sb = new StringBuffer();
    BufferedReader br = new BufferedReader(new FileReader(fname));
    String line = null;
    while((line = br.readLine()) != null) {
      sb.append(line);
      break;//read one line
    }
    br.close();
    return sb.toString();
  }
  
  static void print(JSONObject obj, String tab) {
    @SuppressWarnings("unchecked")
	Set<Map.Entry<Object, Object>> entryset = obj.entrySet();
    for(Map.Entry<Object, Object> keyset : entryset) {
      print(keyset.getKey(), keyset.getValue(), tab + TAB);
    }
  }
  
  static String print(JSONObject obj, List<String> typeList, String parent, String self) {
    @SuppressWarnings("unchecked")
	Set<Map.Entry<Object, Object>> entryset = obj.entrySet();
    String val = "";
    for(Map.Entry<Object, Object> keyset : entryset) {
      if(!val.equals(""))
        val += ", ";
      val += print(keyset.getValue(), typeList, parent+self, keyset.getKey().toString()) + " " + keyset.getKey();
    }
    typeList.add("type " + self + "Type = " + val + ";");
    return self + "Type";
    
  }
  static String print(Object value, List<String> typeList, String parent, String self) {
    if(value == null) return "UNKNOWN_TYPE";
    if(value.getClass().equals(JSONObject.class)) {
      return print((JSONObject)value, typeList, parent, self);
    }
    else if(value.getClass().equals(JSONArray.class)) {
      return print((JSONArray) value, typeList, parent, self);
    }
    else {
      String clname = value.getClass().getSimpleName();
      String val = "";
      if(clname.equals("String"))  val = "rstring";
      else if(clname.equals("Long"))  val = "int64";
      else if(clname.equals("Boolean"))  val = "boolean";
      else {
        System.err.println("Unsupported type: " + clname + " : " + parent + self);
        System.exit(1);
      }
      return val;
    }
  }
  

  static void print(Object key, Object value, String tab) {
    if(value == null) return;
    if(value.getClass().equals(JSONObject.class)) {
      System.out.println(tab + "Tuple: " + key);
      print((JSONObject) value, tab + TAB);
    }
    else if(value.getClass().equals(JSONArray.class)) {
      System.out.println(tab + "List: " + key);
      print((JSONArray) value, tab + TAB);
    }
    else {
      System.out.println(tab + value.getClass().getSimpleName() + " " + key );
    }
  }
  
  static String print(JSONArray obj, List<String> typeList, String parent, String self) {
    @SuppressWarnings("unchecked")
	Iterator<Object> it = obj.iterator();
    if(it.hasNext()) {
      Object nobj = it.next();
      return "list<" + print(nobj, typeList, parent+self, self) + ">";
    }
    return "list<UNKNOWN_TYPE>";
  }

  static void print(JSONArray obj, String tab) {
    @SuppressWarnings("unchecked")
	Iterator<Object> it = obj.iterator();
    if(it.hasNext()) {
      Object nobj = it.next();
      print("", nobj, tab + TAB);
    }
  }
  
}
