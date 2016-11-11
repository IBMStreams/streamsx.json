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
import com.ibm.streamsx.json.i18n.Messages;

/**
 * This is a utility class
 * It requires a filename containing json object/array as input
 * It will print an SPL structure that closely matches the JSON structure.
 * @author rwagle
 *
 */
public class JSONMain {

  static String TAB = "  "; //$NON-NLS-1$
  public static void main(String[] args)  {
    if(args.length!=1) {
      System.err.println(Messages.getString("SPECIFY_FILE_CONTAINING_JSON")); //$NON-NLS-1$
      System.exit(1);
    }
    try {
	    String jsonString = readFile(args[0]);
	    
		List<String> typeList = new ArrayList<String>();
	    if(jsonString.startsWith("[")) { //$NON-NLS-1$
	    	JSONArray jarr = JSONArray.parse(jsonString);
	    	String ret = print(jarr, typeList, "", "Main") ; //$NON-NLS-1$ //$NON-NLS-2$
	    	
	    	for(String s : typeList) {
	    		System.out.println(s+"\n"); //$NON-NLS-1$
	    	}
	    	System.out.println("type MainListType = " + ret + ";"); //$NON-NLS-1$ //$NON-NLS-2$
	    }
	    else {
	    	JSONObject obj = JSONObject.parse(jsonString);
	    	print(obj, typeList, "", "Main"); //$NON-NLS-1$ //$NON-NLS-2$
	    	for(String s : typeList) {
	    		System.out.println(s+"\n"); //$NON-NLS-1$
	    	}
	    }
    }catch(Exception e) {
    	System.err.println(Messages.getString("UNABLE_TO_GENERATE_SPL_TYPES")); //$NON-NLS-1$
    	e.printStackTrace();
    	System.exit(1);
    }
  }
  
  static String readFile(String fname) throws IOException {
    StringBuffer sb = new StringBuffer();
    BufferedReader br = new BufferedReader(new FileReader(fname));
    String line = null;
    while((line = br.readLine()) != null) {
      if(line.trim().isEmpty()) continue;
      sb.append(line.trim());
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
    String val = ""; //$NON-NLS-1$
    for(Map.Entry<Object, Object> keyset : entryset) {
      if(!val.equals("")) //$NON-NLS-1$
        val += ", "; //$NON-NLS-1$
      val += print(keyset.getValue(), typeList, parent+self, keyset.getKey().toString()) + " " + keyset.getKey(); //$NON-NLS-1$
    }
    typeList.add("type " + self + "Type = " + val + ";"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    return self + "Type"; //$NON-NLS-1$
    
  }
  static String print(Object value, List<String> typeList, String parent, String self) {
    if(value == null) return "UNKNOWN_TYPE"; //$NON-NLS-1$
    if(value.getClass().equals(JSONObject.class)) {
      return print((JSONObject)value, typeList, parent, self);
    }
    else if(value.getClass().equals(JSONArray.class)) {
      return print((JSONArray) value, typeList, parent, self);
    }
    else {
      String clname = value.getClass().getSimpleName();
      String val = ""; //$NON-NLS-1$
      if(clname.equals("String"))  val = "rstring"; //$NON-NLS-1$ //$NON-NLS-2$
      else if(clname.equals("Long"))  val = "int64"; //$NON-NLS-1$ //$NON-NLS-2$
      else if(clname.equals("Float"))  val = "float32"; //$NON-NLS-1$ //$NON-NLS-2$
      else if(clname.equals("Double"))  val = "float64"; //$NON-NLS-1$ //$NON-NLS-2$
      else if(clname.equals("Boolean"))  val = "boolean"; //$NON-NLS-1$ //$NON-NLS-2$
      else {
        System.err.println(Messages.getString("UNSUPPORTED_TYPE") + clname + " : " + parent + self); //$NON-NLS-1$ //$NON-NLS-2$
        System.exit(1);
      }
      return val;
    }
  }
  

  static void print(Object key, Object value, String tab) {
    if(value == null) return;
    if(value.getClass().equals(JSONObject.class)) {
      System.out.println(tab + "Tuple: " + key); //$NON-NLS-1$
      print((JSONObject) value, tab + TAB);
    }
    else if(value.getClass().equals(JSONArray.class)) {
      System.out.println(tab + "List: " + key); //$NON-NLS-1$
      print((JSONArray) value, tab + TAB);
    }
    else {
      System.out.println(tab + value.getClass().getSimpleName() + " " + key ); //$NON-NLS-1$
    }
  }
  
  static String print(JSONArray obj, List<String> typeList, String parent, String self) {
    @SuppressWarnings("unchecked")
	Iterator<Object> it = obj.iterator();
    if(it.hasNext()) {
      Object nobj = it.next();
      return "list<" + print(nobj, typeList, parent+self, self) + ">"; //$NON-NLS-1$ //$NON-NLS-2$
    }
    return "list<UNKNOWN_TYPE>"; //$NON-NLS-1$
  }

  static void print(JSONArray obj, String tab) {
    @SuppressWarnings("unchecked")
	Iterator<Object> it = obj.iterator();
    if(it.hasNext()) {
      Object nobj = it.next();
      print("", nobj, tab + TAB); //$NON-NLS-1$
    }
  }
  
}
