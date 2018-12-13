---
title: "Toolkit Development overview"
permalink: /docs/developer/overview/
excerpt: "Contributing to this toolkits development."
last_modified_at: 2017-12-13T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}

##Supported IBM Streams versions

With IBM Streams 4.3 the new type **optional&lt;&gt;** was introduced. Providing support for this feature makes the new JSON toolkit versions (&gt;1.5) incompatible with  IBM Streams releases &lt; 4.3.

New toolkit releases are developed in **develop** branch and released on **master** branch.

To support IBM Streams releases &lt;4.3 with fixes for the JSON toolkit there is the 1.4.x_m branch where the fixes are developed and released.


## Build

The toolkit is build with Java 8 which is also contained in the IBM Streams installation directory.
Please check your environment settings for JAVA.
If you want to use the Java 8 in the IBM Streams installation directory put the following lines to your .bashrc.

```
source /home/<your_streamsowner>/InfoSphere_Streams/4.3.0.0/bin/streamsprofile.sh
export JAVA_HOME=$STREAMS_INSTALL/java
export PATH=$JAVA_HOME/bin:$PATH
```

To compile the toolkit, run ant in the toolkit directory.

```ant clean all```

## Utilities
### Type Helper
A helper utility to assist in creating SPL tuples from JSON strings.  

```streamsx.json/com.ibm.streamsx.json/scripts/createTypes.sh <json file>```  

This utility expects at least one JSON string in the file. It will print SPL types that closely match the JSON structure. 
Note that if there are any empty values in the JSON string, an UNKNOWN_TYPE type will be specified.
