---
title: "Toolkit Development overview"
permalink: /docs/developer/overview/
excerpt: "Contributing to this toolkits development."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}


## Build

To compile the toolkit, run ant in the toolkit directory.

## Utilities
### Type Helper
A helper utility to assist in creating SPL tuples from JSON strings.  

```streamsx.json/com.ibm.streamsx.json/scripts/createTypes.sh <json file>```  

This utility expects at least one JSON string in the file. It will print SPL types that closely match the JSON structure. 
Note that if there are any empty values in the JSON string, an UNKNOWN_TYPE type will be specified.
