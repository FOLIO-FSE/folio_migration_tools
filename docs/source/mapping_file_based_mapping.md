# Mapping file based mapping

## The mapping file
The mapping file is a json file, with one element, "data" that is an array of *Mapping entries*.

A typical mapping file looks like this:
```
{
    "data": [
        {
            "folio_field": "active",
            "legacy_field": "Not mapped",
            "value": "",
            "description": "",
            "fallback_legacy_field": "",
            "fallback_value": ""
        },
        {
            "folio_field": "barcode",
            "legacy_field": "P BARCODE",
            "value": "",
            "description": "",
            "fallback_legacy_field": "",
            "fallback_value": ""
        },

        [...]

        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "RECORD #(PATRON)",
            "value": "",
            "description": "",
            "fallback_legacy_field": "",
            "fallback_value": ""
        }
    ]
}
```    
You do not have to map every FOLIO property. You can either leave the entries unmapped or remove them from the file alltogether.

## The mapping file mapping entries
A typical entry looks like this:
```
{
    "folio_field": "email",
    "legacy_field": "RECORD #(PATRON)",
    "value": "",
    "description": "",
    "fallback_legacy_field": "",
    "fallback_value": ""
}
```
folio_field and legacy_field are mandatory. All other fields are optional. 
### The priority of the mappings in the mapping entry:
There might be exceptions to this in some areas, but generally, this is the how the mapping works:
1. If there are reference data mappings or special cases for particular fields, then this has precedence
2. Values added to the *value* field are returned immediately without any further manipulation
3. Then, the *legacy field* value gets extracted from the source record.
4. If there is an entry for rules.replaceValues, the extracted value is run through this process
5. If there is an entry for rules.regexGetFirstMatchOrEmpty, the extracted value is run through this process as well and then return the value
6. If the above steps does not result in a value, and if there is a fallback field in the legacy data, mapped by the *fallback_legacy_field*, this field will be returned
7. If none of the above have resulted in a value, and there is an entry for *fallback_value* in the mapping entry, this value will be returned

*If there are multiple mapping entries for the same FOLIO field, the results from the above process will get concatenated with a space between them, in the order that they appear in the mapping file.*


### The folio_field property
The folio_field must contain the target folio field.

_String array properties_   
If the target field is an array, you must add a [0] at the end of the field for the first element in the arry and a [1] in the second.

Example: If you have two fields in your legacy data you want to add to the formerIds string array property, your mapping entry should look like this:

```
{
    "folio_field": "formerIds[0]",
    "legacy_field": "RECORD #(item)"
},
{
    "folio_field": "formerIds[1]",
    "legacy_field": "RECORD #(bib)"
}
```
This would render the following results:
```
{
    "formerIds":[
        "value of RECORD #(item)",
        "value of RECORD #(bib)"
    ]
}
```
_Object array properties_   
If the target field is a property of an object in an array, you must add a [0] at the end of each object in the array.

Example: you have an array of addresses with, and each patron has two addresses.

```
{
    "folio_field": "addresses[0].city",
    "legacy_field": "office city"
},
{
    "folio_field": "addresses[0].zip",
    "legacy_field": "office zip"
}
{
    "folio_field": "addresses[1].city",
    "legacy_field": "home city"
},
{
    "folio_field": "addresses[1].zip",
    "legacy_field": "home zip"
}
```
This would render the following results:
```
{
    "addresses":[
        {
            "city":"value of office city",
            "zip": "value of office zip"
        },
        {
            "city":"value of home city",
            "zip": "value of home zip"
        }
    ]
}
```

### The legacy_field property
This field should contain the name of the column in the TSV source data file. 
_Turn off mapping_   
If you do not want it mapped, just add "Not mapped" as the value, or an empty string:
```
{
    "folio_field": "addresses[0].city",
    "legacy_field": "Not mapped"
},
{
    "folio_field": "addresses[0].zip",
    "legacy_field": ""
}
```

### The value property
The value property is a way to add the same value to all records. 

```{caution}
⚠️ The value field has preceedence over all other mappings. If you put a value here, no other mappings will be taken into consideration
```

### The description property
The description field is used for your own notes.

### The fallback_legacy_field property
The fallback_legacy_field is used as a falback, so when the legacy_field and the fallback_legacy_field is mapped and has a value, this value will be used.

### The fallback_value property
The fallback_value is used as a last resort, so if no other mappings have returned a value, this value will be set.

### The rules mapping entry
This is a placeHolder for more advanced mappings. 

### rules.regexGetFirstMatchOrEmpty
This propety should contain a regular expression with a capturing group. The resulting string will be the first capturing group. Imagine the following mapping:

```
{
    "folio_field": "username",
    "legacy_field": "EMAIL",
    "value": "",
    "description": "",
    "fallback_legacy_field": "RECORD #(PATRON)",
    "rules": {
        "regexGetFirstMatchOrEmpty": "(.*)@.*"
    }
}
```
If the contents of the EMAIL field is *someone@example.com*, the resulting string will be *someone*
If there is no match, the empty string will be returned.

### rules.replaceValues
This rule allows you to map codes to strings. Given the following mapping:

```
{
    "folio_field": "notes[0].title",
    "legacy_field": "STATUS",
    "value": "",
    "description": "",
    "fallback_legacy_field": "",
    "rules": {
        "replaceValues": {
            "0": "Graduate",
            "a": "Alumni"
        }
    }
},
```

If the STATUS field contains *0*, then the resulting value in the note title will be *Graduate*.
If no match is made, the original string will be returned. So if STATUS is *1*, then the note title will be *1*.
