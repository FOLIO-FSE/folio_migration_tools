# Mapping file based mapping

## The mapping file
The mapping file is a json file, with one element, "data" that is an array of *Mapping entries*. For convenience, there is a web tool, the [Data mapping file creator](https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation) that simplifies the creation of these files.

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

You do not have to map every FOLIO property. You can either leave the entries unmapped or remove them from the file alltogether.

```
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

⚠️ The value field has preceedence over all other mappings. If you put a value here, no other mappings will be taken into consideration 
### The description property
The description field is used for your own notes.

### The fallback_legacy_field property
The fallback_legacy_field is used as a falback, so when the legacy_field and the fallback_legacy_field is mapped and has a value, this value will be used.

### The fallback_value property
The fallback_value is used as a last resort, so if no other mappings have returned a value, this value will be set.

### The rules mapping entry
TBA
