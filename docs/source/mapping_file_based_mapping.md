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
2. Values added to the *value* field are returned immediately without any further manipulation (rules are not applied)
3. If *value* is not set, a source value is resolved in this order:
    - *legacy_field*
    - *fallback_legacy_field* (string or ordered array, first non-empty)
    - *fallback_value*
4. By default, if a source value was resolved from *legacy_field* or *fallback_legacy_field*, rules are applied once to that resolved value.
5. If the resolved value came from *fallback_value*, rules are not applied (literal fallback behavior, same as *value*).

#### Rule execution order
The following order applies only when rules are applied (that is, when the resolved source is *legacy_field* or *fallback_legacy_field*):
1. If there is an entry for rules.regexGsub, it is applied first.
2. If there is an entry for rules.replaceValues, it is applied next.
3. If there is an entry for rules.regexGetFirstMatchOrEmpty, it is applied last.

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

### Handling required fields in object arrays

When mapping to object arrays (for example `contacts[0].firstName` or `electronicAccess[0].uri`), required-field validation is handled per array item.

| Item state | Missing required fields | Outcome | Logged as field mapping issue |
|-----------|--------------------------|---------|-------------------------------|
| Item has all required fields | No | Item is kept | No |
| Item has legacy-sourced content | Yes | Item is discarded | Yes |
| Item has only static mapped values (from `value`) | Yes | Item is discarded | No |

Example:

```
{
    "folio_field": "electronicAccess[0].relationshipId",
    "legacy_field": "",
    "value": "23d1669c-a32d-5bd0-b232-ac40181a5c7e"
},
{
    "folio_field": "electronicAccess[0].uri",
    "legacy_field": "LINK",
    "value": ""
}
```

If `uri` is required and `LINK` is empty, the item only contains static content (`relationshipId`) and is discarded.

If `LINK` has content, the item is treated as legacy-content, and missing required fields are logged as a field mapping issue before the item is discarded.

See also: [Migration Reports](migration_reports.md) and [Logging](logging.md).

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
The fallback_legacy_field is used as a fallback, so when the legacy_field does not produce a value, fallback_legacy_field is checked.

This property accepts either:
- A string (backward-compatible behavior)
- An array of strings for ordered fallback resolution

Examples:

Single fallback field:
```
{
    "folio_field": "email",
    "legacy_field": "PRIMARY EMAIL",
    "fallback_legacy_field": "SECONDARY EMAIL"
}
```

Ordered fallback fields:
```
{
    "folio_field": "email",
    "legacy_field": "PRIMARY EMAIL",
    "fallback_legacy_field": [
        "SECONDARY EMAIL",
        "WORK EMAIL",
        "RECORD #(PATRON)"
    ]
}
```

### The fallback_value property
The fallback_value is used as a last resort, so if no other mappings have returned a value, this value will be set.

The fallback_value is treated as a literal fallback (same behavior as *value*), so rules are not applied to it.

### The rules mapping entry
This is a placeHolder for more advanced mappings.

Rules are applied once after a source value has been resolved from *legacy_field* or *fallback_legacy_field*.

If *value* is explicitly set in the mapping entry, that literal value is returned directly and rules are not applied.

If *fallback_value* is set, it is only used after *legacy_field* and *fallback_legacy_field* resolution returns no value. When used, *fallback_value* is treated as a literal and rules are not applied.

Optional: you can control this behavior with *rules_apply_scope* in a mapping entry:
- *resolved_non_literal* (default): apply rules for *legacy_field* and *fallback_legacy_field* sources
- *legacy_only*: apply rules only when the resolved source is *legacy_field*
- *none*: do not apply rules for that mapping entry

Example:
```
{
    "folio_field": "email",
    "legacy_field": "PRIMARY EMAIL",
    "fallback_legacy_field": "SECONDARY EMAIL",
    "rules_apply_scope": "legacy_only",
    "rules": {
        "regexGetFirstMatchOrEmpty": "(.*)@.*"
    }
}
```

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

## Validation of Hardcoded Note Type Values

When mapping item or holdings note types (`itemNoteTypeId` or `holdingsNoteTypeId` fields), any hardcoded values specified in the `value` or `fallback_value` properties are validated at mapper initialization. This ensures that mapping files are correct before data transformation begins.

### Validation Rules

Each hardcoded note type value is validated against the FOLIO tenant's available note types:
- **Empty values** are allowed and skipped
- **UUID values** must exist in FOLIO's note type repository
- **Name values** must match a valid FOLIO note type name (case-insensitive)

### Error Handling

If any hardcoded note type value is invalid, a comprehensive error is raised listing all invalid values with their locations in the mapping file:

```
Invalid note type values found in field mapping:
  - 'invalid-type-1' (in field: notes[0].itemNoteTypeId, value) - name not found in FOLIO
  - '99999999-9999-9999-9999-999999999999' (in field: notes[1].itemNoteTypeId, value) - UUID not found in FOLIO
  - 'unknown-note' (in field: notes[2].holdingsNoteTypeId, fallback_value) - name not found in FOLIO

Available note types: action-note, public-note, staff-note, ...

Please update your mapping file with valid note type names or UUIDs.
```

### Example: Valid Configuration

```json
{
    "folio_field": "notes[0].itemNoteTypeId",
    "legacy_field": "",
    "value": "staff-note",
    "description": "Always use staff note type",
    "fallback_value": ""
},
{
    "folio_field": "notes[1].itemNoteTypeId",
    "legacy_field": "NOTE_TYPE",
    "value": "",
    "description": "Map from legacy data, fallback to public-note",
    "fallback_value": "public-note"
}
```

This validation ensures that mapping configuration errors are caught immediately, rather than during data processing when iteration is more costly.
