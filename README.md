# bulk_es
A set of simple ElasticSearch bulk import tools from source files such as: JSON, CSV, and XML. With options to stream and parallel bulk import.

## Configuration options:
### Main section
Set values to connect to the ES instance, data source:

`[main]`

* `host` Hostname
* `port` 
* `index`
* `type`
* `source_path` source of the data
* `index_settings` <Optional> the index definition in JSON format, this allows to manually control the field types and other index settings.

### Data source type sections:

`[csv]`

`delimiter`: possible values:

   * ','  : comma
   * '\t' : tab
   * ' '  : space

`header`: the names of the fields comma separated

`[xml]`

`parser`: The custom parser function to convert the XML to a list of dicts. It must implement a method:
```
def parse(file_path):
  pass
```
A basic XML parser is provided useful for XML containing a simple list, see bellow.
`xml_parser_flat.py`: this file allows to parse a XML file that generally look like it contains a list, such as:

```
<?xml version="1.0" encoding="UTF-8"?>
<root_tag ...>
  <list_item_tag ...>
    <item_property_tag ...>
    </item_property_tag>
  </list_item_tag>
</root_tag>

```
Where `...` means namespaces, attributes etc, which will be appended to the value (text) of the element.

## To use:
It requires python 3

