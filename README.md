# Dobby
Dobby is a map server modeled to a certain degree after the IF-Map
server.  Unlike the IF-Map server, Dobby is intended to be a more
general purpose graph database that applications can then use for
whatever purpose they require.

This repository, dobby_core_lib, is the implementation of Dobby which
can be used as a dependency to build nodes containing a dobby server.
https://github.com/ivanos/dobby_core_node.git runs Dobby as a
standalone service.

This is an open source project sponsored by Infoblox.

<!-- markdown-toc start - Don't edit this section. Run M-x markdown-toc/generate-toc again -->
**Table of Contents**

- [Dobby](#dobby)
    - [Requirements](#requirements)
    - [Building](#building)
    - [Running](#running)
    - [Clients](#clients)
    - [Utility Functions](#utility-functions)
        - [json import/export format](#json-importexport-format)
    - [Origin of Name](#origin-of-name)

<!-- markdown-toc end -->


## Requirements
- Erlang R17+

## Building
To build the application call: `make`.

## Running

To run `dobby` as an Erlang node use
[dobby_allinone_node](https://github.com/ivanos/dobby_allinone_node).

To run `dobby` straight away call `make dev`.

## Clients
Use [dobby_clib](https://github.com/ivanos/dobby_clib) to send commands
to the dobby server.

## Utility Functions
- dby_bulk:export(Format, Filename): writes the graph database to the
named file in the specified format.  The only supported format is `json`.
- dby_bulk:import(Format, Filename): read the graph database from the
named file in the specified format.  The only supported format is `json`.

### json import/export format
The json import/export file is a list of json objects.  There are two type
of objects: Identifiers and Links.

Identifiers:
```
{
    "identifier": Id,
    "metadata" : {
        "metakey": {
            "value": "metavalue",
            "pubisher_id": "publisher",
            "timestamp": "2015-03-04T00:45:54Z"
        }
    }
}
```

Links:
```
{
    "link": [Id, Id],
    "metadata" : {
        "metakey": {
            "value": "metavalue",
            "pubisher_id": "publisher",
            "timestamp": "2015-03-04T00:45:54Z"
        }
    }
}
```

Where an Id is a string, keys are strings, and metadata values are one of:
1. true
2. false
3. null
4. string
5. list
5. object (associative array)

Example:
```
[
    {
        "identifier": "A",
        "metadata": {
                        "metakey": {
                            "value": "metavalue-for-A",
                            "pubisher_id": "publisher",
                            "timestamp": "2015-03-04T00:45:54Z"
                        }
                    }
    },
    {
        "identifier": "B",
        "metadata": {
                        "metakey": {
                            "value": "metavalue-for-B",
                            "pubisher_id": "publisher",
                            "timestamp": "2015-03-04T00:45:54Z"
                        }
                    }
    },
    {
        "link": ["A", "B"],
        "metadata": {
                        "metakey": {
                            "value": "metavalue-for-link",
                            "pubisher_id": "publisher",
                            "timestamp": "2015-03-04T00:45:54Z"
                        }
                    }
    }
]
```

Older style export file has this format:
```
[
    {
        "identifier": "A",
        "metadata": {
                        "metakey": "metavalue-for-A"
                    }
    },
    {
        "identifier": "B",
        "metadata": {
                        "metakey": "metavalue-for-B"
                    }
    },
    {
        "link": ["A", "B"],
        "metadata": {
                        "metakey": "metavalue-for-link"
                    }
    }
]
```

The older style export files may be imported using:

```
dby_bulk:import(json0, Filename)
```

## Origin of Name
The Infoblox OpenFlow controller projects follow a fabric theme.
The overall project is called LOOM.  Dobby fabric is a type of
fabric.  Coincidentally it is also the name of a character in the
Harry Potter franchise.
