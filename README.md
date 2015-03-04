# Dobby
Dobby is a map server modeled to a certain degree after the IF-Map
server.  Unlike the IF-Map server, Dobby is intended to be a more
general purpose graph database that applications can then use for
whatever purpose they require.

This is an open source project sponsored by Infoblox.

## Requirements
- Erlang R17+

## Building
```
% rebar get-deps
% rebar compile
% rebar generate
```

## Running
```
% rel/dobby/bin/dobby console
```

## Clients
Use `dobby_clib` to send commands to the dobby server.

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

## Origin of Name
The Infoblox OpenFlow controller projects follow a fabric theme.
The overall project is called LOOM.  Dobby fabric is a type of
fabric.  Coincidentally it is also the name of a character in the
Harry Potter franchise.
