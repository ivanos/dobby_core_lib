Dobby
=====
Dobby is a map server modeled to a certain degree after the IF-Map
server.  Unlike the IF-Map server, Dobby is intended to be a more
general purpose graph database that applications can then use for
whatever purpose they require.

This is an open source project sponsored by Infoblox.

# Requirements
- Erlang R17+

# Building
```
% rebar get-deps
% rebar compile
% rebar generate
```

# Running
```
% rel/dobby/bin/dobby console
```

# Clients
Use `dobby_clib` to send commands to the dobby server.


# Origin of Name
The Infoblox OpenFlow controller projects follow a fabric theme.
The overall project is called LOOM.  Dobby fabric is a type of
fabric.  Coincidentally it is also the name of a character in the
Harry Potter franchise.
