-module(dby_db).

-export([transaction/1,
         abort/1,
         write/1,
         read/1]).

transaction(Fn) ->
    mnesia:transaction(Fn).

abort(Reason) ->
    mnesia:abort(Reason).

write(Record) ->
    mnesia:write(Record).

read(Key) ->
    mnesia:read(Key).
