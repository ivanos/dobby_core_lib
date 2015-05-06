-module(dby_db).

-export([start/0,
         transaction/1,
         abort/1,
         write/1,
         read/1,
         exists/1,
         delete/1,
         foldl/3,
         clear/0]).

start() ->
    dby_mnesia:start().

transaction(Fn) ->
    case mnesia:transaction(Fn) of
        {aborted, Reason} ->
            {error, {mnesia, Reason}};
        {atomic, Ret} ->
            Ret
    end.

abort(Reason) ->
    mnesia:abort(Reason).
    % does not return

write(Record) ->
    ok = mnesia:write(Record).

read(Key) ->
    mnesia:read(Key).

exists(Key) ->
    [] /= mnesia:read(Key).

delete(Key) ->
    ok = mnesia:delete(Key).

foldl(Fn, Acc, Table) ->
    mnesia:foldl(Fn, Acc, Table).

clear() ->
    dby_mnesia:clear().
