-module(dby_db).

-export([transaction/1,
         abort/1,
         write/1,
         read/1,
         delete/1]).

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

delete(Key) ->
    ok = mnesia:delete(Key).
