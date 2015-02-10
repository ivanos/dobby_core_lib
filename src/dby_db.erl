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
        {atomic, ok} ->
            ok
    end.

abort(Reason) ->
    {atomic, ok} = mnesia:abort(Reason),
    ok.

write(Record) ->
    {atomic, ok} = mnesia:write(Record),
    ok.

read(Key) ->
    mnesia:read(Key).

delete(Key) ->
    {atomic, ok} = mnesia:delete(Key),
    ok.
