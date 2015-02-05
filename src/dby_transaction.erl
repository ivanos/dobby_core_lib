-module(dby_transaction).

-export([run/1,
         abort/2]).

run(Fn) ->
    TransactionId = {node(), make_ref()},
    case dby_db:transaction(fun() -> Fn(TransactionId) end) of
        {atomic, Return} -> {ok, Return};
        {aborted, {abort, Reason}} -> {user_abort, Reason};
        {aborted, {user_error, Reason}} -> {user_error, Reason};
        {aborted, Reason} -> {error, {mnesia_error, Reason}}
    end.

abort(_, Reason) ->
    dby_db:abort({abort, Reason}).
