-module(dby_transaction_test).

-include_lib("eunit/include/eunit.hrl").

-define(TRANSACTION, transactionid).

dby_transaction_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"run", fun run/0},
        {"abort", fun abort/0}
     ]
    }.

setup() ->
    ok = meck:new(dby_db, [no_history]),
    ok = meck:expect(dby_db, transaction, fun(Fn) -> Fn() end),
    ok = meck:expect(dby_db, abort, 1, ok).

cleanup(ok) ->
    ok = meck:unload(dby_db).

run() ->
    ?assertEqual({ok, return}, dby_transaction:run(fun(_) -> {atomic, return} end)),
    ?assertEqual({user_abort, aborted}, dby_transaction:run(fun(_) -> {aborted, {abort, aborted}} end)),
    ?assertEqual({user_error, error}, dby_transaction:run(fun(_) -> {aborted, {user_error, error}} end)),
    ?assertEqual({error, {mnesia_error, mnesia}}, dby_transaction:run(fun(_) -> {aborted, mnesia} end)).

abort() ->
    ?assertEqual(ok, dby_transaction:abort(?TRANSACTION, reason)).
