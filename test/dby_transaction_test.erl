-module(dby_transaction_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("dobby_clib/include/dobby.hrl").
-include("../src/dobby.hrl").

dby_subscription_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
       fun each_setup/0,
       [
        {"commit", fun transaction1/0},
        {"abort", fun transaction2/0},
        {"readfn", fun transaction3/0}
       ]
     }
    }.

setup() ->
    ok = meck:new(dby_subscription),
    ok = meck:expect(dby_subscription, publish, 3, ok).

cleanup(ok) ->
    ok = meck:unload(dby_subscription).

each_setup() ->
    ok = meck:reset(dby_subscription).

transaction1() ->
    % commit transaction
    T = new_transaction(),
    ok = dby_transaction:publish(T, identifier1(<<"A">>, [<<"sub1">>])),
    ok = dby_transaction:publish(T, identifier1(<<"B">>, [<<"sub1">>, <<"sub2">>])),
    ok = dby_transaction:publish(T, identifier1(<<"C">>, [<<"sub3">>])),
    ok = dby_transaction:commit(T, persistent),
    wait(T),
    timer:sleep(10),
    ?assertEqual(1, meck:num_calls(dby_subscription, publish, [<<"sub1">>, persistent, '_'])),
    ?assertEqual(1, meck:num_calls(dby_subscription, publish, [<<"sub2">>, persistent, '_'])),
    ?assertEqual(1, meck:num_calls(dby_subscription, publish, [<<"sub3">>, persistent, '_'])).

transaction2() ->
    % abort transaction
    T = new_transaction(),
    ok = dby_transaction:publish(T, identifier1(<<"A">>, [<<"sub1">>])),
    ok = dby_transaction:publish(T, identifier1(<<"B">>, [<<"sub1">>, <<"sub2">>])),
    ok = dby_transaction:publish(T, identifier1(<<"C">>, [<<"sub3">>])),
    ok = dby_transaction:abort(T),
    wait(T),
    ?assertEqual(0, meck:num_calls(dby_subscription, publish, '_')).

transaction3() ->
    T = new_transaction(),
    ok = dby_transaction:publish(T, identifier1(<<"A">>, [<<"sub1">>])),
    ok = dby_transaction:publish(T, identifier1(<<"B">>, [<<"sub1">>])),
    ok = dby_transaction:delete(T, dby_test_utils:identifier_delete(<<"C">>)),
    ok = dby_transaction:commit(T, message),
    wait(T),
    timer:sleep(10),
    ReadFn = read_fn(meck:history(dby_subscription)),
    ?assertEqual([#identifier{id = <<"C">>, metadata = delete, links = #{}}], ReadFn({identifier, <<"C">>})),
    ?assertMatch([#identifier{id = <<"B">>}], ReadFn({identifier, <<"B">>})),
    ?assertMatch([#identifier{id = <<"A">>}], ReadFn({identifier, <<"A">>})).

% ------------------------------------------------------------------------------
% helper functions
% ------------------------------------------------------------------------------

read_fn(History) ->
    [{_, {dby_subscription, publish, [_, _, ReadFn]}, _}] = History,
    ReadFn.

new_transaction() ->
    {ok, Pid} = dby_transaction:start_link(),
    Pid.

wait(Pid) ->
    case is_process_alive(Pid) of
        true ->
            timer:sleep(10),
            wait(Pid);
        false ->
            ok
    end.

identifier1(Id, Subscriptions) ->
    dby_test_utils:identifier1(Id, [{sub, Sub} || Sub <- Subscriptions]).

tr() ->
    dbg:start(),
    dbg:tracer(),
    dbg:p(all, c),
    % dbg:tpl(dby_subscription, [{'_', [], [{return_trace}]}]).
    dbg:tpl(dby_transaction, []).
