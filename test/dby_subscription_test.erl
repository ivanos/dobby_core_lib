-module(dby_subscription_test).

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
        {"create", fun subscription1/0},
        {"publish - no delta", fun subscription2/0},
        {"publish - deltafn nodelta", fun subscription3/0},
        {"publish - deltafn stop", fun subscription4/0},
        {"publish - deltafn delta", fun subscription5/0},
        {"publish - deliveryfn stop", fun subscription6/0},
        {"publish - message", fun subscription7/0},
        {"publish - message delete", fun subscription8/0},
        {"publish - message publish, persisent sub", fun subscription9/0},
        {"publish - persistent publish, message sub", fun subscription10/0}
       ]
     }
    }.

setup() ->
    ok = meck:new(dby_db), 
    ok = meck:expect(dby_db, exists, 1, true),
    ok = meck:expect(dby_db, write, 1, ok),
    ok = meck:expect(dby_db, delete, 1, ok),
    ok = meck:expect(dby_db, transaction, fun(Fn) -> Fn() end),
    ok = meck:new(dby_publish),
    ok = meck:expect(dby_publish, publish, 3, ok),
    % provide implementations for dby_test_mock used by dby_test_utils
    ok = meck:new(dby_test_mock),
    ok = meck:expect(dby_test_mock, search_fn, search_fn()),
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(delta)),
    ok = meck:expect(dby_test_mock, delivery_fn, delivery_fn(ok)).

cleanup(ok) ->
    ok = meck:unload(dby_db),
    ok = meck:unload(dby_publish),
    ok = meck:unload(dby_test_mock).

each_setup() ->
    ok = meck:reset(dby_db),
    ok = meck:reset(dby_publish),
    ok = meck:reset(dby_test_mock),
    ok = meck:expect(dby_test_mock, search_fn, search_fn()),
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(delta)),
    ok = meck:expect(dby_test_mock, delivery_fn, delivery_fn(ok)).

subscription1() ->
    % create subscription
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example1())),
    {ok, _, _} = dby_subscription:subscribe(search_fn(), [], <<"A">>,
        [{max_depth, 1}, depth, persistent,
        {delta, delta_fn(delta)}, {delivery, delivery_fn(ok)}]),
    {Identifiers, Options} = publish_call(),
    % expected options
    ?assertEqual([system, persistent], Options),
    % subscription identifier
    Subscription = [{SubscriptionId, SubscriptionPL}] =
                                                    identifiers(Identifiers),
    #{
        system := SystemV,
        acc0 := AccV,
        last_result := LastResultV,
        options := OptionsV,
        start_identifier := StartIdentifierV
    } = maps:from_list(SubscriptionPL),
    ?assertEqual(subscription, SystemV),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>], lists:sort(LastResultV)),
    ?assertEqual(<<"A">>, StartIdentifierV),
    ?assertEqual([], AccV),
    ?assertEqual(persistent, OptionsV#options.publish),
    ?assertEqual(depth, OptionsV#options.traversal),
    ?assertEqual(1, OptionsV#options.max_depth),
    % subscription linked to each of the identifiers in the result
    Links = Identifiers -- Subscription,
    {Neighbors, Subs} = lists:unzip([{Neighbor, SId} ||
                {SId, Neighbor, [{system,  subscription}]} <- Links]),
    ?assertEqual(lists:sort(LastResultV), lists:sort(Neighbors)),
    ?assert(lists:all(fun(S) -> S == SubscriptionId end, Subs)).

subscription2() ->
    % no delta
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(nodelta)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub1())),
    ok = dby_subscription:publish(<<"sub">>, persistent, read_fn()),
    ?assertNot(meck:called(dby_test_mock, delta_fn, '_')),
    ?assertNot(meck:called(dby_test_mock, delivery_fn, '_')),
    {Identifiers, _} = publish_call(),
    ?assertEqual([], Identifiers).

subscription3() ->
    % delta in graph, delta fun returns no nodelta
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(nodelta)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub2())),
    ok = dby_subscription:publish(<<"sub">>, persistent, read_fn()),
    ?assert(meck:called(dby_test_mock, delta_fn, '_')),
    ?assertNot(meck:called(dby_test_mock, delivery_fn, '_')),
    {Identifiers, _} = publish_call(),
    % subscription has updated last results
    Subscription = [{SubscriptionId, SubscriptionMetadataPL}] =
                                                    identifiers(Identifiers),
    #{last_result := LastResultV} = maps:from_list(SubscriptionMetadataPL),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>], lists:sort(LastResultV)),
    % link added to identifier "E"
    [Link] = Identifiers -- Subscription,
    ?assertEqual({SubscriptionId, <<"E">>, [{system, subscription}]}, Link).

subscription4() ->
    % delta in graph, delta fun returns stop
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(stop)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub2())),
    ok = dby_subscription:publish(<<"sub">>, persistent, read_fn()),
    ?assert(meck:called(dby_test_mock, delta_fn, '_')),
    ?assertNot(meck:called(dby_test_mock, delivery_fn, '_')),
    {Identifiers, _} = publish_call(),
    [{_, SubscriptionMetadata}] = identifiers(Identifiers),
    ?assertEqual(delete, SubscriptionMetadata).

subscription5() ->
    % delta in graph, delta fun returns {delta, Delta}
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(delta)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub2())),
    ok = dby_subscription:publish(<<"sub">>, persistent, read_fn()),
    Old = [<<"C">>,<<"B">>,<<"A">>],
    New = [<<"E">>,<<"C">>,<<"B">>,<<"A">>],
    ?assert(meck:called(dby_test_mock, delta_fn, [Old, New])),
    ?assert(meck:called(dby_test_mock, delivery_fn, [{Old, New}])),
    {Identifiers, _} = publish_call(),
    % subscription has updated last results
    Subscription = [{SubscriptionId, SubscriptionMetadataPL}] =
                                                    identifiers(Identifiers),
    #{last_result := LastResultV} = maps:from_list(SubscriptionMetadataPL),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>], lists:sort(LastResultV)),
    % link added to identifier "E"
    [Link] = Identifiers -- Subscription,
    ?assertEqual({SubscriptionId, <<"E">>, [{system, subscription}]}, Link).

subscription6() ->
    % delta in graph, delivery fun returns stop
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(delta)),
    ok = meck:expect(dby_test_mock, delivery_fn, delivery_fn(stop)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub2())),
    ok = dby_subscription:publish(<<"sub">>, persistent, read_fn()),
    ?assert(meck:called(dby_test_mock, delta_fn, '_')),
    ?assert(meck:called(dby_test_mock, delivery_fn, '_')),
    {Identifiers, _} = publish_call(),
    [{_, SubscriptionMetadata}] = identifiers(Identifiers),
    ?assertEqual(delete, SubscriptionMetadata).

subscription7() ->
    % no publish on message publish
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(delta)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub3())),
    ok = dby_subscription:publish(<<"sub">>, message, read_fn()),
    Old = [<<"C">>,<<"B">>,<<"A">>],
    New = [<<"E">>,<<"C">>,<<"B">>,<<"A">>],
    ?assert(meck:called(dby_test_mock, delta_fn, [Old, New])),
    ?assert(meck:called(dby_test_mock, delivery_fn, [{Old, New}])),
    ?assert(meck:called(dby_publish, publish, ['_', [], '_'])).

subscription8() ->
    % delta in graph, delivery fun returns stop
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(delta)),
    ok = meck:expect(dby_test_mock, delivery_fn, delivery_fn(stop)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub3())),
    ok = dby_subscription:publish(<<"sub">>, message, read_fn()),
    ?assert(meck:called(dby_test_mock, delta_fn, '_')),
    ?assert(meck:called(dby_test_mock, delivery_fn, '_')),
    {Identifiers, _} = publish_call(),
    [{_, SubscriptionMetadata}] = identifiers(Identifiers),
    ?assertEqual(delete, SubscriptionMetadata).

subscription9() ->
    % no subscription processsing when message publish, persistent subscription
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(delta)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub2())),
    ok = dby_subscription:publish(<<"sub">>, message, read_fn()),
    Old = [<<"C">>,<<"B">>,<<"A">>],
    New = [<<"E">>,<<"C">>,<<"B">>,<<"A">>],
    ?assertEqual(0, meck:num_calls(dby_test_mock, delta_fn, [Old, New])),
    ?assertEqual(0, meck:num_calls(dby_test_mock, delivery_fn, [{Old, New}])),
    ?assert(meck:called(dby_publish, publish, ['_', [], '_'])).

subscription10() ->
    % no subscription processsing when persistent publish, message subscription
    ok = meck:expect(dby_test_mock, delta_fn, delta_fn(delta)),
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub3())),
    ok = dby_subscription:publish(<<"sub">>, persistent, read_fn()),
    Old = [<<"C">>,<<"B">>,<<"A">>],
    New = [<<"E">>,<<"C">>,<<"B">>,<<"A">>],
    ?assertEqual(0, meck:num_calls(dby_test_mock, delta_fn, [Old, New])),
    ?assertEqual(0, meck:num_calls(dby_test_mock, delivery_fn, [{Old, New}])),
    ?assert(meck:called(dby_publish, publish, ['_', [], '_'])).

% ------------------------------------------------------------------------------
% helper functions
% ------------------------------------------------------------------------------

read_fn() ->
    fun(Key) ->
        dby_db:read(Key)
    end.

publish_call() ->
    [{_, {_, publish, [_, Identifiers, Options]}, ok}] =
                                                meck:history(dby_publish),
    {Identifiers, Options}.

search_fn() ->
    fun(Identifier, _, _, Acc0) ->
        {continue, [Identifier | Acc0]}
    end.

delta_fn(stop) ->
    fun(_, _) ->
        stop
    end;
delta_fn(nodelta) ->
    fun(_, _) ->
        nodelta
    end;
delta_fn(delta) ->
    fun(Old, New) ->
        {delta, {Old, New}}
    end.

delivery_fn(ok) ->
    fun(_) ->
        ok
    end;
delivery_fn(stop) ->
    fun(_) ->
        stop
    end.

identifiers(Identifiers) ->
    lists:filter(
        fun({_, _}) -> true;
           (_) -> false
        end, Identifiers).

tr() ->
    dbg:start(),
    dbg:tracer(),
    dbg:p(all, c),
    % dbg:tpl(dby_subscription, [{'_', [], [{return_trace}]}]).
    dbg:tpl(dby_subscription, []).
