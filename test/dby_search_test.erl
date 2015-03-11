-module(dby_search_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("dobby_clib/include/dobby.hrl").
-include("../src/dobby.hrl").

-define(TRANSACTION, transactionid).

dby_search_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
       fun each_setup/0,
       [
        {"depth no links", fun search1/0},
        {"depth links", fun search2/0},
        {"depth first", fun search3/0},
        {"depth stop", fun search4/0},
        {"depth skip", fun search5/0},
        {"breadth no links", fun search6/0},
        {"breadth links", fun search7/0},
        {"breadth first", fun search8/0},
        {"breadth stop", fun search9/0},
        {"breadth skip", fun search10/0},
        {"no loop", fun search11/0},
        {"link loop", fun search12/0},
        {"system", fun search13/0},
        {"path", fun search14/0},
        {"depth loop, link check", fun search15/0}
       ]
     }
    }.

setup() ->
    ok = meck:new(dby_db),
    ok = meck:expect(dby_db, write, 1, ok),
    ok = meck:expect(dby_db, delete, 1, ok),
    ok = meck:expect(dby_db, transaction, fun(Fn) -> Fn() end).

cleanup(ok) ->
    ok = meck:unload(dby_db).

each_setup() ->
    ok = meck:reset(dby_db).

search1() ->
    % no links
    Identifier1 = <<"id1">>,
    Metadata1 = dby_test_utils:metadatainfo([{<<"key1">>, <<"value1">>}]),
    IdentifierR1 = #identifier{id = Identifier1, metadata = Metadata1, links = #{}},

    % continue
    dby_test_utils:dby_read(dby_test_utils:dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby_search:search(search_fn(continue), [], Identifier1, [depth])),

    % skip
    dby_test_utils:dby_read(dby_test_utils:dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby_search:search(search_fn(skip), [], Identifier1, [depth])),

    % stop
    dby_test_utils:dby_read(dby_test_utils:dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby_search:search(search_fn(stop), [], Identifier1, [depth])).

search2() ->
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example1())),
    ?assertEqual([{<<"A">>, dby_test_utils:id_metadata1(<<"A">>), undefined}],
                        dby_search:search(search_fn(continue), [], <<"A">>, [depth])),
    ?assertEqual([{<<"A">>, dby_test_utils:id_metadata1(<<"A">>), undefined}],
        dby_search:search(search_fn(continue), [], <<"A">>, [depth, {max_depth, 0}])),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                                    [depth, {max_depth, 1}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                                    [depth, {max_depth, 2}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                                    [depth, {max_depth, 3}]))),
    % start at differnt point
    ?assertEqual([{<<"B">>, dby_test_utils:id_metadata1(<<"B">>), undefined}],
                        dby_search:search(search_fn(continue), [], <<"B">>, [depth])),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"B">>,
                                                    [depth, {max_depth, 2}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"B">>,
                                                    [depth, {max_depth, 3}]))).

search3() ->
    % depth order search
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example2())),
    Order = lists:reverse(dby_search:search(search_fn2(continue), [], <<"A">>,
                                                    [depth, {max_depth, 2}])),
    % A is first, D is before C or E is before B.
    [First | _] = Order,
    ?assertEqual(<<"A">>, First),
    ?assert(list_before(<<"D">>, <<"C">>, Order) orelse list_before(<<"E">>, <<"B">>, Order)).

search4() ->
    % stop
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example1())),
    Order = lists:reverse(dby_search:search(search_fn_stop_on(<<"D">>),
                                        [], <<"A">>, [depth, {max_depth, 2}])),
    % D is last identifier
    ?assertEqual(<<"D">>, lists:last(Order)).

search5() ->
    % skip
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example2())),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>],
            lists:sort(dby_search:search(search_fn_skip_on(<<"B">>), [], <<"A">>,
                                                    [depth, {max_depth, 2}]))).

search6() ->
    % no links
    Identifier1 = <<"id1">>,
    Metadata1 = dby_test_utils:metadatainfo([{<<"key1">>, <<"value1">>}]),
    IdentifierR1 = #identifier{id = Identifier1, metadata = Metadata1, links = #{}},

    % continue
    dby_test_utils:dby_read(dby_test_utils:dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby_search:search(search_fn(continue), [], Identifier1, [breadth])),

    % skip
    dby_test_utils:dby_read(dby_test_utils:dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby_search:search(search_fn(skip), [], Identifier1, [breadth])),

    % stop
    dby_test_utils:dby_read(dby_test_utils:dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby_search:search(search_fn(stop), [], Identifier1, [breadth])).

search7() ->
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example1())),
    ?assertEqual([{<<"A">>, dby_test_utils:id_metadata1(<<"A">>), undefined}],
                    dby_search:search(search_fn(continue), [], <<"A">>, [breadth])),
    ?assertEqual([{<<"A">>, dby_test_utils:id_metadata1(<<"A">>), undefined}],
        dby_search:search(search_fn(continue), [], <<"A">>,
                                                [breadth, {max_depth, 0}])),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                                [breadth, {max_depth, 1}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                                [breadth, {max_depth, 2}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                                [breadth, {max_depth, 3}]))),
    % start at differnt point
    ?assertEqual([{<<"B">>, dby_test_utils:id_metadata1(<<"B">>), undefined}],
                        dby_search:search(search_fn(continue), [], <<"B">>, [breadth])),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"B">>,
                                                [depth, {max_depth, 2}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"B">>,
                                                [breadth, {max_depth, 3}]))).

search8() ->
    % breadth order search
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example2())),
    Order = lists:reverse(dby_search:search(search_fn2(continue), [], <<"A">>,
                                                    [breadth, {max_depth, 2}])),
    % A is first, B and C are before D and E
    [First | _] = Order,
    ?assertEqual(<<"A">>, First),
    ?assert(list_before(<<"B">>, <<"D">>, Order) andalso list_before(<<"B">>, <<"E">>, Order) andalso list_before(<<"C">>, <<"D">>, Order) andalso list_before(<<"C">>,<<"E">>, Order)).

search9() ->
    % stop
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example1())),
    Order = lists:reverse(dby_search:search(search_fn_stop_on(<<"C">>),
                                    [], <<"A">>, [breadth, {max_depth, 2}])),
    % C is last identifier
    ?assertEqual(<<"C">>, lists:last(Order)).

search10() ->
    % skip
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example2())),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>],
            lists:sort(dby_search:search(search_fn_skip_on(<<"B">>), [], <<"A">>,
                                                [breadth, {max_depth, 2}]))).

search11() ->
    % no loop detection
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example3())),
    ?assertEqual([<<"A">>,<<"B">>,<<"A">>,<<"A">>,<<"B">>,<<"A">>],
            lists:reverse(dby_search:search(search_fn2(continue), [], <<"A">>,
                                    [depth, {max_depth, 2}, {loop, none}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"A">>],
            lists:reverse(dby_search:search(search_fn2(continue), [], <<"A">>,
                                    [breadth, {max_depth, 2}, {loop, none}]))).

search12() ->
    % link loop detection
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example2())),
    ?assertEqual([{<<"A">>, dby_test_utils:id_metadata1(<<"A">>), undefined}],
                        dby_search:search(search_fn(continue), [], <<"A">>, [depth, {loop, link}])),
    ?assertEqual([{<<"A">>, dby_test_utils:id_metadata1(<<"A">>), undefined}],
        dby_search:search(search_fn(continue), [], <<"A">>, [depth, {max_depth, 0}, {loop, link}])),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                    [depth, {max_depth, 1}, {loop, link}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                    [depth, {max_depth, 2}, {loop, link}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                    [depth, {max_depth, 3}, {loop, link}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                    [breadth, {max_depth, 3}, {loop, link}]))),
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example1())),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"F">>,<<"G">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                    [breadth, {max_depth, 3}, {loop, link}]))).

search13() ->
    % system search
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub1())),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                        [user, depth, {max_depth, 1}]))),
    ?assertEqual([<<"A">>,<<"sub">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                        [system, depth, {max_depth, 1}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                        [user, breadth, {max_depth, 1}]))),
    ?assertEqual([<<"A">>,<<"sub">>],
        lists:sort(dby_search:search(search_fn2(continue), [], <<"A">>,
                                        [system, breadth, {max_depth, 1}]))).

search14() ->
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example2())),
    ?assertEqual([
        {<<"A">>, []},
        {<<"B">>, [<<"A">>]},
        {<<"C">>, [<<"A">>]},
        {<<"D">>, [<<"B">>,<<"A">>]},
        {<<"E">>, [<<"C">>,<<"A">>]}
        ],
        lists:sort(dby_search:search(search_fn3(), [], <<"A">>,
                                        [depth, {max_depth, 2}]))),
    ?assertEqual([
        {<<"A">>, []},
        {<<"B">>, [<<"A">>]},
        {<<"C">>, [<<"A">>]},
        {<<"D">>, [<<"B">>,<<"A">>]},
        {<<"E">>, [<<"C">>,<<"A">>]}
        ],
        lists:sort(dby_search:search(search_fn3(), [], <<"A">>,
                                        [breadth, {max_depth, 2}]))).

search15() ->
    % link loop detection
    dby_test_utils:dby_read(dby_test_utils:dby_db(dby_test_utils:example4())),
    ?assertEqual([<<"A">>,<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>],
                    lists:sort(
                        dby_search:search(search_fn2(continue),
                            [], <<"A">>,
                            [depth, {max_depth, 10}, {loop, link}]))).

% ------------------------------------------------------------------------------
% helper functions
% ------------------------------------------------------------------------------

search_fn(Control) ->
    fun(Identifier, Metadata, [], Acc0) ->
        {Control, [{Identifier, Metadata, undefined} | Acc0]};
       (Identifier, Metadata, [{_, _, LinkMetadata} | _], Acc0) ->
        {Control, [{Identifier, Metadata, LinkMetadata} | Acc0]}
    end.

search_fn2(Control) ->
    fun(Identifier, _, _, Acc) ->
        {Control, [Identifier | Acc]}
    end.

search_fn3() ->
    fun(Identifier, _, Path, Acc) ->
        {continue, [{Identifier, [Id || {Id, _, _} <- Path]} | Acc]}
    end.

search_fn_stop_on(StopOnIdentifier) ->
    search_fn_control_on(stop, StopOnIdentifier).

search_fn_skip_on(SkipOnIdentifier) ->
    search_fn_control_on(skip, SkipOnIdentifier).

search_fn_control_on(Control, OnIdentifier) ->
    fun(Identifier, _, _, Acc0) ->
        C = case OnIdentifier =:= Identifier of
            true ->
                Control;
            false ->
                continue
        end,
        {C, [Identifier | Acc0]}
    end.

list_before(X, Y, List) ->
    XY = lists:filter(
            fun(E) ->
                E == X orelse E == Y
            end, List),
    XY == [X,Y].

tr() ->
    dbg:start(),
    dbg:tracer(),
    dbg:p(all, c),
    % dbg:tpl(dby_search, [{'_', [], [{return_trace}]}]).
    dbg:tpl(dby_search, []).
