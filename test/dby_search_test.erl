-module(dby_search_test).

-include_lib("eunit/include/eunit.hrl").
-include("../include/dobby.hrl").

-define(TRANSACTION, transactionid).

dby_search_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
       fun each_setup/0,
       [
        {"no links", fun search1/0},
        {"links", fun search2/0},
        {"depth first", fun search3/0},
        {"stop", fun search4/0},
        {"skip", fun search5/0}
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
    Metadata1 = #{<<"key1">> => <<"value1">>},
    IdentifierR1 = #identifier{id = Identifier1, metadata = Metadata1, links = #{}},

    % continue
    dby_read(dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby:search(search_fn(continue), Identifier1, [], [depth])),

    % skip
    dby_read(dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby:search(search_fn(skip), Identifier1, [], [depth])),

    % stop
    dby_read(dby_db([IdentifierR1])),
    ?assertEqual([{Identifier1, Metadata1, undefined}],
                dby:search(search_fn(stop), Identifier1, [], [depth])).

search2() ->
    dby_read(dby_db(example1())),
    ?assertEqual([{<<"A">>, id_metadata1(<<"A">>), undefined}],
                        dby:search(search_fn(continue), <<"A">>, [], [depth])),
    ?assertEqual([{<<"A">>, id_metadata1(<<"A">>), undefined}],
        dby:search(search_fn(continue), <<"A">>, [], [depth, {max_depth, 0}])),

    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>],
        lists:sort(dby:search(search_fn2(continue), <<"A">>, [],
                                                    [depth, {max_depth, 1}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby:search(search_fn2(continue), <<"A">>, [],
                                                    [depth, {max_depth, 2}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby:search(search_fn2(continue), <<"A">>, [],
                                                    [depth, {max_depth, 3}]))),
    % start at differnt point
    ?assertEqual([{<<"B">>, id_metadata1(<<"B">>), undefined}],
                        dby:search(search_fn(continue), <<"B">>, [], [depth])),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>],
        lists:sort(dby:search(search_fn2(continue), <<"B">>, [],
                                                    [depth, {max_depth, 2}]))),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>],
        lists:sort(dby:search(search_fn2(continue), <<"B">>, [],
                                                    [depth, {max_depth, 3}]))).

search3() ->
    % depth order search
    dby_read(dby_db(example2())),
    Order = dby:search(search_fn2(continue), <<"A">>, [],
                                                    [depth, {max_depth, 2}]),
    % A is first, D is before C or E is before B.
    [First | _] = Order,
    ?assertEqual(<<"A">>, First),
    ?assert(list_before(<<"D">>, <<"C">>, Order) orelse list_before(<<"E">>, <<"B">>, Order)).

search4() ->
    % stop
    dby_read(dby_db(example1())),
    Order = dby:search(search_fn_stop_on(<<"D">>), <<"A">>, [],
                                                    [depth, {max_depth, 2}]),
    % D is last identifier
    ?assertEqual(<<"D">>, lists:last(Order)).

search5() ->
    % skip
    dby_read(dby_db(example2())),
    ?assertEqual([<<"A">>,<<"B">>,<<"C">>,<<"E">>],
            lists:sort(dby:search(search_fn_skip_on(<<"B">>), <<"A">>, [],
                                                    [depth, {max_depth, 2}]))).

% ------------------------------------------------------------------------------
% helper functions
% ------------------------------------------------------------------------------

dby_read(Items) when is_list(Items) ->
    ok = meck:expect(dby_db, read, 1, meck:seq(Items));
dby_read(Fn) when is_function(Fn) ->
    ok = meck:expect(dby_db, read, Fn).

dby_db(Items) ->
    Dict = lists:foldl(
        fun(IdentifierR, D) ->
            dict:store({identifier, IdentifierR#identifier.id}, IdentifierR, D)
        end, dict:new(), Items),
    fun(Key) ->
        [dict:fetch(Key, Dict)]
    end.

search_fn(Control) ->
    fun(Identifier, Metadata, LinkMetadata, Acc0) ->
        {Control, [{Identifier, Metadata, LinkMetadata} | Acc0]}
    end.

search_fn2(Control) ->
    fun(Identifier, _, _, Acc0) ->
        {Control, [Identifier | Acc0]}
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


%        A
%       /|\
%      B C E
%     /| | |
%    D F G |
%      |___|
example1() ->
    [
        identifier1(<<"A">>, [<<"B">>,<<"C">>,<<"E">>]),
        identifier1(<<"B">>, [<<"A">>,<<"D">>,<<"F">>]),
        identifier1(<<"C">>, [<<"A">>,<<"G">>]),
        identifier1(<<"D">>, [<<"B">>]),
        identifier1(<<"E">>, [<<"A">>,<<"F">>]),
        identifier1(<<"F">>, [<<"B">>,<<"E">>]),
        identifier1(<<"G">>, [<<"C">>])
    ].

%        A
%       / \
%      B   C
%      |   |
%      D   E
example2() ->
    [
        identifier1(<<"A">>,[<<"B">>,<<"C">>]),
        identifier1(<<"B">>,[<<"A">>,<<"D">>]),
        identifier1(<<"C">>,[<<"A">>,<<"E">>]),
        identifier1(<<"D">>,[<<"B">>]),
        identifier1(<<"E">>,[<<"C">>])
    ].

identifier1(Id, Links) ->
    #identifier{id = Id, metadata = id_metadata1(Id), links = links1(Links)}.

id_metadata1(Id) ->
    maps:put(<<"id">>, Id, #{<<"type">> => <<"identifier">>}).

link_metadata1(Id) ->
    maps:put(<<"id">>, Id, #{<<"type">> => <<"link">>}).

links1(Ids) ->
    lists:foldl(
        fun(Id, M) ->
            maps:put(Id, link_metadata1(Id), M)
        end, #{}, Ids).

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
