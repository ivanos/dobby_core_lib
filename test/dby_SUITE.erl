%%%=============================================================================
%%% @copyright (C) 2015, Erlang Solutions Ltd
%%% @author Marc Sugiyama <marc.sugiyama@erlang-solutions.com>
%%% @doc Dobby system test
%%% @end
%%%=============================================================================
-module(dby_SUITE).
-copyright("2015, Erlang Solutions Ltd.").

%% Note: This directive should only be used in test suites.
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PUBLISHER_ID, <<"PUBLISHER">>).

%%%=============================================================================
%%% Callbacks
%%%=============================================================================


suite() ->
    [{timetrap,{minutes,10}}].

init_per_suite(Config) ->
    start_applications(),
    case is_dobby_server_running() of
        false ->
            ct:pal(Reason = "Dobby server is not running"),
            {skip, Reason};
        true ->
            Config
    end.

end_per_testcase(_,_) ->
    % clean up graph
    Identifiers = dby:search(search_fn1(), [], <<"A">>, [depth, {max_depth, 100}]),
    ok = dby:publish(?PUBLISHER_ID,
        [{Identifier, delete} || Identifier <- Identifiers], [persistent]).

all() ->
    [search1,
     delete1,
     subscription1,
     subscription2].

%%%=============================================================================
%%% Testcases
%%%=============================================================================

search1(_Config) ->
    %% GIVEN
    Identifiers = identifiers(),
    Graph = graph(),

    %% WHEN
    ok = dby:publish(?PUBLISHER_ID, Graph, [persistent]),

    %% THEN
    ?assertEqual(
        Identifiers,
        lists:sort(
            dby:search(search_fn1(), [], <<"A">>, [depth, {max_depth, 10}]))),
    ?assertEqual(
        [<<"A">>,<<"B">>,<<"C">>,<<"E">>],
        lists:sort(
            dby:search(search_fn1(), [], <<"A">>, [depth, {max_depth, 1}]))),
    ?assertEqual(
        [<<"A">>,<<"B">>,<<"C">>,<<"E">>],
        lists:sort(
            dby:search(search_fn1(), [], <<"A">>, [breadth, {max_depth, 1}]))).

-define(KEY, <<"when">>).
delete1(_Config) ->
    %% GIFEN
    Identifier = <<"A">>,
    AfterMeta = <<"after delete">>,
    BeforeIdentifier = {Identifier, [{?KEY, <<"before delete">>}]},
    AfterIdentifier = {Identifier, [{?KEY, AfterMeta}]},
    ok = dby:publish(?PUBLISHER_ID, [BeforeIdentifier], [persistent]),

    %% WHEN
    ok = dby:publish(?PUBLISHER_ID, [{Identifier, delete},
                                     AfterIdentifier], [persistent]),

    %% THEN
    ?assertMatch(
        #{?KEY := #{value := AfterMeta}},
        dby:identifier(Identifier)).

subscription1(_Config) ->
    %% GIVEN
    Graph = graph(),
    Ref = make_ref(),
    ok = dby:publish(?PUBLISHER_ID, Graph, [persistent]),
    {ok, _, _} = dby:subscribe(search_fn1(), [], <<"A">>,
        [depth, {max_depth, 10}, persistent,
            {delta, delta_fn1()}, {delivery, delivery_fn1(Ref)}]),

    %% WHEN
    ok = dby:publish(?PUBLISHER_ID, [{<<"A">>, <<"Q">>, []}], [persistent]),

    %% THEN
    receive
        {Ref, Msg} ->
            ?assertEqual([<<"Q">>], Msg)
    end.

subscription2(_Config) ->
    %% GIVEN
    Identifiers = identifiers(),
    Graph = graph(),
    Ref = make_ref(),
    ok = dby:publish(?PUBLISHER_ID, Graph, [persistent]),
    {ok, _, _} = dby:subscribe(search_fn1(), [], <<"A">>,
        [depth, {max_depth, 10}, message,
            {delta, delta_fn1()}, {delivery, delivery_fn1(Ref)}]),

    %% WHEN
    ok = dby:publish(?PUBLISHER_ID, [{<<"A">>, <<"Q">>, []}], [message]),

    %% THEN
    receive
        {Ref, Msg} ->
            ?assertEqual([<<"Q">>], Msg)
    end,
    ?assertEqual(
        Identifiers,
        lists:sort(
            dby:search(search_fn1(), [], <<"A">>, [depth, {max_depth, 10}]))).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

start_applications() ->
    ok = application:set_env(erl_mnesia, options, [persistent]),
    application:ensure_all_started(dobby).

is_dobby_server_running() ->
    proplists:is_defined(dobby, application:which_applications()).

% search function returns list of identifiers
search_fn1() ->
    fun(Identifier, _, _, Acc) ->
        {continue, [Identifier | Acc]}
    end.

% delta function returns what's new in the list
delta_fn1() ->
    fun(OldAcc, NewAcc) ->
        {delta, NewAcc -- OldAcc}
    end.

% delivery function sends the message to the pid: {Ref, Delta}
delivery_fn1(Ref) ->
    Target = self(),
    fun(Delta) ->
        Target ! {Ref, Delta}
    end.

identifiers() ->
    [<<"A">>,<<"B">>,<<"C">>,<<"D">>,<<"E">>,<<"F">>,<<"G">>].

graph() ->
    [
        {<<"A">>,<<"B">>, []},
        {<<"A">>,<<"C">>, []},
        {<<"A">>,<<"E">>, []},
        {<<"B">>,<<"D">>, []},
        {<<"B">>,<<"F">>, []},
        {<<"C">>,<<"G">>, []},
        {<<"E">>,<<"F">>, []}
    ].
