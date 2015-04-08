-module(dby_search_eqc).

-include_lib("eqc/include/eqc.hrl").

-define(PUBLISHER_ID, <<"qc">>).

-compile(export_all).

gen_id() ->
    ?SUCHTHAT(Id, binary(), Id /= <<>>).

prop_searches() ->
    numtests(1000,
        ?SETUP(
            fun() ->
                start_dobby(),
                fun() -> stop_dobby() end
            end,
            ?FORALL(
                {MaxDepth, Edges},
                {
                    nat(),
                    ?SUCHTHAT(L, list({gen_id(), gen_id()}), length(L) > 0)
                },
                begin
                    cleanup(),
                    {StartingId, _} = select_random(Edges),
                    insert(Edges),
                    Depth = search(depth, MaxDepth, StartingId),
                    Breadth = search(breadth, MaxDepth, StartingId),
                    collect(length(Depth),
                        equals(Depth, Breadth))
                end
            ))).

select_random(L) ->
    lists:nth(random:uniform(length(L)), L).

start_dobby() ->
    application:ensure_all_started(dobby),
    mnesia:wait_for_tables([identifiers], 5000).

stop_dobby() ->
    application:stop(dobby).

cleanup() ->
    dby_db:clear().

insert(Edges) ->
    dby:publish(?PUBLISHER_ID, hairball(Edges), [persistent]).

hairball(Edges) ->
    hairball(Edges, undefined, []).

hairball([], _, Acc) ->
    Acc;
hairball([{Id1, Id2} | Edges], undefined, Acc) ->
    hairball(Edges, Id1, [{Id1, Id2, []} | Acc]);
hairball([{Id1, Id2} | Edges], LastId, Acc) ->
    hairball(Edges, Id1, [{LastId, Id2, []}, {Id1, Id2, []} | Acc]).

search(Traversal, MaxDepth, StartingId) ->
    lists:sort(
        dby:search(search_fn(),
            [], StartingId, [Traversal, {max_depth, MaxDepth}])).

search_fn() ->
    fun(Identifier, _, _, Acc) ->
        {continue, [Identifier | Acc]}
    end.
