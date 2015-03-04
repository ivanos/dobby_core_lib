-module(dby_bulk_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("dobby_clib/include/dobby.hrl").
-include("../src/dobby.hrl").

-define(PUBLISHER_ID, dby_test_utils:publisherid()).
-define(TIMESTAMP, dby_test_utils:timestamp()).

dby_publish_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
       fun each_setup/0,
       [
        {"export/import", fun bulk1/0}
       ]
     }
    }.

setup() ->
    ok = meck:new(dby_db),
    ok = meck:expect(dby_db, transaction, fun(Fn) -> Fn() end),
    ok = meck:new(dby_publish),
    ok = meck:expect(dby_publish, publish, 3, ok).

cleanup(ok) ->
    ok = meck:unload(dby_db),
    ok = meck:unload(dby_publish).

each_setup() ->
    ok = meck:reset(dby_db),
    ok = meck:reset(dby_publish).

bulk1() ->
    Metadata = #{
        <<"true">> => metadatainfo(true),
        <<"false">> => metadatainfo(false),
        <<"null">> => metadatainfo(null),
        <<"integer">> => metadatainfo(1234),
        <<"binary">> => metadatainfo(<<"string">>),
        <<"map">> => metadatainfo(#{<<"string">> => <<"string">>})
    },
    Items = [#identifier{id = <<"Z">>, metadata = Metadata} |
                                                dby_test_utils:example1()],
    dby_foldl(Items),
    ok = dby_bulk:export(json, "test_bulk1.out"),
    ok = dby_bulk:import(json, "test_bulk1.out"),
    meck:called(dby_publish, publish, []).

% ------------------------------------------------------------------------------
% helper functions
% ------------------------------------------------------------------------------

metadatainfo(Value) ->
    #{
        value => Value,
        publisher_id => ?PUBLISHER_ID,
        timestamp => ?TIMESTAMP
    }.
    
dby_foldl(Items) ->
    Fn = fun(Fun, Acc, identifier) ->
        lists:foldl(Fun, Acc, Items)
    end,
    ok = meck:expect(dby_db, foldl, Fn).

tr() ->
    dbg:start(),
    dbg:tracer(),
    dbg:p(all, c),
    dbg:tpl(dby_bulk, []).

