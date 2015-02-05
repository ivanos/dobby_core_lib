-module(dby_publish_test).

-include_lib("eunit/include/eunit.hrl").
-include("../include/dobby.hrl").

-define(TRANSACTION, transactionid).

dby_transaction_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
       fun each_setup/0,
       [
        {"new link", fun publish1/0},
        {"new link identifier metadata", fun publish2/0},
        {"replace link metadata", fun publish3/0},
        {"update link metadata", fun publish4/0},
        {"replace identifier metadata", fun publish5/0},
        {"update identifier metadata", fun publish6/0}
       ]
     }
    }.

setup() ->
    ok = meck:new(dby_db),
    ok = meck:expect(dby_db, write, 1, ok).

cleanup(ok) ->
    ok = meck:unload(dby_db).

each_setup() ->
    ok = meck:reset(dby_db).

publish1() ->
    % new link
    Identifier1 = <<"id1">>,
    Identifier2 = <<"id2">>,
    LinkMetadata = #{<<"key1">> => <<"value1">>},
    IdentifierR1 = #identifier{id = Identifier1, metadata = null, links = maps:put(Identifier2, LinkMetadata, #{})},
    IdentifierR2 = #identifier{id = Identifier2, metadata = null, links = maps:put(Identifier1, LinkMetadata, #{})},
    dby_read([[],[]]),
    ok = dby_publish:publish(Identifier1, Identifier2, LinkMetadata, [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1])),
    ?assert(meck:called(dby_db, write, [IdentifierR2])).

publish2() ->
    % new link with identifier metadata
    Identifier1 = <<"id1">>,
    Identifier1Metadata = #{<<"key_id1">> => <<"value_id1">>},
    Identifier2 = <<"id2">>,
    Identifier2Metadata = #{<<"key_id2">> => <<"value_id2">>},
    LinkMetadata = #{<<"key1">> => <<"value1">>},
    IdentifierR1 = #identifier{id = Identifier1, metadata = Identifier1Metadata, links = maps:put(Identifier2, LinkMetadata, #{})},
    IdentifierR2 = #identifier{id = Identifier2, metadata = Identifier2Metadata, links = maps:put(Identifier1, LinkMetadata, #{})},
    dby_read([[],[]]),
    ok = dby_publish:publish({Identifier1, Identifier1Metadata}, {Identifier2, Identifier2Metadata}, LinkMetadata, [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1])),
    ?assert(meck:called(dby_db, write, [IdentifierR2])).

publish3() ->
    % update link metadata
    Identifier1 = <<"id1">>,
    Identifier2 = <<"id2">>,
    LinkMetadata = #{<<"key1">> => <<"value1">>},
    NewLinkMetadata = #{<<"key1">> => <<"value2">>},
    IdentifierR1 = #identifier{id = Identifier1, metadata = null, links = maps:put(Identifier2, LinkMetadata, #{})},
    IdentifierR2 = #identifier{id = Identifier2, metadata = null, links = maps:put(Identifier1, LinkMetadata, #{})},
    dby_read([[IdentifierR1],[IdentifierR2]]),
    ok = dby_publish:publish(Identifier1, Identifier2, NewLinkMetadata, [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{links = maps:put(Identifier2, NewLinkMetadata, #{})}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{links = maps:put(Identifier1, NewLinkMetadata, #{})}])).

publish4() ->
    % update link metadata
    Identifier1 = <<"id1">>,
    Identifier2 = <<"id2">>,
    LinkMetadata = #{<<"key1">> => <<"value1">>},
    DeltaFn = fun(M) -> maps:put(<<"key2">>, <<"valueX">>, M) end,
    NewLinkMetadata = DeltaFn(LinkMetadata),
    IdentifierR1 = #identifier{id = Identifier1, metadata = null, links = maps:put(Identifier2, LinkMetadata, #{})},
    IdentifierR2 = #identifier{id = Identifier2, metadata = null, links = maps:put(Identifier1, LinkMetadata, #{})},
    dby_read([[IdentifierR1],[IdentifierR2]]),
    ok = dby_publish:publish(Identifier1, Identifier2, DeltaFn, [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{links = maps:put(Identifier2, NewLinkMetadata, #{})}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{links = maps:put(Identifier1, NewLinkMetadata, #{})}])).

publish5() ->
    % new link with identifier metadata
    Identifier1 = <<"id1">>,
    Identifier1Metadata = #{<<"key_id1">> => <<"value_id1">>},
    NewIdentifier1Metadata = #{<<"key_id1">> => <<"value_id1new">>},
    Identifier2 = <<"id2">>,
    Identifier2Metadata = #{<<"key_id2">> => <<"value_id2">>},
    NewIdentifier2Metadata = #{<<"key_id2">> => <<"value_id2new">>},
    LinkMetadata = #{<<"key1">> => <<"value1">>},
    IdentifierR1 = #identifier{id = Identifier1, metadata = Identifier1Metadata, links = maps:put(Identifier2, LinkMetadata, #{})},
    IdentifierR2 = #identifier{id = Identifier2, metadata = Identifier2Metadata, links = maps:put(Identifier1, LinkMetadata, #{})},
    dby_read([[IdentifierR1],[IdentifierR2]]),
    ok = dby_publish:publish({Identifier1, NewIdentifier1Metadata}, {Identifier2, NewIdentifier2Metadata}, LinkMetadata, [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{metadata = NewIdentifier1Metadata}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{metadata = NewIdentifier2Metadata}])).

publish6() ->
    % new link with identifier metadata
    DeltaFn = fun(M) -> maps:put(<<"key2">>, <<"valueX">>, M) end,
    Identifier1 = <<"id1">>,
    Identifier1Metadata = #{<<"key_id1">> => <<"value_id1">>},
    NewIdentifier1Metadata = DeltaFn(Identifier1Metadata),
    Identifier2 = <<"id2">>,
    Identifier2Metadata = #{<<"key_id2">> => <<"value_id2">>},
    NewIdentifier2Metadata = DeltaFn(Identifier2Metadata),
    LinkMetadata = #{<<"key1">> => <<"value1">>},
    IdentifierR1 = #identifier{id = Identifier1, metadata = Identifier1Metadata, links = maps:put(Identifier2, LinkMetadata, #{})},
    IdentifierR2 = #identifier{id = Identifier2, metadata = Identifier2Metadata, links = maps:put(Identifier1, LinkMetadata, #{})},
    dby_read([[IdentifierR1],[IdentifierR2]]),
    ok = dby_publish:publish({Identifier1, DeltaFn}, {Identifier2, DeltaFn}, LinkMetadata, [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{metadata = NewIdentifier1Metadata}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{metadata = NewIdentifier2Metadata}])).

% ------------------------------------------------------------------------------
% helper functions
% ------------------------------------------------------------------------------

dby_read(Items) ->
    ok = meck:expect(dby_db, read, 1, meck:seq(Items)).
