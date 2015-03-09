-module(dby_publish_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("dobby_clib/include/dobby.hrl").
-include("../src/dobby.hrl").

-define(PUBLISHER_ID, dby_test_utils:publisherid()).

dby_publish_test_() ->
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
        {"update identifier metadata", fun publish6/0},
        {"delete identifier, does not exist", fun publish7/0},
        {"delete identifier", fun publish8/0},
        {"delete identifier links", fun publish9/0},
        {"delete link", fun publish10/0},
        {"delete metadata", fun publish11/0},
        {"merge metadata", fun publish12/0},
        {"set pubilsher, timestamp", fun publish13/0},
        {"calls dby_transaction publish", fun publish14/0}
       ]
     }
    }.

setup() ->
    ok = meck:new(dby_db), 
    ok = meck:expect(dby_db, write, 1, ok),
    ok = meck:expect(dby_db, delete, 1, ok),
    ok = meck:expect(dby_db, transaction, fun(Fn) -> Fn() end),
    ok = meck:new(dby_time),
    ok = meck:expect(dby_time, timestamp, 0, dby_test_utils:timestamp()),
    ok = meck:new(dby_transaction),
    ok = meck:expect(dby_transaction, new, 0, transaction_pid),
    ok = meck:expect(dby_transaction, publish, 2, ok),
    ok = meck:expect(dby_transaction, commit, 1, ok),
    ok = meck:expect(dby_transaction, abort, 1, ok).

cleanup(ok) ->
    ok = meck:unload(dby_transaction),
    ok = meck:unload(dby_time),
    ok = meck:unload(dby_db).

each_setup() ->
    ok = meck:reset(dby_db).

publish1() ->
    % new link
    Identifier1 = <<"id1">>,
    Identifier2 = <<"id2">>,
    LinkMetadata = [{<<"key1">>,<<"value1">>}],
    IdentifierR1 = identifier(Identifier1, [], Identifier2, LinkMetadata),
    IdentifierR2 = identifier(Identifier2, [], Identifier1, LinkMetadata),
    dby_test_utils:dby_read([[],[]]),
    ok = dby_publish:publish(?PUBLISHER_ID, [{Identifier1, Identifier2, LinkMetadata}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1])),
    ?assert(meck:called(dby_db, write, [IdentifierR2])),
    ?assert(meck:called(dby_transaction, new, [])),
    ?assert(meck:called(dby_transaction, commit, [transaction_pid])).

publish2() ->
    % new link with identifier metadata
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}],
    Identifier2 = <<"id2">>,
    Identifier2Metadata = [{<<"key_id2">>, <<"value_id2">>}],
    LinkMetadata = [{<<"key1">>,<<"value1">>}],
    IdentifierR1 = identifier(Identifier1, Identifier1Metadata, Identifier2, LinkMetadata),
    IdentifierR2 = identifier(Identifier2, Identifier2Metadata, Identifier1, LinkMetadata),
    dby_test_utils:dby_read([[],[]]),
    ok = dby_publish:publish(?PUBLISHER_ID, [{{Identifier1, Identifier1Metadata}, {Identifier2, Identifier2Metadata}, LinkMetadata}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1])),
    ?assert(meck:called(dby_db, write, [IdentifierR2])).

publish3() ->
    % update link metadata
    Identifier1 = <<"id1">>,
    Identifier2 = <<"id2">>,
    LinkMetadata = [{<<"key1">>,<<"value1">>}],
    NewLinkMetadata = [{<<"key1">>,<<"value2">>}],
    IdentifierR1 = identifier(Identifier1, [], Identifier2, LinkMetadata),
    IdentifierR2 = identifier(Identifier2, [], Identifier1, LinkMetadata),
    dby_test_utils:dby_read([[IdentifierR1],[IdentifierR2]]),
    ok = dby_publish:publish(?PUBLISHER_ID, [{Identifier1, Identifier2, NewLinkMetadata}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{links = maps:put(Identifier2, metadatainfo(NewLinkMetadata), #{})}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{links = maps:put(Identifier1, metadatainfo(NewLinkMetadata), #{})}])).

publish4() ->
    % update link metadata
    Identifier1 = <<"id1">>,
    Identifier2 = <<"id2">>,
    LinkMetadata = [{<<"key1">>,<<"value1">>}],
    DeltaFn = fun(M) -> [{<<"key2">>, <<"valueX">>} | M] end,
    NewLinkMetadata = DeltaFn(LinkMetadata),
    IdentifierR1 = identifier(Identifier1, [], Identifier2, LinkMetadata),
    IdentifierR2 = identifier(Identifier2, [], Identifier1, LinkMetadata),
    dby_test_utils:dby_read([[IdentifierR1],[IdentifierR2]]),
    ok = dby_publish:publish(?PUBLISHER_ID, [{Identifier1, Identifier2, DeltaFn}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{links = maps:put(Identifier2, metadatainfo(NewLinkMetadata), #{})}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{links = maps:put(Identifier1, metadatainfo(NewLinkMetadata), #{})}])).

publish5() ->
    % update identifier metadata
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}],
    NewIdentifier1Metadata = [{<<"key_id1">>, <<"value_id1new">>}],
    Identifier2 = <<"id2">>,
    Identifier2Metadata = [{<<"key_id2">>, <<"value_id2">>}],
    NewIdentifier2Metadata = [{<<"key_id2">>, <<"value_id2new">>}],
    LinkMetadata = [{<<"key1">>, <<"value1">>}],
    IdentifierR1 = identifier(Identifier1, Identifier1Metadata, Identifier2, LinkMetadata),
    IdentifierR2 = identifier(Identifier2, Identifier2Metadata, Identifier1, LinkMetadata),
    dby_test_utils:dby_read([[IdentifierR1],[IdentifierR2]]),
    ok = dby_publish:publish(?PUBLISHER_ID, [{{Identifier1, NewIdentifier1Metadata}, {Identifier2, NewIdentifier2Metadata}, LinkMetadata}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{metadata = metadatainfo(NewIdentifier1Metadata)}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{metadata = metadatainfo(NewIdentifier2Metadata)}])).

publish6() ->
    % update identifer metadata with function
    DeltaFn = fun(M) -> [{<<"key2">>, <<"valueX">>} |  M] end,
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}],
    NewIdentifier1Metadata = DeltaFn(Identifier1Metadata),
    Identifier2 = <<"id2">>,
    Identifier2Metadata = [{<<"key_id2">>, <<"value_id2">>}],
    NewIdentifier2Metadata = DeltaFn(Identifier2Metadata),
    LinkMetadata = [{<<"key1">>, <<"value1">>}],
    IdentifierR1 = identifier(Identifier1, Identifier1Metadata, Identifier2, LinkMetadata),
    IdentifierR2 = identifier(Identifier2, Identifier2Metadata, Identifier1, LinkMetadata),
    dby_test_utils:dby_read([[IdentifierR1],[IdentifierR2]]),
    ok = dby_publish:publish(?PUBLISHER_ID, [{{Identifier1, DeltaFn}, {Identifier2, DeltaFn}, LinkMetadata}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{metadata = metadatainfo(NewIdentifier1Metadata)}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{metadata = metadatainfo(NewIdentifier2Metadata)}])).

publish7() ->
    % delete identifier that's not in graph
    Identifier1 = <<"id1">>,
    dby_test_utils:dby_read([[]]),
    dby_publish:publish(?PUBLISHER_ID, [{Identifier1, delete}], [persistent]),
    ?assert(meck:called(dby_db, delete, [{identifier, Identifier1}])).

publish8() ->
    % delete identifier that is in graph
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}],
    IdentifierR1 = identifier(Identifier1, Identifier1Metadata, []),
    dby_test_utils:dby_read([[IdentifierR1]]),
    dby_publish:publish(?PUBLISHER_ID, [{Identifier1, delete}], [persistent]),
    ?assert(meck:called(dby_db, delete, [{identifier, Identifier1}])).

publish9() ->
    % delete identifier that is in graph and has links
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}],
    Identifier2 = <<"id2">>,
    Identifier2Metadata = [{<<"key_id2">>, <<"value_id2">>}],
    LinkMetadata = [{<<"key1">>, <<"value1">>}],
    IdentifierR1 = identifier(Identifier1, Identifier1Metadata, Identifier2, LinkMetadata),
    IdentifierR2 = identifier(Identifier2, Identifier2Metadata, Identifier1, LinkMetadata),
    dby_test_utils:dby_read([[IdentifierR1],[IdentifierR2]]),
    dby_publish:publish(?PUBLISHER_ID, [{Identifier1, delete}], [persistent]),
    ?assert(meck:called(dby_db, delete, [{identifier, Identifier1}])),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{links = #{}}])).

publish10() ->
    % delete link
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}],
    Identifier2 = <<"id2">>,
    Identifier2Metadata = [{<<"key_id2">>, <<"value_id2">>}],
    LinkMetadata = [{<<"key1">>, <<"value1">>}],
    IdentifierR1 = identifier(Identifier1, Identifier1Metadata, Identifier2, LinkMetadata),
    IdentifierR2 = identifier(Identifier2, Identifier2Metadata, Identifier1, LinkMetadata),
    dby_test_utils:dby_read([[IdentifierR1],[IdentifierR2]]),
    dby_publish:publish(?PUBLISHER_ID, [{Identifier1, Identifier2, delete}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR2#identifier{links = #{}}])),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{links = #{}}])).

publish11() ->
    % delete metadata
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}, {<<"todelete">>, <<"value to delete">>}],
    AfterDeleteIdentifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}],
    IdentifierR1 = identifier(Identifier1, Identifier1Metadata, []),
    dby_test_utils:dby_read([[IdentifierR1]]),
    dby_publish:publish(?PUBLISHER_ID, [{Identifier1, [{<<"todelete">>, delete}]}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{metadata = metadatainfo(AfterDeleteIdentifier1Metadata)}])).

publish12() ->
    % merge metadata
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}],
    AddedMetadata = {<<"newkey">>, <<"value added">>},
    AfterMergeIdentifier1Metadata = [{<<"key_id1">>, <<"value_id1">>}, AddedMetadata],
    IdentifierR1 = identifier(Identifier1, Identifier1Metadata, []),
    dby_test_utils:dby_read([[IdentifierR1]]),
    dby_publish:publish(?PUBLISHER_ID, [{Identifier1, [AddedMetadata]}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{metadata = metadatainfo(AfterMergeIdentifier1Metadata)}])).

publish13() ->
    % merge metadata
    Identifier1 = <<"id1">>,
    Identifier1Metadata = [{<<"key_id1">>, {<<"value_id1">>, <<"setpubid">>, <<"setts">>}}],
    IdentifierR1 = identifier(Identifier1, [], []),
    dby_test_utils:dby_read([[IdentifierR1]]),
    dby_publish:publish(?PUBLISHER_ID,
                        [{Identifier1, Identifier1Metadata}], [persistent]),
    ?assert(meck:called(dby_db, write, [IdentifierR1#identifier{metadata =
        #{
            <<"key_id1">> => #{
                publisher_id => <<"setpubid">>,
                timestamp => <<"setts">>,
                value => <<"value_id1">>
            }
        }
    }])).

publish14() ->
    % with subscription, calls publish
    dby_test_utils:dby_read(
                        dby_test_utils:dby_db(dby_test_utils:example_sub1())),
    dby_publish:publish(?PUBLISHER_ID,
                [{<<"A">>, [{<<"newmdata">>, <<"data">>}]}], [persistent]),
    ?assert(
        meck:called(dby_transaction, publish, [transaction_pid, <<"sub">>])).

% ------------------------------------------------------------------------------
% helper functions
% ------------------------------------------------------------------------------

metadatainfo(MetadataPL) ->
    dby_test_utils:format_metadata(MetadataPL).

identifier(Id, Metadata, Neighbor, LinkMetadata) ->
    identifier(Id, Metadata, [{Neighbor, LinkMetadata}]).

identifier(Id, Metadata, Links) ->
    #identifier{id = Id,
                metadata = metadatainfo(Metadata),
                links = lists:foldl(
                    fun({Neighbor, LinkMetadata}, Acc) ->
                        maps:put(Neighbor, metadatainfo(LinkMetadata), Acc)
                    end, #{}, Links)}.
