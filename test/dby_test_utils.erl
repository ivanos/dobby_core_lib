-module(dby_test_utils).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("dobby_clib/include/dobby.hrl").
-include("../src/dobby.hrl").

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
%       /|\
%      B C E
%     /| | |
%    D F G |
%      |___|
%
% persistent subscription that is linked to A, B, C, E
example_sub1() ->
    [
        identifier1(<<"A">>, [{sub, <<"sub">>},<<"B">>,<<"C">>,<<"E">>]),
        identifier1(<<"B">>, [{sub, <<"sub">>},<<"A">>,<<"D">>,<<"F">>]),
        identifier1(<<"C">>, [{sub, <<"sub">>},<<"A">>,<<"G">>]),
        identifier1(<<"D">>, [<<"B">>]),
        identifier1(<<"E">>, [{sub, <<"sub">>},<<"A">>,<<"F">>]),
        identifier1(<<"F">>, [<<"B">>,<<"E">>]),
        identifier1(<<"G">>, [<<"C">>]),
        % last result order may change depending on dby_subscription or
        % dby_search implementation
        subscription1(<<"sub">>, [<<"E">>,<<"C">>,<<"B">>,<<"A">>],
                [{sub, <<"A">>},{sub, <<"B">>},{sub, <<"C">>},{sub, <<"E">>}])
    ].

%        A
%       /|\
%      B C E
%     /| | |
%    D F G |
%      |___|
%
% subscription that is linked to A, B, C
% simulates a change in the graph that causes a delta.  E is added to the
% sesarch result.  persistent subscription.
example_sub2() ->
    [
        identifier1(<<"A">>, [{sub, <<"sub">>},<<"B">>,<<"C">>,<<"E">>]),
        identifier1(<<"B">>, [{sub, <<"sub">>},<<"A">>,<<"D">>,<<"F">>]),
        identifier1(<<"C">>, [{sub, <<"sub">>},<<"A">>,<<"G">>]),
        identifier1(<<"D">>, [<<"B">>]),
        identifier1(<<"E">>, [<<"A">>,<<"F">>]),
        identifier1(<<"F">>, [<<"B">>,<<"E">>]),
        identifier1(<<"G">>, [<<"C">>]),
        % last result order may change depending on dby_subscription or
        % dby_search implementation
        subscription1(<<"sub">>, [<<"C">>,<<"B">>,<<"A">>],
                [{sub, <<"A">>},{sub, <<"B">>},{sub, <<"C">>}])
    ].

%        A
%       /|\
%      B C E
%     /| | |
%    D F G |
%      |___|
%
% subscription that is linked to A, B, C
% simulates a change in the graph that causes a delta.  E is added to the
% sesarch result.  message subscription.
example_sub3() ->
    [
        identifier1(<<"A">>, [{sub, <<"sub">>},<<"B">>,<<"C">>,<<"E">>]),
        identifier1(<<"B">>, [{sub, <<"sub">>},<<"A">>,<<"D">>,<<"F">>]),
        identifier1(<<"C">>, [{sub, <<"sub">>},<<"A">>,<<"G">>]),
        identifier1(<<"D">>, [<<"B">>]),
        identifier1(<<"E">>, [<<"A">>,<<"F">>]),
        identifier1(<<"F">>, [<<"B">>,<<"E">>]),
        identifier1(<<"G">>, [<<"C">>]),
        % last result order may change depending on dby_subscription or
        % dby_search implementation
        subscription2(<<"sub">>, [<<"C">>,<<"B">>,<<"A">>],
                [{sub, <<"A">>},{sub, <<"B">>},{sub, <<"C">>}])
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

% A -- B
example3() ->
    [
        identifier1(<<"A">>,[<<"B">>]),
        identifier1(<<"B">>,[<<"A">>])
    ].

%        A
%       / \
%      B   C
%      |   |
%      D---E
example4() ->
    [
        identifier1(<<"A">>,[<<"B">>,<<"C">>]),
        identifier1(<<"B">>,[<<"A">>,<<"D">>]),
        identifier1(<<"C">>,[<<"A">>,<<"E">>]),
        identifier1(<<"D">>,[<<"B">>,<<"E">>]),
        identifier1(<<"E">>,[<<"C">>,<<"D">>])
    ].

subscription1(Id, LastResult, Links) ->
    #identifier{
        id = Id,
        metadata = sub_metadata(LastResult),
        links = links1(Links)
    }.

subscription2(Id, LastResult, Links) ->
    #identifier{
        id = Id,
        metadata = sub_metadata2(LastResult),
        links = links1(Links)
    }.

publisherid() -> <<"testpubid">>.

timestamp() -> <<"2015-03-04T00:45:54Z">>.

identifier1(Id, Links) ->
    #identifier{id = Id, metadata = id_metadata1(Id), links = links1(Links)}.

sub_metadata(LastResult) ->
    #{
        system => subscription,
        search_fun => fun dby_test_mock:search_fn/4,
        acc0 => [],
        start_identifier => <<"A">>,
        options => #options{
            publish = persistent,
            persistent = true,
            traversal = depth,
            max_depth = 1,
            delta_fun = fun dby_test_mock:delta_fn/2,
            delivery_fun = fun dby_test_mock:delivery_fn/1
        },
        last_result => LastResult
    }.

sub_metadata2(LastResult) ->
    #{
        system => subscription,
        search_fun => fun dby_test_mock:search_fn/4,
        acc0 => [],
        start_identifier => <<"A">>,
        options => #options{
            publish = message,
            message = true,
            traversal = depth,
            max_depth = 1,
            delta_fun = fun dby_test_mock:delta_fn/2,
            delivery_fun = fun dby_test_mock:delivery_fn/1
        },
        last_result => LastResult
    }.

metadatainfo(Value) ->
    #{
        value => Value,
        publisher_id => publisherid(),
        timestamp => timestamp()
    }.

format_metadata(PL) ->
    lists:foldl(
        fun({Key, Value}, Acc) ->
            maps:put(Key, metadatainfo(Value), Acc)
        end, #{}, PL).

id_metadata1(Id) ->
    format_metadata([{<<"id">>, Id}, {<<"type">>,<<"identifier">>}]).

link_metadata1(Id) ->
    format_metadata([{<<"id">>, Id}, {<<"type">>,<<"link">>}]).

link_submetadata1(Id) ->
    maps:put(<<"id">>, Id, #{system => subscription}).

links1(Ids) ->
    lists:foldl(
        fun({sub, Id}, M) ->
            maps:put(Id, link_submetadata1(Id), M);
           (Id, M) ->
            maps:put(Id, link_metadata1(Id), M)
        end, #{}, Ids).

dby_read(Items) when is_list(Items) ->
    ok = meck:expect(dby_db, read, 1, meck:seq(Items));
dby_read(Fn) when is_function(Fn) ->
    ok = meck:expect(dby_db, read, Fn).

dby_db(Items) ->
    Dict = lists:foldl(
        fun(IdentifierR, D) ->
            dict:store({identifier, IdentifierR#identifier.id}, IdentifierR, D)
        end, dict:new(), Items),
    fun(Key = {identifier, Id}) ->
        case dict:find(Key, Dict) of
            error ->
                [#identifier{id = Id}];
            {ok, Value} ->
                [Value]
        end
    end.
