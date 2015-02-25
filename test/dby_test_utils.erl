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
