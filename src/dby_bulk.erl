-module(dby_bulk).

-export([export/2,
         import/2]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

-record(term, {
    type :: link | identifier,
    identifier :: identifier(),
    link :: {identifier(), identifier()},
    metadata :: jsonable()
}).

% ==============================================================================
% API functions
% ==============================================================================

% @doc
% `export/2' writes the graph database to the file.  The only supported
% format is json.
% @end
-spec export(Format :: json, string()) -> ok | {error, term()}.
export(json, Filename) ->
    Fn = fun() ->
        lists:flatten(dby_db:foldl(
            fun(IdentifierR, Jsons) ->
                [json_link(IdentifierR) | Jsons]
            end, [], identifier
        ))
    end,
    Data = dby_db:transaction(Fn),
    file:write_file(Filename, jiffy:encode(Data)).

% @doc
% `import/2' imports a file written `export/2' into the graph database.
% The only supported format is json.
% @end
-spec import(Format :: json, string()) -> ok | {error, reason()}.
import(json, Filename) ->
    {ok, Binary} = file:read_file(Filename),
    Decode = jiffy:decode(Binary),
    Links = links(Decode),
    dby_publish:publish(Links, [persistent]).

% ==============================================================================
% export helper functions
% ==============================================================================

json_link(#identifier{id = Identifier, metadata = Metadata, links = Links}) ->
    [
        {[{<<"identifier">>, Identifier},
          {<<"metadata">>, json_metadata(Metadata)}]},
        maps:fold(
            fun(NeighborIdentifier, LinkMetadata, Jsons) ->
                [
                    {[{<<"link">>, [Identifier, NeighborIdentifier]},
                      {<<"metadata">>, json_metadata(LinkMetadata)}]} |
                    Jsons
                ]
            end, [], Links)
    ].

json_metadata(true) ->
    true;
json_metadata(false) ->
    false;
json_metadata(null) ->
    null;
json_metadata(Data) when is_integer(Data) ->
    Data;
json_metadata(Data) when is_binary(Data) ->
    Data;
json_metadata(Data) when is_list(Data) ->
    lists:map(
        fun(Element) ->
            json_metadata(Element)
        end, Data);
json_metadata(Data) when is_map(Data) ->
    {
        maps:fold(
            fun(Key, Value, Acc) ->
                [{Key, json_metadata(Value)} | Acc]
            end, [], Data)
    }.


% ==============================================================================
% import helper functions
% ==============================================================================

links(Json) ->
    lists:reverse(lists:foldl(
        fun(Element, Data) ->
            [term(Element) | Data]
        end, [], Json)).

term({Json}) ->
    term(term_decode(Json));
term(#term{type = identifier, identifier = Identifier, metadata = Metadata}) ->
    {Identifier, term_metadata(Metadata)};
term(#term{type = link, link = {Identifier1, Identifier2}, metadata = Metadata}) ->
    {Identifier1, Identifier2, term_metadata(Metadata)}.

term_decode(Json) ->
    term_decode(Json, #term{}).

term_decode([], Term) ->
    Term;
term_decode([{<<"metadata">>, Metadata} | Rest], Term) ->
    term_decode(Rest, Term#term{metadata = Metadata});
term_decode([{<<"link">>, [Identifier1, Identifier2]} | Rest], Term) ->
    term_decode(Rest, Term#term{type = link,
                                link = {Identifier1, Identifier2}});
term_decode([{<<"identifier">>, Identifier} | Rest], Term) ->
    term_decode(Rest, Term#term{type = identifier, identifier = Identifier}).

term_metadata(true) ->
    true;
term_metadata(false) ->
    false;
term_metadata(null) ->
    null;
term_metadata(Data) when is_integer(Data) ->
    Data;
term_metadata(Data) when is_binary(Data) ->
    Data;
term_metadata(Data) when is_list(Data) ->
    lists:map(
        fun(Element) ->
            term_metadata(Element)
        end, Data);
term_metadata({Map}) ->
    lists:foldl(
        fun({Key, Value}, Acc) ->
            maps:put(Key, term_metadata(Value), Acc)
        end, #{}, Map).
