-module(dby_bulk).

-export([export/2,
         import/2]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

-record(term, {
    type :: link | identifier,
    identifier :: dby_identifier(),
    link :: {dby_identifier(), dby_identifier()},
    metadata :: jsonable()
}).

% ==============================================================================
% API functions
% ==============================================================================

% @doc
% `export/2' writes the graph database to the file.  The only supported
% format is `json'.
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
% The only supported formats are `json' and `json0'.
%
% `json0' is a backward compatible importer.  Use this for the first
% version of the export files.
% @end
-spec import(Format :: json | json0, string()) -> ok | {error, reason()}.
import(Format, Filename) ->
    {ok, Binary} = file:read_file(Filename),
    Decode = jiffy:decode(Binary),
    Links = links(Format, Decode),
    PublisherId = <<"bulk">>,
    dby_publish:publish(PublisherId, Links, [persistent]).

% ==============================================================================
% export helper functions
% ==============================================================================

% XXX encode and decode publisher id and timestamp

json_link(#identifier{metadata = #{system := _}}) ->
    % do not save system identifiers
    [];
json_link(#identifier{id = Identifier, metadata = Metadata, links = Links}) ->
    [
        {[{<<"identifier">>, Identifier},
          {<<"metadata">>, {json_metadatainfo(Metadata)}}]},
        maps:fold(
            fun(_, #{system := _}, Jsons) ->
                % do not save links to system identifiers
                Jsons;
               (NeighborIdentifier, LinkMetadata, Jsons) ->
                [
                    {[{<<"link">>, [Identifier, NeighborIdentifier]},
                      {<<"metadata">>, {json_metadatainfo(LinkMetadata)}}]} |
                    Jsons
                ]
            end, [], Links)
    ].

json_metadatainfo(Metadata) ->
    maps:fold(
        fun(Key, #{value := Value, publisher_id := PublisherId, timestamp := Timestamp}, Acc) ->
            [ {Key, {[
                {<<"value">>, json_metadata(Value)},
                {<<"publisher_id">>, PublisherId},
                {<<"timestamp">>, Timestamp}
            ]}} | Acc]
        end, [], Metadata).

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

links(Format, Json) ->
    lists:reverse(lists:foldl(
        fun(Element, Data) ->
            [term(Format, Element) | Data]
        end, [], Json)).

term(Format, {Json}) ->
    term(Format, term_decode(Json));
term(Format, #term{type = identifier, identifier = Identifier, metadata = Metadata}) ->
    {Identifier, term_metadatainfo(Format, Metadata)};
term(Format, #term{type = link, link = {Identifier1, Identifier2}, metadata = Metadata}) ->
    {Identifier1, Identifier2, term_metadatainfo(Format, Metadata)}.

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

term_metadatainfo(json0, null) ->
    [];
term_metadatainfo(json0, {Map}) ->
    lists:foldl(
        fun({Key, Value}, Acc) ->
            [{Key, term_metadata(Value)} | Acc]
        end, [], Map);
term_metadatainfo(json, {Map}) ->
    lists:foldl(
        fun({Key, {Value}}, Acc) ->
            [{Key, term_metadatainfo_value(Value)} | Acc]
        end, [], Map).

term_metadatainfo_value(Value) ->
    lists:foldl(
        fun({<<"value">>, V}, {_, P, T}) ->
            {term_metadata(V), P, T};
           ({<<"publisher_id">>, PublisherId}, {V, _, T}) ->
            {V, PublisherId, T};
           ({<<"timestamp">>, Timestamp}, {V, P, _}) ->
            {V, P, Timestamp}
        end, {v, p, t}, Value).

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
