-module(dby_publish).

-export([publish/2]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

-spec publish([link() | dby_endpoint()], [publish_option()]) -> ok | reason().
publish(Data, Options) ->
    % XXX need to catch badarg
    Publish = (dby_options:options(Options))#options.publish,
    Fns = lists:foldl(
        fun({Endpoint1, Endpoint2, LinkMetadata}, Acc) ->
            [do_publish_link(Publish, Endpoint1, Endpoint2, LinkMetadata) | Acc];
           (Endpoint, Acc) ->
            [do_publish_endpoint(Publish, Endpoint) | Acc]
        end, [], Data),
    dby_db:transaction(joinfns(Fns)).

joinfns(Fns) ->
    fun() ->
        try
            lists:foreach(fun(Fn) -> Fn() end, Fns)
        catch
            throw:Reason ->
                dby_db:abort(Reason)
        end
    end.

do_publish_endpoint(persistent, Endpoint) ->
    fun() ->
        ok = persist_endpoint(Endpoint)
    end;
do_publish_endpoint(message, Endpoint) ->
    fun() ->
        ok = message_endpoint(Endpoint)
    end.

do_publish_link(persistent, Endpoint1, Endpoint2, LinkMetadata) ->
    fun() ->
        ok = persist_endpoint(Endpoint1, identifier(Endpoint2), LinkMetadata),
        ok = persist_endpoint(Endpoint2, identifier(Endpoint1), LinkMetadata)
    end;
do_publish_link(message, _, _, delete) ->
    throw({badarg, delete});
do_publish_link(message, Endpoint1, Endpoint2, LinkMetadata) ->
    fun() ->
        ok = message_endpoint(Endpoint1, identifier(Endpoint2), LinkMetadata),
        ok = message_endpoint(Endpoint2, identifier(Endpoint1), LinkMetadata)
    end.

identifier(Identifier) when is_binary(Identifier) ->
    Identifier;
identifier({Identifier, _}) when is_binary(Identifier) ->
    Identifier;
identifier(_) ->
    throw({badarg, identifier}).

persist_endpoint(Identifier) when is_binary(Identifier) ->
    persist_endpoint({Identifier, nochange});
persist_endpoint({Identifier, Metadata}) ->
    IdentifierR = read_identifier(Identifier),
    IdentifierR1 = update_identifier_metadata(IdentifierR, Metadata),
    ok = write_identifier(IdentifierR1).

persist_endpoint(Identifier, NeighborIdentifier, LinkMetadata) 
                                            when is_binary(Identifier) ->
    persist_endpoint({Identifier, nochange}, NeighborIdentifier, LinkMetadata);
persist_endpoint({Identifier, Metadata}, NeighborIdentifier, LinkMetadata) ->
    IdentifierR = read_identifier(Identifier),
    IdentifierR1 = update_identifier_metadata(IdentifierR, Metadata),
    IdentifierR2 = update_link(IdentifierR1, NeighborIdentifier, LinkMetadata),
    ok = write_identifier(IdentifierR2).

read_identifier(Identifier) ->
    case dby_db:read({identifier, Identifier}) of
        [R] -> R;
        _ -> #identifier{id = Identifier}
    end.

write_identifier(IdentifierR = #identifier{id = Identifier, metadata = delete}) ->
    % delete record
    ok = dby_db:delete({identifier, Identifier}),
    % delete links to this identifer
    lists:foreach(
        fun(NeighborIdentifier) ->
            ok = remove_link(Identifier, NeighborIdentifier)
        end, maps:keys(IdentifierR#identifier.links));
write_identifier(IdentifierR) ->
    % XXX write only if different
    ok = dby_db:write(IdentifierR).

% remove the link from neighbor identifier
remove_link(Identifier, NeighborIdentifier) ->
    case dby_db:read({identifier, NeighborIdentifier}) of
        [R = #identifier{links = Links}] ->
            dby_db:write(R#identifier{links = maps:without([Identifier], Links)});
        _ ->
            ok
    end.

update_identifier_metadata(IdentifierR = #identifier{}, nochange) ->
    IdentifierR;
update_identifier_metadata(IdentifierR = #identifier{}, delete) ->
    IdentifierR#identifier{metadata = delete};
update_identifier_metadata(
    IdentifierR = #identifier{metadata = OldMetadata}, Fn) 
                                                        when is_function(Fn) ->
    IdentifierR#identifier{metadata = apply_in_xact(Fn, [OldMetadata])};
update_identifier_metadata(IdentifierR = #identifier{}, NewMetadata) ->
    IdentifierR#identifier{metadata = NewMetadata}.

update_link(IdentifierR = #identifier{links = Links}, NeighborIdentifier, delete) ->
    IdentifierR#identifier{links = maps:without([NeighborIdentifier], Links)};
update_link(IdentifierR = #identifier{links = Links}, NeighborIdentifier, LinkMetadata) ->
    OldLinkMetadata = read_link_metadata(Links, NeighborIdentifier),
    IdentifierR#identifier{links =
                maps:put(NeighborIdentifier,
                         update_link_metadata(OldLinkMetadata, LinkMetadata),
                         Links)}.

read_link_metadata(Links, NeighborIdentifier) ->
    case maps:find(NeighborIdentifier, Links) of
        error -> null;
        {ok, Metadata} -> Metadata
    end.

update_link_metadata(OldMetadata, nochange) ->
    OldMetadata;
update_link_metadata(OldMetadata, Fn) when is_function(Fn) ->
    apply_in_xact(Fn, [OldMetadata]);
update_link_metadata(_, NewMetadata) ->
    NewMetadata.

message_endpoint(_) ->
    error(not_imlemented).

message_endpoint(_, _, _) ->
    error(not_imlemented).

apply_in_xact(Fn, Args) ->
    case catch {ok, apply(Fn, Args)} of
        {ok, Reply} -> Reply;
        {'EXIT', Reason} -> mnesia:abort({user_error, Reason})
    end.
