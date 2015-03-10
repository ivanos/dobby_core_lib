-module(dby_publish).

-export([publish/3]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

-spec publish(publisher_id(), [link() | dby_endpoint()], [publish_option()]) -> ok | {error, reason()}.
publish(_, [], _) ->
    ok;
publish(PublisherId, Data, Options) ->
    % XXX need to catch badarg
    Publish = (dby_options:options(Options))#options.publish,
    Type = (dby_options:options(Options))#options.type,
    Transaction = transaction_new(Type),
    Fns = lists:foldl(
        fun({Endpoint1, Endpoint2, LinkMetadata}, Acc) ->
            [do_publish_link(Transaction, PublisherId, Publish, Endpoint1, Endpoint2, LinkMetadata) | Acc];
           (Endpoint, Acc) ->
            [do_publish_endpoint(Transaction, PublisherId, Publish, Endpoint) | Acc]
        end, [], Data),
    case dby_db:transaction(joinfns(Fns)) of
        ok ->
            transaction_commit(Transaction, Publish),
            ok;
        {error, Reason} ->
            transaction_abort(Transaction),
            {error, Reason}
    end.

joinfns(Fns) ->
    fun() ->
        try
            lists:foreach(fun(Fn) -> Fn() end, Fns)
        catch
            throw:Reason ->
                dby_db:abort(Reason)
        end
    end.

do_publish_endpoint(Transaction, PublisherId, persistent, Endpoint) ->
    fun() ->
        ok = persist_endpoint(Transaction, PublisherId, Endpoint)
    end;
do_publish_endpoint(Transaction, PublisherId, message, Endpoint) ->
    fun() ->
        ok = message_endpoint(Transaction, PublisherId, Endpoint)
    end.

do_publish_link(Transaction, PublisherId, persistent, Endpoint1, Endpoint2, LinkMetadata) ->
    fun() ->
        ok = persist_endpoint(Transaction, PublisherId, Endpoint1, identifier(Endpoint2), LinkMetadata),
        ok = persist_endpoint(Transaction, PublisherId, Endpoint2, identifier(Endpoint1), LinkMetadata)
    end;
do_publish_link(_, _, message, _, _, delete) ->
    throw({badarg, delete});
do_publish_link(Transaction, PublisherId, message, Endpoint1, Endpoint2, LinkMetadata) ->
    fun() ->
        ok = message_endpoint(Transaction, PublisherId, Endpoint1, identifier(Endpoint2), LinkMetadata),
        ok = message_endpoint(Transaction, PublisherId, Endpoint2, identifier(Endpoint1), LinkMetadata)
    end.

identifier(Identifier) when is_binary(Identifier) ->
    Identifier;
identifier({Identifier, _}) when is_binary(Identifier) ->
    Identifier;
identifier(_) ->
    throw({badarg, identifier}).

persist_endpoint(Transaction, PublisherId, Identifier) when is_binary(Identifier) ->
    persist_endpoint(Transaction, PublisherId, {Identifier, nochange});
persist_endpoint(Transaction, PublisherId, {Identifier, Metadata}) ->
    IdentifierR = dby_store:read_identifier(Identifier),
    IdentifierR1 = update_identifier_metadata(PublisherId, IdentifierR, Metadata),
    ok = write_identifier(Transaction, IdentifierR1).

persist_endpoint(Transaction, PublisherId, Identifier, NeighborIdentifier, LinkMetadata) 
                                            when is_binary(Identifier) ->
    persist_endpoint(Transaction, PublisherId, {Identifier, nochange}, NeighborIdentifier, LinkMetadata);
persist_endpoint(Transaction, PublisherId, {Identifier, Metadata}, NeighborIdentifier, LinkMetadata) ->
    IdentifierR = dby_store:read_identifier(Identifier),
    IdentifierR1 = update_identifier_metadata(PublisherId, IdentifierR, Metadata),
    IdentifierR2 = update_link(PublisherId, IdentifierR1, NeighborIdentifier, LinkMetadata),
    ok = write_identifier(Transaction, IdentifierR2).

write_identifier(Transaction, IdentifierR = #identifier{id = Identifier, metadata = delete}) ->
    % delete record
    ok = dby_db:delete({identifier, Identifier}),
    % delete links to this identifer
    lists:foreach(
        fun(NeighborIdentifier) ->
            ok = remove_link(Transaction, Identifier, NeighborIdentifier)
        end, maps:keys(IdentifierR#identifier.links));
write_identifier(Transaction, IdentifierR) ->
    % XXX write only if different
    ok = transaction_publish(Transaction, IdentifierR),
    ok = dby_db:write(IdentifierR).

% remove the link from neighbor identifier
remove_link(Transaction, Identifier, NeighborIdentifier) ->
    case dby_db:read({identifier, NeighborIdentifier}) of
        [R0 = #identifier{links = Links}] ->
            R1 = R0#identifier{links = maps:without([Identifier], Links)},
            ok = transaction_publish(Transaction, R1),
            ok = dby_db:write(R1);
        _ ->
            ok
    end.

update_identifier_metadata(PublisherId,
            IdentifierR = #identifier{metadata = OldMetadata}, NewMetadata) ->
    IdentifierR#identifier{
        metadata = merge_metadata(PublisherId, OldMetadata, NewMetadata)
    }.

update_link(_, IdentifierR = #identifier{links = Links}, NeighborIdentifier, delete) ->
    IdentifierR#identifier{links = maps:without([NeighborIdentifier], Links)};
update_link(PublisherId, IdentifierR = #identifier{links = Links}, NeighborIdentifier, LinkMetadata) ->
    OldLinkMetadata = read_link_metadata(Links, NeighborIdentifier),
    IdentifierR#identifier{links =
        maps:put(NeighborIdentifier,
                 merge_metadata(PublisherId, OldLinkMetadata, LinkMetadata),
                 Links)}.

read_link_metadata(Links, NeighborIdentifier) ->
    case maps:find(NeighborIdentifier, Links) of
        error -> #{};
        {ok, Metadata} -> Metadata
    end.

merge_metadata(_, OldMetadata, nochange) ->
    OldMetadata;
merge_metadata(_, _, delete) ->
    delete;
merge_metadata(PublisherId, OldMetadata, Fn) when is_function(Fn) ->
    merge_metadata(PublisherId, OldMetadata,
                    apply_in_xact(Fn, [metadata_proplist(OldMetadata)]));
merge_metadata(PublisherId, OldMetadata, NewMetadata) ->
    Timestamp = dby_time:timestamp(),
    lists:foldl(
        fun({Key, delete}, Acc) ->
            maps:without([Key], Acc);
           ({Key, {Value, PubId, TS}}, Acc) ->
            maps:put(Key, #{
                value => Value,
                publisher_id => PubId,
                timestamp => TS
            }, Acc);
           ({Key, Value}, Acc) when is_atom(Key) ->
            % system keys do not need publisher id or timestamp
            maps:put(Key, Value, Acc);
           ({Key, Value}, Acc) ->
            maps:put(Key, #{
                value => Value,
                publisher_id => PublisherId,
                timestamp => Timestamp
            }, Acc)
        end, OldMetadata, NewMetadata).

metadata_proplist(Metadata = #{}) ->
    maps:fold(
        fun(Key, #{value := Value}, Acc) ->
            [{Key, Value} | Acc]
        end, [], Metadata).

message_endpoint(_, _, _) ->
    error(not_imlemented).

message_endpoint(_, _, _, _, _) ->
    error(not_imlemented).

apply_in_xact(Fn, Args) ->
    case catch {ok, apply(Fn, Args)} of
        {ok, Reply} -> Reply;
        {'EXIT', Reason} -> mnesia:abort({user_error, Reason})
    end.

transaction_new(user) ->
    dby_transaction:new();
transaction_new(system) ->
    no_transaction.

transaction_publish(no_transaction, _) ->
    ok;
transaction_publish(Transaction, IdentifierR) ->
    dby_transaction:publish(Transaction, IdentifierR).

transaction_commit(no_transaction, _) ->
    ok;
transaction_commit(Transaction, Publish) ->
    dby_transaction:commit(Transaction, Publish).

transaction_abort(no_transaction) ->
    ok;
transaction_abort(Transaction) ->
    dby_transaction:abort(Transaction).
