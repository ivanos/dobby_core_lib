-module(dby_publish).

-export([publish/4]).

-include_lib("dobby/include/dobby.hrl").

% options for all API functions
-record(options, {
    publish = message :: persistent | message,
    traversal = breadth :: breadth | depth,
    max_depth = 0 :: non_neg_integer(),
    delta_fun = fun delta_default/2 :: fun(),
    delivery_fun = fun delivery_default/1 :: fun()
}).

-spec publish(endpoint(), endpoint(), metadata(), publish_option()) -> ok | reason().
publish(Endpoint1, Endpoint2, LinkMetadata, Options) ->
    Publish = (options(Options))#options.publish,
    do_publish(Publish, Endpoint1, Endpoint2, LinkMetadata).

do_publish(persistent, Endpoint1, Endpoint2, LinkMetadata) ->
    ok = persist_endpoint(Endpoint1, identifier(Endpoint2), LinkMetadata),
    ok = persist_endpoint(Endpoint2, identifier(Endpoint1), LinkMetadata);
do_publish(message, Endpoint1, Endpoint2, LinkMetadata) ->
    ok = message_endpoint(Endpoint1, identifier(Endpoint2), LinkMetadata),
    ok = message_endpoint(Endpoint2, identifier(Endpoint1), LinkMetadata).

identifier(Identifier) when is_binary(Identifier) ->
    Identifier;
identifier({Identifier, _}) when is_binary(Identifier) ->
    Identifier;
identifier(_) ->
    throw({error, badarg, identifier}).

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

write_identifier(IdentifierR) ->
    % XXX write only if different
    ok = dby_db:write(IdentifierR).

update_identifier_metadata(IdentifierR = #identifier{}, nochange) ->
    IdentifierR;
update_identifier_metadata(
    IdentifierR = #identifier{metadata = OldMetadata}, Fn) 
                                                        when is_function(Fn) ->
    IdentifierR#identifier{metadata = apply_in_xact(Fn, [OldMetadata])};
update_identifier_metadata(IdentifierR = #identifier{}, NewMetadata) ->
    IdentifierR#identifier{metadata = NewMetadata}.

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

message_endpoint(_, _, _) ->
    error(not_imlemented).

apply_in_xact(Fn, Args) ->
    case catch {ok, apply(Fn, Args)} of
        {ok, Reply} -> Reply;
        {'EXIT', Reason} -> mnesia:abort({user_error, Reason})
    end.

options(Options) ->
    lists:foldl(
        fun(persistent, Record) ->
            Record#options{publish = persistent};
           (message, Record) ->
            Record#options{publish = message};
           (breadth, Record) ->
            Record#options{traversal = breadth};
           (depth, Record) ->
            Record#options{traversal = depth};
           ({max_depth, Depth}, Record) when Depth >= 0 and is_integer(Depth) ->
            Record#options{max_depth = Depth};
           ({delta_fun, DFun}, Record) when is_function(DFun) ->
            Record#options{delta_fun = DFun};
           ({delivery_fun, SFun}, Record) when is_function(SFun) ->
            Record#options{delivery_fun = SFun};
           (BadArg, _) ->
            throw({error, badarg, BadArg})
        end, #options{}, Options).

% Default delta function for subscriptions.  Return the new value.
delta_default(_, New) ->
    {delta, New}.

% Default delivery function for subscriptions.  Do nothing.
delivery_default(_) ->
    ok.
