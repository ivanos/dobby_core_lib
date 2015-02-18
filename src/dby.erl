-module(dby).

% @doc
% dobby API
% @end

-export([publish/2,
         publish/4,
         search/4,
         identifiers/4
        ]).

-include_lib("dobby/include/dobby.hrl").

% =============================================================================
% API functions
% =============================================================================

% @equiv publish([{Endpoint1, Endpoint2, LinkMetadata}], Options).
-spec publish(endpoint(), endpoint(), metadata(), [publish_option()]) -> ok | {error, reason()}.
publish(Endpoint1, Endpoint2, LinkMetadata, Options) ->
    publish([{Endpoint1, Endpoint2, LinkMetadata}], Options).

% @doc
% `publish/2' adds, removes, or modifies an identifier or link,
% or sends a message via an identifier or
% link. The endpoints of the link are specified using an identifier
% or a tuple with the identifier and metadata.  If the endpoint is
% specified as an identifier, the metadata for that identifier is not
% changed.  Likewise, if the metadata is `nochange' the metadata for
% the endpoint is not changed.  If the identifier metadata is `delete' the
% identifier is deleted and all links to the identifier are also deleted.
% If `LinkMetadata' is not specified or
% is `nochange' the link metadata is not changed.  If LinkMetadata is
% `delete' the link between the two endpionts is deleted.
% If `publish/2' creates
% a new identifier or link and no metadata is provided, the metadata
% is set to `null'.  `Metadata' may be any Erlang term that can be
% represented as a JSON.  If the metadata is specified as a function,
% the function is called with the old metadata and returns the updated
% metadata for that link or identifier.  The `persistent' option means
% metadata changes are persisted in Dobby.  The `message' option means
% metadata changes are communicated to subscriptions but the changes
% are not persisted in dobby.  `message' is the default behavior. For
% a `message' publish, the two endpoints and link must already exist.
%
% Returns `badarg' for `message' publish if one of the endppoints or the
% link between them does not exist.
%
% `publish/2' may also be called with an endpoint.  This is a convenience
% for adding, removing, or modifying a single identifier.
% @end
-spec publish([endpoint() | link()] | endpoint(), [publish_option()]) -> ok | {error, reason()}.
publish(Endpoint, Options) when is_tuple(Endpoint); is_binary(Endpoint) ->
    publish([Endpoint], Options);
publish(Data, Options) ->
    dby_publish:publish(Data, Options).

% @doc
% `search/4' performs a fold over the graph beginning with the identifier
% in `StartIdentifier'.  The first identifier passed to `Fun' is always
% `StartIdentifier' with `LinkMetadata' as undefined.  The options `breadth'
% and `depth' control how the graph is traversed.  For `breadth', all the
% links to `StartIdentifier' are traversed first, followed by all the
% links of `StartIdentifier'’s neighbors, etc.  For `depth', one link of
% `StartIdentifier' is traversed, followed by one link of that neighbor
% identifier, etc.  If neither is specified, `breadth' is used.  `Acc' is the
% initial accumulator value.  `Fun' is called for every `Identifier'
% traversed by search. It controls the graph traversal and may also
% transform the result.  `Identifier' is an identifier, `IdMetadata' is
% the metadata for that identifier, `LinkMetadata' is the metadata on
% the link to the identifier, and `Acc0' is the current accumulator.
% `Fun' returns a status that controls the next step of the navigation
% and the new accumulator.  The possible control values are: `continue'
% to continue the search, `skip' to continue the search but skip
% navigating to any neighbors of this identifier, `stop' to stop the
% search with this identifier.  The option `max_depth' controls how far
% to navigate away from the starting identifier.  `max_depth' of 0 means
% no navigation is performed.  `max_depth' of one means search only
% navigates to the immediate neighbors of the starting identifier.
% If `max_depth' is not provided, `0' is used.
% @end
-spec search(Fun :: search_fun(), Acc :: term(), StartIdentifier :: identifier(), [search_options()]) -> term() | {error, reason()}.
search(Fun, Acc, StartIdentifier, Options) ->
        dby_search:search(Fun, Acc, StartIdentifier, Options).

% @doc
% When used as the function for `dby:search/4', returns the list of
% identifiers traversed in the search as tuples containing the
% identifier, the identifier's metadata, and the link's metadata.
% @end
-spec identifiers(identifier(), jsonable(), jsonable(), list()) -> {continue, list()}.
identifiers(Identifier, IdMetadata, LinkMetadata, Acc) ->
    {continue, [{Identifier, IdMetadata, LinkMetadata} | Acc]}.
