-module(dby).

% @doc
% dobby API
% @end

-export([publish/2,
         publish/4
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
