-module(dby).

% @doc
% dobby API
% @end

-export([transaction/1,
         abort/1,
         publish/3,
         publish/4,
         publish/5
        ]).

-include_lib("dobby/include/dobby.hrl").

% =============================================================================
% API functions
% =============================================================================

% @doc
% `transaction/1' executes Fn as a transaction.
% @end
-spec transaction(fun( (transaction()) -> term() )) -> {ok, term()} | {error, reason()}.
transaction(Fn) ->
    dby_transcation:transaction(Fn).

% @doc
% `abort/1' aborts the transaction.
% @end
-spec abort(transaction()) -> ok.
abort(_) ->
    dby_transcation:abort().

% @equiv publish(Transaction, Endpoint1, Endpoint2, null, [message]).
-spec publish(transaction(), endpoint(), endpoint()) -> ok | {error, reason()}.
publish(Transaction, Endpoint1, Endpoint2) ->
    publish(Transaction, Endpoint1, Endpoint2, null).

% @equiv publish(Transaction, Endpoint1, Endpoint2, LinkMetadata, [message]).
-spec publish(transaction(), endpoint(), endpoint(), metadata()) -> ok | {error, reason()}.
publish(Transaction, Endpoint1, Endpoint2, LinkMetadata) ->
    publish(Transaction, Endpoint1, Endpoint2, LinkMetadata, [message]).

% @doc
% `publish/5' adds or modifies a link, or sends a message via the
% link.  Transaction is the transaction identifier provided by
% `dby_transaction/1'.
% The endpoints of the link are specified using an identifier
% or a tuple with the identifier and metadata.  If the endpoint is
% specified as an identifier, the metadata for that identifier is not
% changed.  Likewise, if the metadata is `nochange' the metadata for
% the endpoint is not changed.  If `LinkMetadata' is not specified or
% is `nochange' the link metadata is not changed.  If `publish/4' creates
% a new identifier or link and no metadata is provided, the metadata
% is set to `null'.  `Metadata' may be any Erlang term that can be
% represented as a JSON.  If the metadata is specified as a function,
% the function is called with the old metadata and returns the updated
% metadata for that link or identifier.  The `persistent' option means
% metadata changes are persisted in dobby.  The `message' option means
% metadata changes are communicated to subscriptions but the changes
% are not persisted in dobby.  `message' is the default behavior. For
% a `message' publish, the two endpoints and link must already exist.
%
% Returns `badarg' for `message' publish if one of the endppoints or the
% link between them does not exist.
% @end
-spec publish(transaction(), endpoint(), endpoint(), metadata(), [publish_option()]) -> ok | {error, reason()}.
publish(_, Endpoint1, Endpoint2, LinkMetadata, Options) ->
    dby_publish:publish(Endpoint1, Endpoint2, LinkMetadata, Options).
