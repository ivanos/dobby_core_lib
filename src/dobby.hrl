% options for all API functions
-record(options, {
    publish = message :: persistent | message,
    traversal = breadth :: breadth | depth,
    max_depth = 0 :: non_neg_integer(),
    loop = identifier :: loop_detection(),
    delta_fun = fun dby_options:delta_default/2 :: fun(),
    delivery_fun = fun dby_options:delivery_default/1 :: fun()
}).

% database representation of an identifier (vertex)
-record(identifier, {
    id :: identifier(),
    metadata = null :: jsonable(),
    links = #{} :: #{identifier() => jsonable()}
}).

% XXX store the subcriptions in the graph
% subscription is an identifier
%   metadata has other values
%   linked to results of search
% On publish, publish follows links on identifiers and links it modifies to
% the subscriptions, building a list of subscriptions to run.
% Passes the subscriptions to dby_subscribe to rerun searches.

% database representation of a subscription definition
-record(subscription, {
    id :: term(),
    searchfun :: search_fun(),
    acc0 :: term(),
    start_identifier :: identifier(),
    options :: #options{},
    last_result :: term(),
    last_discovered :: [identifier()]
}).

% database representation of a subscriber.  Maps identifiers to subscriptions.
-record(subscriber, {
    identifier :: identifier(),
    id = #subscription{}
}).
