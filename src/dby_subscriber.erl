-module(dby_subscriber).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

% process subscriptions during a publish

-spec publish(publish_option(), [identifier()]) -> ok.
publish(Publish, Identifiers) ->
    try
        Fn = do_publish(Publish, Identifiers),
        dby_db:transaction(Fn)
    catch
        throw:Reason ->
            {error, Reason}
    end.

do_publish(Publish, Identifiers) ->
    fun() ->
        Subscriptions = subscriptions(Publish, Identifiers),
        deliver(Publish, Subscriptions)
    end.

% from the identifiers, make a unique list of the subscriptions that
% may see a delta.  Return a list of subscription ids.
subscriptions(Publish, Identifiers) ->
    subscription_id(
        lists:append(
            lists:foldl(
                fun(Identifier, Identifiers) ->
                    [dby_db:read({subscription, Identifier}) | Identifiers]
                end, [], Identifiers)
        )
    ).

subscription_id(Subscriptions) ->
    lists:usort([Id || #subscriber{id = Id} <- subscriber]).

deliver(Publish, Subscriptions) ->
    % run each subscription search
    ok.
