-module(dby_subscription).

-export([subscribe/4]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

-spec subscribe(search_fun(), term(), dby_identifier(), subscribe_options()) -> {ok, term()} | {error, reason()}.
subscribe(Fun, Acc, StartIdentifier, Options) ->
    try
        Subscription = #subscription{
            id = {node(), now()},
            searchfun = Fun,
            acc0 = Acc,
            start_identifier = StartIdentifier,
            options = dby_options:options(Options)
        },
        Fn = do_subscribe(Subscription),
        dby_db:transaction(Fn)
    catch
        throw:Reason ->
            {error, Reason}
    end.

publish(SubscriptionId) ->
    % lookup subscription
    % run search
    % apply delta
    % deliver if needed
    % update subscriber table
    % update last_result, last_discovered in subscription
    ok.

do_subscribe(Subscription0 = #subscription{id = SubscriptionId}) ->
    fun() ->
        % run search and identify identifiers touched by search
        {Discovered, SearchResult} = search(Subscription0),
        % store subscription,
        Subscription1 = Subscription0#subscription{
                                        last_result = SearchResult,
                                        last_discovered = Discovered},
        ok = dby_db:write(subscription, Subscription1),
        % tag identifiers
        lists:foreach(
            fun(Identifier) ->
                ok = dby_db:write(subscriber, #subscriber{
                                    identifier = Identifier,
                                    id = SubscriptionId})
            end, Discovered),
        % return subscription id
        SubscriptionId
    end.

% returns list of identifiers traversed by the search and search result
search(#subscription{
            searchfun = Fun,
            acc0 = Acc,
            start_identifier = StartIdentifier,
            options = Options}) ->
    case dby:search(Fun, {[], Acc}, StartIdentifier, search_options(Options)) of
        {error, Reason} ->
            throw(Reason);
        {Discovered, SearchResult} ->
            {Discovered, SearchResult}
    end.

search_fun(Fun) ->
    fun(Identifier, IdMetadata, LinkMetadata, {Discovered, Acc0}) ->
        {Control, Fun1, Acc1} = search_fun(Fun,
                                    Identifier, IdMetadata, LinkMetadata, Acc0),
        {Control, Fun1, {[Identifier | Discovered], Acc1}}
    end.

search_fun(Fun, Identifier, IdMetadata, LinkMetadata, Acc0) ->
        case Fun(Identifier, IdMetadata, LinkMetadata, Acc0) of
            {Control, Fun1, Acc1} ->
                {Control, Fun1, Acc1};
            {Control, Acc1} ->
                {Control, Fun, Acc1}
        end.

search_options(#options{publish = Publish, max_depth = MaxDepth}) ->
    [Publish, {max_depth, MaxDepth}].
