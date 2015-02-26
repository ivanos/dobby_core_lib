% Create and manage subscriptions.
% Subscription processing is in dby_subscriber.

% Subscriptions are stored in the graph as "system" identifiers.
% The subscription is an identifier metadata about the subscription
% in the identifier's metadata.  The identifiers that match the search
% are linked to the subscription.  On a publish, follow the links
% from the affected identifiers to the subscriptions.  Build a list of
% subscriptions to process and pass this to dby_subscribe.  dby_subscribe
% reruns the search and updates the links if the shape the sesarch
% result changes.

-module(dby_subscription).

-export([subscribe/4,
         publish/1,
         delete/1]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

-spec subscribe(search_fun(), term(), dby_identifier(), subscribe_options()) -> {ok, dby_identifier()} | {error, reason()}.
subscribe(Fun, Acc, StartIdentifier, Options) ->
    % store subscription in the graph
    Subscription = #{
        type => subscription,
        search_fun => Fun,
        acc0 => Acc,
        start_identifier => StartIdentifier,
        options => dby_options:options(Options)
    },
    Id = id(),
    Fn = fun() ->
        dby_publish:publish(subscription(Id, Subscription),
                                                [system, persistent])
    end,
    case dby_db:transaction(Fn) of
        ok ->
            {ok, Id};
        Error ->
            Error
    end.

-spec delete(identifier()) -> ok | {error, reason()}.
delete(SubscriptionId) ->
    Fn = fun() ->
        dby_publish:publish(SubscriptionId, delete, [system, persistent])
    end,
    dby_db:transaction(Fn).

-spec publish(identifier()) -> ok | {error, reason()}.
publish(SubscriptionId) ->
    % XXX update subscriber links to reflect discovered identifiers
    Fn = fun() ->
        dby:publish(do_publish(SubscriptionId), [system, persistent])
    end,
    dby_db:transaction(Fn).

% =============================================================================
% Internal functions
% =============================================================================

do_publish(SubscriptionId) ->
    % read identifier
    Id0 = dby_store:read_identifier(SubscriptionId),
    #identifier{metadata = Subscription0, links = Links} = Id0,
    % run search
    #{last_result := LastResult,
      options := #options{delta_fun = DeltaFn,
                          delivery_fun = DeliveryFn}} = Subscription0,
    {Discovered, SearchResult} = search(Subscription0),
    Id1 = case SearchResult =:= LastResult of
        true ->
            % subscription search returned the same result set
            % as last time so there is no delta to process
            Id0;
        false ->
            % apply delta
            case DeltaFn(LastResult, SearchResult) of
                stop ->
                    % delete subscription
                    set_delete(Id0);
                nodelta ->
                    % no delta, update the last result in the metadata
                    % nothing to deliver
                    set_last_result(Id0, SearchResult);
                {delta, Delta} ->
                    % deliver the delta
                    case DeliveryFn(Delta) of
                        ok ->
                            % set the last result in the metadata
                            set_last_result(Id0, SearchResult);
                        stop ->
                            % delete subscription
                            set_delete(Id0)
                    end
            end
    end,
    lists:flatten([
        update_discovered(SubscriptionId, maps:keys(Links), Discovered),
        publish_id_change(Id0, Id1)
    ]).

update_discovered(SubscriptionId, LastDiscovered, Discovered) ->
    [
        % added links
        lists:foldl(
            fun(Identifier, Acc) ->
                [{SubscriptionId, Identifier, #{type => subscriber}} | Acc]
            end, [], LastDiscovered --  Discovered),
        % removed links
        lists:foldl(
            fun(Identifier, Acc) ->
                [{SubscriptionId, Identifier, delete} | Acc]
            end, [], Discovered -- LastDiscovered)
    ].

publish_id_change(Id, Id) ->
    [];
publish_id_change(_, #identifier{id = Id, metadata = Metadata}) ->
    [{Id, Metadata}].

set_last_result(Id = #identifier{metadata = Subscription}, LastResult) ->
    Id#identifier{metadata = Subscription#{last_result => LastResult}}.

set_delete(Id = #identifier{}) ->
    Id#identifier{metadata = delete}.

subscription(Id, Subscription0 = #{type := subscription}) ->
    % run search and identify identifiers touched by search
    {Discovered, SearchResult} = search(Subscription0),
    % return list of identifiers to publish
    [
        % subscription
        {Id, Subscription0#{last_result => SearchResult}} |
        lists:map(
            fun(Identifier) ->
                {Id, Identifier, #{type => subscriber}}
            end, Discovered)
    ].

% returns list of identifiers traversed by the search and search result
search(#{
        search_fun := Fun,
        acc0 := Acc,
        start_identifier := StartIdentifier,
        options := Options}) ->
    case dby:search(search_fun(Fun), {[], Acc}, StartIdentifier,
                                                search_options(Options)) of
        {error, Reason} ->
            throw(Reason);
        {Discovered, SearchResult} ->
            {Discovered, SearchResult}
    end.

search_fun(Fun) ->
    fun(Identifier, IdMetadata, LinkMetadata, {Discovered, Acc0}) ->
        {Control, Fun1, Acc1} = search_fun(Fun, Identifier,
                                            IdMetadata, LinkMetadata, Acc0),
        {Control, search_fun(Fun1), {[Identifier | Discovered], Acc1}}
    end.

search_fun(Fun, Identifier, IdMetadata, LinkMetadata, Acc0) ->
        case Fun(Identifier, IdMetadata, LinkMetadata, Acc0) of
            {Control, Fun1, Acc1} ->
                {Control, Fun1, Acc1};
            {Control, Acc1} ->
                {Control, Fun, Acc1}
        end.

search_options(#options{publish = Publish,
                        loop = LoopDetection,
                        type = Type,
                        max_depth = MaxDepth}) ->
    [Publish, Type, {loop, LoopDetection}, {max_depth, MaxDepth}].

id() ->
    term_to_binary({node(), now()}).
