% Create and manage subscriptions.

% Subscriptions are stored in the graph as "system" identifiers.
% The subscription is an identifier metadata about the subscription
% in the identifier's metadata.  The identifiers that match the search
% are linked to the subscription.  On a publish, follow the links
% from the affected identifiers to the subscriptions.  Build a list of
% subscriptions to process and pass this to publish/1.  publish/1
% reruns the search and updates the links if the shape the sesarch
% result changes.

-module(dby_subscription).

-export([subscribe/4,
         publish/3,
         delete/1]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

-spec subscribe(search_fun(), term(), dby_identifier(), subscribe_options()) -> {ok, dby_identifier()} | {error, reason()}.
subscribe(Fun, Acc, StartIdentifier, Options) ->
    % store subscription in the graph
    Subscription = #{
        system => subscription,
        search_fun => Fun,
        acc0 => Acc,
        start_identifier => StartIdentifier,
        options => dby_options:options(Options)
    },
    Id = id(),
    Fn = fun() ->
        dby_publish:publish(?PUBLISHER,
                                subscription(dby_search:read_fn(),
                                                    Id, Subscription),
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
        dby_publish:publish(?PUBLISHER, SubscriptionId, delete, [persistent])
    end,
    dby_db:transaction(Fn).

-spec publish(identifier(), publish_type(), db_read_fun()) -> ok | {error, reason()}.
publish(SubscriptionId, Publish, ReadFn) ->
    Fn = fun() ->
        dby_publish:publish(?PUBLISHER, do_publish(Publish,
                                SubscriptionId, ReadFn), [system, persistent])
    end,
    dby_db:transaction(Fn).

% =============================================================================
% Internal functions
% =============================================================================

do_publish(Publish, SubscriptionId, ReadFn) ->
    Id0 = dby_store:read_identifier(SubscriptionId),
    #identifier{metadata = Subscription0, links = Links} = Id0,
    % run search
    #{last_result := LastResult,
      options := #options{persistent = Persistent,
                          message = Message,
                          delta_fun = DeltaFn,
                          delivery_fun = DeliveryFn}} = Subscription0,
    case (Publish == message andalso Message) orelse
            (Publish == persistent andalso Persistent) of
        false ->
            % publish is not the same type as the subscription
            [];
        true ->
            {Discovered, SearchResult} = search(ReadFn, Subscription0),
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
                            set_last_result(Publish, Id0, SearchResult);
                        {delta, Delta} ->
                            % deliver the delta
                            case DeliveryFn(Delta) of
                                ok ->
                                    % set the last result in the metadata
                                    set_last_result(Publish, Id0, SearchResult);
                                stop ->
                                    % delete subscription
                                    set_delete(Id0)
                            end
                    end
            end,
            lists:flatten([
                update_discovered(Publish, SubscriptionId,
                                                        maps:keys(Links), Discovered),
                publish_id_change(Id0, Id1)
            ])
    end.

% update the links to the discovered identifiers.  Only do this for
% persistent publishes.
update_discovered(message, _, _, _) ->
    [];
update_discovered(persistent, SubscriptionId, LastDiscovered, Discovered) ->
    [
        % added links
        lists:foldl(
            fun(Identifier, Acc) ->
                [{SubscriptionId, Identifier, [{system, subscription}]} | Acc]
            end, [], Discovered --  LastDiscovered),
        % removed links
        lists:foldl(
            fun(Identifier, Acc) ->
                [{SubscriptionId, Identifier, delete} | Acc]
            end, [], LastDiscovered -- Discovered)
    ].

publish_id_change(Id, Id) ->
    [];
publish_id_change(_, #identifier{id = Id, metadata = delete}) ->
    [{Id, delete}];
publish_id_change(_, #identifier{id = Id, metadata = Metadata}) ->
    [{Id, maps:to_list(Metadata)}].

% update the last result of the search.  Only do this for persistent
% publishes.  Message publishes always compute delta against the last
% persistent update.
set_last_result(message, Identifier, _) ->
    Identifier;
set_last_result(persistent, Id = #identifier{metadata = Subscription}, LastResult) ->
    Id#identifier{metadata = Subscription#{last_result => LastResult}}.

set_delete(Id = #identifier{}) ->
    Id#identifier{metadata = delete}.

subscription(ReadFn, Id, Subscription0 = #{system := subscription}) ->
    % run search and identify identifiers touched by search
    {Discovered, SearchResult} = search(ReadFn, Subscription0),
    % return list of identifiers to publish
    [
        % subscription
        {Id, maps:to_list(Subscription0#{last_result => SearchResult})} |
        lists:map(
            fun(Identifier) ->
                {Id, Identifier, [{system, subscription}]}
            end, Discovered)
    ].

% returns list of identifiers traversed by the search and search result
search(ReadFn, #{
        search_fun := Fun,
        acc0 := Acc,
        start_identifier := StartIdentifier,
        options := Options}) ->
    case dby_search:search(ReadFn, search_fun(Fun), {[], Acc}, StartIdentifier,
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

search_options(#options{loop = LoopDetection,
                        type = Type,
                        max_depth = MaxDepth}) ->
    [Type, {loop, LoopDetection}, {max_depth, MaxDepth}].

id() ->
    term_to_binary({node(), now()}).
