-module(dby_search).

-export([search/4,
         search/5,
         read_fn/0]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").
-include("dby_logger.hrl").

-record(search, {
    fn :: fun(),
    identifier :: dby_identifier(),
    from = undefined :: dby_identifier() | undefined,
    metadata :: jsonable(),
    path :: [{dby_identifier(), metadata_info(), metadata_info()}], % metadata on link to this identifier
    links = [] :: [{dby_identifier(), metadata_info()}], % remaining links to follow
    depth :: non_neg_integer(),
    loaded = false
}).

-spec read_fn() -> db_read_fun().
read_fn() ->
    fun db_read/1.

-spec search(search_fun(), dby_identifier(), term(), [search_options()]) -> term() | {error, reason()}.
search(Fun, Acc, StartIdentifier, Options) ->
    search(read_fn(), Fun, Acc, StartIdentifier, Options).

-spec search(db_read_fun(), search_fun(), dby_identifier(), term(), [search_options()]) -> term() | {error, reason()}.
search(ReadFn, Fun, Acc, StartIdentifier, Options) ->
    % XXX need to catch badarg
    OptionsR = dby_options:options(Options),
    Traversal = OptionsR#options.traversal,
    MaxDepth = OptionsR#options.max_depth,
    TypeFn = typefn(OptionsR#options.type),
    ?DEBUG("Search: start(~s) options(~p,~p)",
            [StartIdentifier, OptionsR#options.traversal, OptionsR#options.type]),
    DiscoveryFn = discoveryfn(OptionsR#options.loop),
    Fn =
        fun() ->
            case dby_db:exists({identifier, StartIdentifier}) of
                true ->
                    do_search(Traversal, ReadFn, TypeFn, DiscoveryFn, MaxDepth,
                                                    Fun, StartIdentifier, Acc);
                false ->
                    Acc
            end
        end,
    dby_db:transaction(Fn).

do_search(depth, ReadFn, TypeFn, DiscoveryFn, MaxDepth, Fun, StartIdentifier, Acc) ->
    start_depth_search(ReadFn, MaxDepth, TypeFn, DiscoveryFn,
                                                Fun, StartIdentifier, Acc);
do_search(breadth, ReadFn, TypeFn, DiscoveryFn, MaxDepth, Fun, StartIdentifier, Acc) ->
    start_breadth_search(ReadFn, MaxDepth, TypeFn, DiscoveryFn,
                                                Fun, StartIdentifier, Acc).

%-------------------------------------------------------------------------------
% depth search
%-------------------------------------------------------------------------------

start_depth_search(ReadFn, MaxDepth, TypeFn, DiscoveryFn, Fun, StartIdentifier, Acc0) ->
    % seed the search with the starting identifier
    SearchStack = [
        #search{
            fn = Fun,
            identifier = StartIdentifier,
            path = [],
            depth = 0
        }
    ],
    depth_search(continue, ReadFn, MaxDepth, TypeFn, DiscoveryFn, SearchStack, Acc0).

depth_search(_, _, _, _, _, [], Acc) ->
    % we're done - ran out of links to follow
    Acc;
depth_search(stop, _, _, _, _, _, Acc) ->
    % search function says stop
    Acc;
depth_search(skip, ReadFn, MaxDepth, TypeFn, DiscoveryFn,
                                State0 = [#search{fn = Fun} | _], Acc) ->
    % Skip this identifier
    State1 = depth_search_next(continue, State0, TypeFn, DiscoveryFn, Fun),
    depth_search(continue, ReadFn, MaxDepth, TypeFn, DiscoveryFn, State1, Acc);
depth_search(continue, ReadFn, MaxDepth, TypeFn, DiscoveryFn,
                [#search{depth = Depth} | Rest], Acc) when Depth > MaxDepth ->
    % depth exceed maximum search depth - skip this identifier
    depth_search(continue, ReadFn, MaxDepth, TypeFn, DiscoveryFn, Rest, Acc);
depth_search(continue, ReadFn, MaxDepth, TypeFn, DiscoveryFn0,
        State0 = [Search0 = #search{identifier =  Identifier, from = From} |
                                                        RestState], Acc0) ->
    % discovered?
    case DiscoveryFn0(is, Identifier, From) of
        true ->
            % traverse the next link
            State1 = depth_search_next(continue, State0, TypeFn, DiscoveryFn0,
                                                        Search0#search.fn),
            depth_search(continue, ReadFn, MaxDepth, TypeFn, DiscoveryFn0,
                                                        State1, Acc0);
        false ->
            % apply the search function
            Search1 = read_identifier(ReadFn, Search0),
            DiscoveryFn1 = DiscoveryFn0(add, Identifier, From),
            {Control, Fun1, Acc1} = apply_fun(Search1#search.fn,
                                              Identifier,
                                              Search1#search.metadata,
                                              Search1#search.path,
                                              Acc0),
            % traverse the next link
            State1 = [Search1 | RestState],
            State2 = depth_search_next(Control, State1, TypeFn, DiscoveryFn1, Fun1),
            depth_search(Control, ReadFn, MaxDepth, TypeFn, DiscoveryFn1, State2, Acc1)
    end.

% process the next identifier in the search
depth_search_next(stop, SearchStack0, _, _, _) ->
    % search is stopping so need to need to do anything
    SearchStack0;
depth_search_next(skip, [_ | SearchStack0], _, _, _) ->
    % skipping this identifier - pop it off the stack
    SearchStack0;
depth_search_next(continue,
        [Search0 = #search{links = Links,
                           identifier = Identifier,
                           metadata = Metadata,
                           path = Path} | SearchStack0],
        TypeFn, DiscoveryFn, Fun1) ->
    % get the next link to traverse
    case first_not_discovered(Identifier, Links, TypeFn, DiscoveryFn) of
        [] ->
            % no more links to follow at this identifier.
            % pop the stack and continue.
            SearchStack0;
        [{NeighborIdentifier, LinkMetadata} | RestLinks] ->
            [
                % depth first search - push the neighbor onto the
                % stack so it is the next identifier processed.
                #search{
                    depth = Search0#search.depth + 1,
                    fn = Fun1,
                    identifier = NeighborIdentifier,
                    from = Identifier,
                    path = [{Identifier, Metadata, LinkMetadata} | Path]
                },
                % remove the link just traversed from the list of links
                % that still need to be examined
                Search0#search{links = RestLinks} | SearchStack0
            ]
    end.

first_not_discovered(Identifier, Links, TypeFn, DiscoveryFn) ->
    lists:dropwhile(
        fun({NeighborIdentifier, LinkMetadata}) ->
            (not TypeFn(LinkMetadata))
                orelse
            DiscoveryFn(is, NeighborIdentifier, Identifier)
        end, Links).

%-------------------------------------------------------------------------------
% breadth search
%-------------------------------------------------------------------------------

start_breadth_search(ReadFn, MaxDepth, TypeFn, DiscoveryFn,
                                            Fun, StartIdentifier, Acc0) ->
    Search = #search{
        fn = Fun,
        identifier = StartIdentifier,
        path = [],
        depth = 0
    },
    breadth_search(ReadFn, MaxDepth,
        queue:in(Search, queue:new()),
        TypeFn,
        DiscoveryFn(add, StartIdentifier, undefined),
        Acc0).

breadth_search(ReadFn, MaxDepth, Q0, TypeFn, Queued0, Acc0) ->
    case queue:len(Q0) of
        0 ->
            Acc0;
        _ ->
            {{value, Search}, Q1} = queue:out(Q0),
            Search1 = #search{
                identifier = Identifier,
                metadata = IdentifierMetadata,
                path = Path,
                fn = Fun
            } = read_identifier(ReadFn, Search),
            {Control, Fun1, Acc1} = apply_fun(Fun, Identifier,
                                    IdentifierMetadata, Path, Acc0),
            {Q2, Queued1} = queue_links(Control, MaxDepth,
                                            Fun1, Search1, Q1, TypeFn, Queued0),
            breadth_search(ReadFn, MaxDepth, Q2, TypeFn, Queued1, Acc1)
    end.

queue_links(stop, _, _, _, _, _, Queued) ->
    % empty queue to stop search
    {queue:new(), Queued};
queue_links(skip, _, _, _, Q, _, Queued) ->
    {Q, Queued};
queue_links(continue, MaxDepth, _, #search{depth = Depth}, Q, _, Queued)
                                                    when Depth >= MaxDepth ->
    {Q, Queued};
queue_links(_, _, Fun, #search{identifier = Identifier,
                               metadata = Metadata,
                               links = Links,
                               path = Path,
                               depth = Depth}, Q0, TypeFn, Queued0) ->
    lists:foldl(
        fun({NeighborIdentifier, LinkMetadata}, {Q, Queued}) ->
            case (not TypeFn(LinkMetadata)) orelse
                        Queued(is, NeighborIdentifier, Identifier) of
                true ->
                    {Q, Queued};
                false ->
                    Search = #search{
                        fn = Fun,
                        identifier = NeighborIdentifier,
                        from = Identifier,
                        path = [{Identifier, Metadata, LinkMetadata} | Path],
                        depth = Depth + 1
                    },
                    {queue:in(Search, Q),
                     Queued(add, NeighborIdentifier, Identifier)}
            end
        end, {Q0, Queued0}, Links).

%-------------------------------------------------------------------------------
% helper functions
%-------------------------------------------------------------------------------

% apply the search function.  Normalize the return result.
apply_fun(Fun, Identifier, IdentifierMetadata, Path, Acc) ->
    case Fun(Identifier, IdentifierMetadata, Path, Acc) of
        {Control, A} -> {Control, Fun, A};
        {Control, NewFun, A} -> {Control, NewFun, A}
    end.

read_identifier(_, Search = #search{loaded = true}) ->
    Search;
read_identifier(ReadFn, Search = #search{identifier = Identifier}) ->
    case ReadFn({identifier, Identifier}) of
        [R] ->
            Search#search{
                    metadata = R#identifier.metadata,
                    links = maps:to_list(R#identifier.links),
                    loaded = true
            };
        _ ->
            % XXX bad link; cleanup source identifier
            % for now, return an empty identifier record
            Search#search{
                    metadata = #{},
                    links = [],
                    loaded = true
            }
    end.

db_read(Key) ->
    dby_db:read(Key).

% Generate the function for loop detection.  The function takes
% three arguments: operation (add, is), the identifier, and
% the identifier's neighbor.
%
% 'add' returns an updated version of itself.
% "is" returns true/false indicating if the
% Identifier or Link is already discovered.
%
% none - no check is made.  'is' always returns false.
%
% identifier - 'add' remembers the identifier by updating
%   adding the identifier to the set.  Returns a new function
%   where the updated set is in the closure.
%
% link - 'add' remembers the link by adding the identifier and
%   its neighbor as a tuple, ordered both ways.  Returns a new
%   function where the updated set is in the closure.
discoveryfn(none) ->
    fun F(add, _, _) ->
            F;
        F(is, _, _) ->
            false
    end;
discoveryfn(identifier) ->
    Fn = fun F(add, Identifier, _, Discovered) ->
                gen_discover(F, sets:add_element(Identifier, Discovered));
             F(is, Identifier, _, Discovered) ->
                sets:is_element(Identifier, Discovered)
    end,
    gen_discover(Fn, sets:new());
discoveryfn(link) ->
    Fn = fun F(add, Identifier, NeighborIdentifier, Discovered) ->
                gen_discover(F,
                    sets:add_element({NeighborIdentifier, Identifier},
                        sets:add_element({Identifier, NeighborIdentifier},
                                                                Discovered)));
             F(is, Identifier, NeighborIdentifier, Discovered) ->
                sets:is_element({Identifier, NeighborIdentifier}, Discovered)
    end,
    gen_discover(Fn, sets:new()).

gen_discover(Fn, Discovered) ->
    fun(Op, Identifier, NeighborIdentifier) ->
        Fn(Op, Identifier, NeighborIdentifier, Discovered)
    end.

typefn(system) ->
    fun(#{system := _}) -> true;
       (_) -> false
    end;
typefn(user) ->
    fun(#{system := _}) -> false;
       (_) -> true
    end.
