-module(dby_search).

-export([search/4]).

-include_lib("dobby/include/dobby.hrl").

-record(search, {
    fn :: fun(),
    identifier :: identifier(),
    metadata :: jsonable(),
    linkmetadata :: jsonable(),         % metadata on link to this identifier
    links :: [{identifier(), jsonable()}], % remaining links to follow
    depth :: non_neg_integer(),
    loaded = false
}).

-spec search(search_fun(), identifier(), term(), search_options()) -> term() | {error, reason()}.
search(Fun, StartIdentifier, Acc, Options) ->
    % XXX need to catch badarg
    OptionsR = dby_options:options(Options),
    Traversal = OptionsR#options.traversal,
    MaxDepth = OptionsR#options.max_depth,
    Fn =
        fun() ->
            do_search(Traversal, MaxDepth, Fun, StartIdentifier, Acc)
        end,
    dby_db:transaction(Fn).

do_search(depth, MaxDepth, Fun, StartIdentifier, Acc) ->
    start_depth_search(MaxDepth, Fun, StartIdentifier, Acc);
do_search(breadth, MaxDepth, Fun, StartIdentifier, Acc) ->
    [].

start_depth_search(MaxDepth, Fun, StartIdentifier, Acc0) ->
    % seed the search with the starting identifier
    SearchStack = [
        #search{
            fn = Fun,
            identifier = StartIdentifier,
            linkmetadata = undefined,
            depth = 0
        }
    ],
    depth_search(continue, MaxDepth, sets:new(), SearchStack, Acc0).

depth_search(_, _, _, [], Acc) ->
    % we're done - ran out of links to follow
    lists:reverse(Acc);
depth_search(stop, _, _, _, Acc) ->
    % search function says stop
    lists:reverse(Acc);
depth_search(skip, MaxDepth, Discovered, [_ | Rest], Acc) ->
    % do not traverse any links from the parent's identifier.
    % depth_search_next pushes the next identifier onto the stack 
    % before checking Control.
    depth_search(continue, MaxDepth, Discovered, Rest, Acc);
depth_search(continue, MaxDepth, Discovered, [#search{depth = Depth} | Rest], Acc) when Depth > MaxDepth ->
    % depth exceed maximum search depth - skip this identifier
    depth_search(continue, MaxDepth, Discovered, Rest, Acc);
depth_search(continue, MaxDepth, Discovered0, State0 = [Search0 = #search{identifier =  Identifier} | RestState], Acc0) ->
    % discovered?
    case sets:is_element(Identifier, Discovered0) of
        true ->
            % traverse the next link
            State1 = depth_search_next(State0, Discovered0, Search0#search.fn),
            depth_search(continue, MaxDepth, Discovered0, State1, Acc0);
        false ->
            % apply the search function
            Discovered1 = sets:add_element(Identifier, Discovered0),
            Search1 = read_identifier(Search0),
            {Control, Fun1, Acc1} = apply_fun(Search1#search.fn, Identifier, Search1#search.metadata, Search1#search.linkmetadata, Acc0),
            % traverse the next link
            State1 = [Search1 | RestState],
            State2 = depth_search_next(State1, Discovered1, Fun1),
            depth_search(Control, MaxDepth, Discovered1, State2, Acc1)
    end.

% process the next identifier in the search
depth_search_next([Search0 | SearchStack0], Discovered, Fun1) ->
    % get the next link to traverse
    case first_not_discovered(Search0#search.links, Discovered) of
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
                    linkmetadata = LinkMetadata
                },
                % remove the link just traversed from the list of links
                % that still need to be examined
                Search0#search{links = RestLinks} | SearchStack0
            ]
    end.

% apply the search function.  Normalize the return result.
apply_fun(Fun, Identifier, IdentifierMetadata, LinkMetadata, Acc) ->
    case Fun(Identifier, IdentifierMetadata, LinkMetadata, Acc) of
        {Control, A} -> {Control, Fun, A};
        {Control, NewFun, A} -> {Control, NewFun, A}
    end.

read_identifier(Search = #search{loaded = true}) ->
    Search;
read_identifier(Search = #search{identifier = Identifier}) ->
    case dby_db:read({identifier, Identifier}) of
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
                    metadata = null,
                    links = [],
                    loaded = true
            }
    end.

first_not_discovered(Links, Discovered) ->
    lists:dropwhile(
        fun(Link = {Identifier, _}) ->
            sets:is_element(Identifier, Discovered)
        end, Links).
