-module(dby_options).

-export([options/1,
         delta_default/2,
         delivery_default/1]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

options(Options) ->
    lists:foldl(
        fun(persistent, Record) ->
            Record#options{publish = persistent};
           (message, Record) ->
            Record#options{publish = message};
           (breadth, Record) ->
            Record#options{traversal = breadth};
           (depth, Record) ->
            Record#options{traversal = depth};
           (system, Record) ->
            Record#options{type = system};
           (user, Record) ->
            Record#options{type = user};
           ({max_depth, Depth}, Record) when Depth >= 0, is_integer(Depth) ->
            Record#options{max_depth = Depth};
           ({loop, none}, Record) ->
            Record#options{loop = none};
           ({loop, link}, Record) ->
            Record#options{loop = link};
           ({loop, identifier}, Record) ->
            Record#options{loop = identifier};
           ({delta, DFun}, Record) when is_function(DFun) ->
            Record#options{delta_fun = DFun};
           ({delivery, SFun}, Record) when is_function(SFun) ->
            Record#options{delivery_fun = SFun};
           (BadArg, _) ->
            throw({badarg, BadArg})
        end, #options{}, Options).

% Default delta function for subscriptions.  Return the new value.
delta_default(_, New) ->
    {delta, New}.

% Default delivery function for subscriptions.  Do nothing.
delivery_default(_) ->
    ok.
