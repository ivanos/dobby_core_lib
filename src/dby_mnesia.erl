-module(dby_mnesia).

-export([start/0, clear/0]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

% start mnesia and create the on disk schema and tables if
% they don't already exist.
start() ->
    erl_mnesia:tables(tabledefs()).

tabledefs() ->
    % [{TableName, TableDef}]
    [
        {identifier, [{attributes, record_info(fields, identifier)},
                      {disc_copies, [node()]},
                      {type, set}]}
    ].

clear() ->
    lists:foreach(
        fun({Name, _}) ->
            {atomic, ok} = mnesia:clear_table(Name)
        end,
    tabledefs()).
