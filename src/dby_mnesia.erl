-module(dby_mnesia).

-export([start/0]).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

% start mnesia and create the on disk schema and tables if
% they don't already exist.
start() ->
    ok = mnesia:start(),
    schema(),
    tables().

schema() ->
    case disc_schema() of
        true ->
            ok;
        false ->
            % schema isn't on disk.  assume this is an initial setup
            % and move the schema to disk.
            {atomic, ok} = mnesia:change_table_copy_type(
                                                schema, node(), disc_copies)
    end,
    ok.

% is the schema on disk?
disc_schema() ->
    case mnesia:table_info(schema, disc_copies) of
        [] -> false;
        _ -> true
    end.

% create tables
tables() ->
    Tables = mnesia:system_info(tables),
    lists:foreach(fun({Name, TabDef}) ->
        case lists:member(Name, Tables) of
            true ->
                % table already exists
                ok;
            false ->
                % create the table
                {atomic, ok} = mnesia:create_table(Name, TabDef)
        end
    end, tabledefs()),
    ok.

tabledefs() ->
    % [{TableName, TableDef}]
    [
        {identifier, [{attributes, record_info(fields, identifier)},
                      {disc_copies, [node()]},
                      {type, set}]},
        {subscription, [{attributes, record_info(fields, subscription)},
                      {disc_copies, [node()]},
                      {type, set}]},
        {subscriber, [{attributes, record_info(fields, subscriber)},
                      {disc_copies, [node()]},
                      {type, bag}]}
    ].
