-module(dby_store).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").

-export([read_identifier/1]).

read_identifier(Identifier) ->
    case dby_db:read({identifier, Identifier}) of
        [R] -> R;
        _ -> #identifier{id = Identifier}
    end.
