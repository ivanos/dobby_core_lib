-module(dby_config).

-export([getconfig/1]).

getconfig(Var) ->
    {ok, Val} = application:get_env(dobby, Var),
    Val.
