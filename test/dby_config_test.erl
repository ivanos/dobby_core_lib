-module(dby_config_test).

-include_lib("eunit/include/eunit.hrl").

dby_config_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
        {"getconfig", fun getconfig/0}
     ]
    }.

-define(PARAM, test_param).
-define(VALUE, test_value).

setup() ->
    ok = application:load(dobby),
    ok = application:set_env(dobby, ?PARAM, ?VALUE).

cleanup(ok) ->
    ok = application:unload(dobby).

getconfig() ->
    ?assertEqual(?VALUE, dby_config:getconfig(?PARAM)).
