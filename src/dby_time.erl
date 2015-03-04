-module(dby_time).

-export([timestamp/0]).

timestamp() ->
    iso8601:format(os:timestamp()).
