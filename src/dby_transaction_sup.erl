-module(dby_transaction_sup).

-behavior(supervisor).

-export([start_link/0, init/1]).
-export([new_transaction/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    C = dby_transaction,
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {ok, {SupFlags,
        [{transaction, {C, start_link, []}, temporary, 1000, worker, [C]}]}}.

new_transaction() ->
    {ok, Pid} = supervisor:start_child(?MODULE, []),
    Pid.
