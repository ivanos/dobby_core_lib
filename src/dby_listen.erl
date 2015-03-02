-module(dby_listen).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

-define(STATE, dby_listen_state).
-record(?STATE, {
}).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    GlobalName = dby_config:getconfig(global_name),
    gen_server:start_link({global, GlobalName}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    {ok, #?STATE{}}.

handle_call({dby_publish, [Data, Options]}, From, State) ->
    run(From, fun() -> dby_publish:publish(Data, Options) end),
    {noreply, State};
handle_call({dby_search, [Fun, Acc, StartIdentifier, Options]}, From, State) ->
    run(From,
        fun() -> dby_search:search(Fun, Acc, StartIdentifier, Options) end),
    {noreply, State};
handle_call({dby_subscribe,
                        [Fun, Acc, StartIdentifier, Options]}, From, State) ->
    run(From,
        fun() ->
            dby_subscription:subscribe(Fun, Acc, StartIdentifier, Options)
        end
    ),
    {noreply, State};
handle_call({dby_unsubscribe, SubscriptionId}, From, State) ->
    run(From,
        fun() ->
            dby_subscription:delete(SubscriptionId)
        end
    ),
    {noreply, State};
handle_call(Request, _From, _State) ->
    error({not_implemented, call, Request}).

handle_cast(Msg, _State) ->
    error({not_implemented, cast, Msg}).

handle_info(Info, _State) ->
    error({not_implemented, info, Info}).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% run a gen_server:handle_call job asynchronously
run(From, Job) ->
    Fn = fun() ->
        Reply = try
            Job()
        catch
            Type:Reason ->
                {error, {Type, Reason}}
        end,
        gen_server:reply(From, Reply)
    end,
    spawn(Fn).
