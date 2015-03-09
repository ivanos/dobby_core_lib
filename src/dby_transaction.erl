-module(dby_transaction).

-behaviour(gen_server).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").
-include("dby_logger.hrl").

-define(STATE, dby_transaction_state).

-record(?STATE, {
    subscriptions = sets:new()
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,
         new/0,
         publish/2,
         commit/1,
         abort/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

-spec new() -> pid().
new() ->
    dby_transaction_sup:new_transaction().

% Add a subscription to the list of subscriptions touched during
% the transaction.
-spec publish(pid(), subscription_id()) -> ok.
publish(Pid, SubscriptionId) ->
    gen_server:call(Pid, {add_subscription, SubscriptionId}).

-spec commit(pid()) -> ok.
commit(Pid) ->
    gen_server:call(Pid, commit).

-spec abort(pid()) -> ok.
abort(Pid) ->
    gen_server:call(Pid, abort).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    {ok, #?STATE{}}.

handle_call({add_subscription, SubscriptionId}, _From,
                            State0 = #?STATE{subscriptions = Subscriptions}) ->
    State1 = State0#?STATE{
            subscriptions = sets:add_element(SubscriptionId, Subscriptions)},
    {reply, ok, State1};
handle_call(commit, _From, State0 = #?STATE{subscriptions = Subscriptions}) ->
    lists:foreach(
        fun(SubscriptionId) ->
            run(fun() -> dby_subscription:publish(SubscriptionId) end)
        end, sets:to_list(Subscriptions)),
    {stop, normal, ok, State0};
handle_call(abort, _From, State0) ->
    {stop, normal, ok, State0};
handle_call(Request, _From, _State) ->
    error({not_imeplemnted, call, Request}).

handle_cast(Msg, _State) ->
    error({not_imeplemnted, cast, Msg}).

handle_info(Info, _State) ->
    error({not_imeplemnted, info, Info}).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

run(Job) ->
    Fn = fun() ->
        try
            Job()
        catch
            Type:Reason ->
                ?ERROR("Subscription Failure: ~p:~p", [Type, Reason])
        end
    end,
    spawn(Fn).
