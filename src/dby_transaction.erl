-module(dby_transaction).

-behaviour(gen_server).

-include_lib("dobby_clib/include/dobby.hrl").
-include("dobby.hrl").
-include("dby_logger.hrl").

-define(STATE, dby_transaction_state).

-record(?STATE, {
    identifiers = dict:new()
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0,
         new/0,
         publish/2,
         delete/2,
         commit/2,
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

% Add an identifier to the transaction.
-spec publish(pid(), #identifier{}) -> ok.
publish(Pid, IdentifierR) ->
    gen_server:call(Pid, {publish, IdentifierR}).

% Mark identifer for delete
-spec delete(pid(), #identifier{}) -> ok.
delete(Pid, IdentifierR) ->
    gen_server:call(Pid, {delete, IdentifierR}).

-spec commit(pid(), publish_type()) -> ok.
commit(Pid, Publish) ->
    gen_server:call(Pid, {commit, Publish}).

-spec abort(pid()) -> ok.
abort(Pid) ->
    gen_server:call(Pid, abort).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    {ok, #?STATE{}}.

handle_call({publish, IdentifierR}, _From,
                            State0 = #?STATE{identifiers = Identifiers}) ->
    Identifier = IdentifierR#identifier.id,
    State1 = State0#?STATE{
            identifiers = dict:store(Identifier, IdentifierR, Identifiers)},
    {reply, ok, State1};
handle_call({delete, IdentifierR}, _From,
                            State0 = #?STATE{identifiers = Identifiers}) ->
    % XXX what if the deleted identifier is the starting identifier?
    Identifier = IdentifierR#identifier.id,
    State1 = State0#?STATE{
            identifiers = dict:store(Identifier, IdentifierR, Identifiers)},
    {reply, ok, State1};
handle_call({commit, Publish}, _From, State0 = #?STATE{identifiers = Identifiers}) ->
    notify_subscriptions(Identifiers, Publish,
                                        publish_readfn(Identifiers, Publish)),
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

publish_readfn(_, persistent) ->
    fun(Key) ->
        dby_db:read(Key)
    end;
publish_readfn(Identifiers, message) ->
    fun (Key = {identifier, Identifier}) ->
        case dict:find(Identifier, Identifiers) of
            error ->
                dby_db:read(Key);
            {ok, IdentifierR} ->
                [IdentifierR]
        end
    end.

notify_subscriptions(Identifiers, Publish, ReadFn) ->
    % make list of subscription ids
    Subscriptions = dict:fold(
        fun(_, #identifier{links = Links}, Acc) ->
            subscription_ids(Links, Acc)
        end, sets:new(), Identifiers),
    lists:foreach(
        fun(SubscriptionId) ->
            run(fun() -> dby_subscription:publish(SubscriptionId, Publish, ReadFn) end)
        end, sets:to_list(Subscriptions)).

subscription_ids(Links, Acc0) ->
    maps:fold(
        fun(SubscriptionId, #{system := subscription}, Acc) ->
            sets:add_element(SubscriptionId, Acc);
           (_, _, Acc) ->
            Acc
        end, Acc0, Links).

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
