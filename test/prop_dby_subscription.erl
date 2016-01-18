-module(prop_dby_subscription).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(APPS, [dobby, dobby_clib]).
-define(ID, <<"id">>).
-define(PUB, <<"pub">>).
-define(MSG, <<"alice">>).
-define(TYPE1, <<"type1">>).
-define(TYPE2, <<"type2">>).
-define(DELAYS_MS, [100, 200, 300, 400, 500]).


%%%===================================================================
%%% Properties
%%%===================================================================

prop_subsriptions_work() ->
    ?TRAPEXIT(
       ?FORALL({PublishersCnt, Delays},
               publishers_cnt_with_delays_gen(),
               begin
                   setup(),
                   publish_id(),
                   subscribe(?TYPE1, delivery_fn()),
                   spawn_link_publishers(?TYPE1, ?TYPE2, Delays),
                   send_msg_to_publishers(PublishersCnt, ?TYPE2),
                   Msgs = collect_messages_from_publishers(Delays),
                   collect(
                     PublishersCnt,
                     ok =:= ?assertEqual(PublishersCnt,
                                         length(Msgs)))
               end)).

%%%===================================================================
%%% Generators & Setup/Teardown
%%%===================================================================

publishers_cnt_with_delays_gen() ->
    ?LET(PubsCnt,
         integer(1,10),
         {PubsCnt, [oneof(?DELAYS_MS) || _ <- lists:seq(1, PubsCnt)]}).

setup() ->
    teardown(),
    application:set_env(lager, handlers,
                        [{lager_console_backend, error}]),
    [{ok, _} = application:ensure_all_started(A) || A <- ?APPS].

teardown() ->
    catch dby_db:clear(),
    [application:stop(A) || A <- lists:reverse(?APPS)].

%%%===================================================================
%%% Helpers
%%%===================================================================

delivery_fn() ->
    Pid = self(),
    fun(Msg) -> Pid ! Msg, ok end.


publish_id() ->
    dby:publish(?PUB, ?ID, [persistent]).

subscribe(Type, DeliveryFn) ->
    SearchFn = fun(_, Metadata, [], Acc) ->
                       case maps:get(Type, Metadata, undefined) of
                           #{value := Message} ->
                               {continue, Message};
                           _ ->
                               {continue, Acc}
                       end
               end,
    Opts = [breadth, {max_depth, 0}, message, {delivery, DeliveryFn}],
    {ok, [], SubId} = dby:subscribe(SearchFn, [], ?ID, Opts),
    SubId.

unsubscribe(SubId) ->
    dby:unsubscribe(SubId).

spawn_link_publishers(SendType, SubscribeType, Delays) ->
    ConfirmSubTo = self(),
    [spawn_link(
       fun() ->
               Id = subscribe(SubscribeType, delivery_fn()),
               ConfirmSubTo ! {confirm_sub, self()},
               timer:sleep(D),
               publish_msg(SendType),
               receive
                   ?MSG ->
                       ok
               after 100 ->
                       unsubscribe(Id),
                       error(no_message_in_publisher)
               end,
               unsubscribe(Id)
       end) || D <- Delays].

send_msg_to_publishers(PublishersCnt, SendType) ->
    collect_sub_confirmations(PublishersCnt),
    publish_msg(SendType).

collect_sub_confirmations(0) ->
    ok;
collect_sub_confirmations(Cnt) ->
    receive
        {confirm_sub, _Pid} ->
            collect_sub_confirmations(Cnt - 1)
    after 100 ->
            error(no_sub_confirmation)
    end.

publish_msg(Type) ->
    Msg = {Type, ?MSG},
    dby:publish(?PUB, {?ID, [Msg]}, []).

collect_messages_from_publishers(Delays) ->
    Max = lists:max(Delays),
    collect_messages_from_publishers(length(Delays), Max, []).

collect_messages_from_publishers(0, _, Acc) ->
    Acc;
collect_messages_from_publishers(Num, MaxDealy, Acc) ->
    receive
        ?MSG = M ->
            collect_messages_from_publishers(Num - 1, MaxDealy, [M | Acc])
    after MaxDealy * 2 ->
            collect_messages_from_publishers(Num - 1 , MaxDealy, Acc)
    end.



