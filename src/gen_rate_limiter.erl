-module(gen_rate_limiter).

-behaviour(gen_server).

%% API
-export([start_link/2, run/2]).
%% Gen Server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


%% This module defines a generic rate limiter (rl) intended to be used with APIs that
%% only allow a certain number of requests per timespan.

%% ===================================================================
%% Rate Limiter Callback Module
%% ===================================================================

%% The callback module has to define 4 function that define the behaviour of the
%% rl:

-callback init(Args :: term()) ->
    State :: term().

%% Called after startup. Args is the same as in
%% gen_rate_limiter:start_link(Module, Args) -> [See Gen Server start_link return]

-callback before_run(State :: term()) ->
    State :: term().

%% Called every time before Module:run is called. Runs in the rl process.

-callback run(Args :: term(), State :: term()) ->
    {Return :: term(), State :: term()}.

%% Called when gen_rate_limiter:run(Pid, Args) -> Return is called. Runs in
%% the caller's process, if this call fails the rl's old State will be kept.

-callback to_wait(State :: term()) ->
    {ToWait :: non_neg_integer(), State :: term()}.

%% Called every time after Module:run was called (even if it failed). ToWait
%% (measured in milliseconds) determines how long the rl will sleep before
%% allowing Module:run to be called again. Runs in the rl process.

%% ===================================================================
%% Gen Rate Limiter
%% ===================================================================

start_link(Module, Args) ->
    gen_server:start_link(?MODULE, {Module, Args}, []).

run(Pid, Args) ->
	{M, RLState} = gen_server:call(Pid, {request, self()}, infinity),
    {Return, NewRLState} = M:run(Args, RLState),
    Pid ! {ok, NewRLState},
    Return.

init({Module, Args}) ->
    {ok, {Module, Module:init(Args)}}.

handle_call({request, Pid}, From, {Module, RLState}) when is_pid(Pid), is_atom(Module) ->
    IntermediateRLState = Module:before_run(RLState),
    MRef = monitor(process, Pid),
    gen_server:reply(From, {Module, IntermediateRLState}),
    AfterRunRLState = receive
                          {ok, S} ->
                              demonitor(MRef, [flush]),
                              S;
                          {'DOWN', MRef, process, Pid, Reason} ->
                              io:format("Gen Rate Limiter: Run of ~p failed with reason ~p.~n",
                                        [Pid, Reason]),
                              IntermediateRLState
                      end,
    {ToWait, FinalRLState} = Module:to_wait(AfterRunRLState),
    if ToWait > 0 -> timer:sleep(ToWait);
       true -> ok end,
    {noreply, {Module, FinalRLState}};

handle_call(Msg, {Pid, _Tag} = _From, {Module, RLState} = State) ->
    io:format("Gen Rate Limiter ~p (callback module ~p): Received unexpected call ~p from ~p while in state ~p.",
              [self(), Module, Msg, Pid, RLState]),
    {noreply, State}.

handle_cast(Msg, {Module, RLState} = State) ->
    io:format("Gen Rate Limiter ~p (callback module ~p): Received unexpected cast ~p while in state ~p.",
              [self(), Module, Msg, RLState]),
    {noreply, State}.

handle_info(Msg, {Module, RLState} = State) ->
    io:format("Gen Rate Limiter ~p (callback module ~p): Received unexpected info ~p while in state ~p.",
              [self(), Module, Msg, RLState]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
