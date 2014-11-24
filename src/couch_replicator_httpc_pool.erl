% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_replicator_httpc_pool).
-behaviour(gen_server).
-vsn(1).

% public API
-export([start_link/2, stop/1]).
-export([get_worker/1, release_worker/2]).

% gen_server API
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include_lib("couch/include/couch_db.hrl").

-import(couch_util, [
    get_value/2
]).

-record(state, {
    url,
    limit,                  % max # of workers allowed
    free = [],              % free workers (connections)
    busy = [],              % busy workers (connections)
    waiting = queue:new(),  % blocked clients waiting for a worker
    callers = []            % clients who've been given a worker
}).


start_link(Url, Options) ->
    gen_server:start_link(?MODULE, {Url, Options}, []).

stop(Pool) ->
    ok = gen_server:call(Pool, stop, infinity).


get_worker(Pool) ->
    {ok, _Worker} = gen_server:call(Pool, get_worker, infinity).


release_worker(Pool, Worker) ->
    ok = gen_server:cast(Pool, {release_worker, Worker}).


init({Url, Options}) ->
    process_flag(trap_exit, true),
    State = #state{
        url = Url,
        limit = get_value(max_connections, Options)
    },
    {ok, State}.


handle_call(get_worker, From, State0) ->
    State1 = State0#state{
        waiting = queue:in(From, State0#state.waiting)
    },
    State2 = maybe_supply_worker(State1),
    {noreply, State2};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_cast({release_worker, Worker}, State0) ->
    State1 = release_worker_int(Worker, State0),
    State2 = maybe_supply_worker(State1),
    {noreply, State2}.


handle_info({'EXIT', Worker, _Reason}, State0) ->
    State1 = release_worker_int(Worker, State0),
    State2 = maybe_supply_worker(State1),
    {noreply, State2};

handle_info({'DOWN', Ref, process, _, _}, #state{callers = Callers} = State0) ->
    case lists:keysearch(Ref, 2, Callers) of
        {value, {Worker, Ref}} ->
            State1 = release_worker_int(Worker, State0),
            State2 = maybe_supply_worker(State1),
            {noreply, State2};
        false ->
            {noreply, State0}
    end.

code_change(_OldVsn, #state{}=State, _Extra) ->
    {ok, State}.


terminate(_Reason, State) ->
    lists:foreach(fun ibrowse_http_client:stop/1, State#state.free),
    lists:foreach(fun ibrowse_http_client:stop/1, State#state.busy).


release_worker_int(Worker, State0) ->
    #state{
        callers = Callers,
        busy = Busy
    } = State0,
    State0#state{
        callers = demonitor_client(Callers, Worker),
        busy = Busy -- [Worker]
    }.


maybe_supply_worker(State) ->
    #state{
        url = Url,
        waiting = Waiting0,
        callers = Callers,
        busy = Busy,
        free = Free0,
        limit = Limit
    } = State,
    case queue:out(Waiting0) of
        {empty, Waiting0} ->
            State;
        {{value, From}, Waiting1} ->
            case length(Busy) >= Limit of
                true ->
                    State;
                false ->
                    {Worker, Free2} = case Free0 of
                        [] ->
                            {ok, W} = ibrowse:spawn_link_worker_process(Url),
                            {W, Free0};
                        [W|Free1] ->
                            {W, Free1}
                    end,
                    gen_server:reply(From, {ok, Worker}),
                    State#state{
                        callers = monitor_client(Callers, Worker, From),
                        waiting = Waiting1,
                        busy = [Worker|Busy],
                        free = Free2
                    }
            end
    end.


monitor_client(Callers, Worker, {ClientPid, _}) ->
    [{Worker, erlang:monitor(process, ClientPid)} | Callers].

demonitor_client(Callers, Worker) ->
    case lists:keysearch(Worker, 1, Callers) of
        {value, {Worker, MonRef}} ->
            erlang:demonitor(MonRef, [flush]),
            lists:keydelete(Worker, 1, Callers);
        false ->
            Callers
    end.
