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

-module(couch_replicator_monitor).
-behaviour(gen_server).
-vsn(1).


-export([
    start_link/0,
    scan_now/0
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-export([
    scan/0
]).


-record(st, {
    scan_pid,
    scan_requested = false
}).


-define(DOC_TO_REP, couch_rep_doc_id_to_rep_id).
-define(REP_TO_STATE, couch_rep_id_to_rep_state).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


scan_now() ->
    gen_server:cast(?MODULE, maybe_start_scan).


init(_) ->
    random:seed(os:timestamp()),
    erlang:send_after(monitor_interval(), self(), start_scan),
    {ok, #st{}}.


terminate(_Reason, _St) ->
    ok.


handle_call(Msg, _From, St) ->
    {reply, {ignored, Msg}, St}.


handle_cast(maybe_start_scan, St) ->
    case St#st.scan_pid of
        undefined ->
            {noreply, start_scan(St)};
        _Else ->
            {noreply, St}
    end;

handle_cast(_Msg, St) ->
    {noreply, St}.


handle_info(start_scan, St) ->
    case St#st.scan_pid of
        undefined ->
            {noreply, start_scan(St)};
        _Else ->
            {noreply, St#st{scan_requested = true}}
    end;

handle_info({'DOWN', _, _, Pid, Reason}, #st{scan_pid = Pid} = St) ->
    if Reason == normal -> ok; true ->
        twig:log(error, "%s scan pid died unexpectedly: ~p", [?MODULE, Reason])
    end,
    case St#st.scan_requested of
        true ->
            {noreply, start_scan(St)};
        false ->
            {noreply, St#st{scan_pid = undefined}}
    end;

handle_info(_Msg, St) ->
    {noreply, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


scan() ->
    

enum_dbs() ->
    DbName = config:get("mem3", "shard_db", "dbs"),
    {ok, Db} = couch_db:open_int(DbName, []),
    FoldFun = fun fold_dbs/3,
    try
        {ok, _, _} = couch_db:enum_docs(Db, FoldFun, nil, []),
    after
        couch_db:close(Db)
    end.


fold_dbs(#full_doc_info{id = <<"_design/", _/binary>>}, _, Acc) ->
    {ok, Acc};
fold_dbs(#full_doc_info{deleted = true}, _, Acc) ->
    {ok, Acc};
fold_dbs(#full_doc_info{id = Id} = FDI, _, Acc) ->
    case is_replicator_db(Id) of
        true ->
            Shards = mem3:local_shards(Id),
            lists:foreach(fun(Shard) ->
                enum_shard(Shard)
            end);
        false ->
            ok
    end,
    {ok, Acc}.


enum_shard(#shard{} = Shard) ->
    enum_shard(Shard#shard.name);

enum_shard(Shard) when is_binary(Shard) ->
    {ok, Db} = couch_db:open_int(Shard, []),
    try
        FoldFun = fun fold_shard/3,
        {ok, _, _} = couch_db:enum_docs(Db, FoldFun, Shard, [])
    after
        couch_db:close(Db)
    end.


fold_shard(#full_doc_info{id = <<"_design/", _/binary>>}, _, ShardName) ->
    {ok, ShardName};
fold_shard(#full_doc_info{deleted = true}, _, ShardName) ->
    {ok, ShardName};
fold_shard(#full_doc_info{id = DocId} = FDI, _, ShardName) ->
    case couch_replicator_manager:owner(ShardName, DocId) of
        true ->
            check_replication_exists(ShardName, DocId);
        false ->
            ok
    end,
    {ok, ShardName}.


check_replication_exists(ShardName, DocId) ->
    case ets:lookup(?DOC_TO_REP, {ShardName, DocId}) of
        [{{ShardName, DocId}, RepId}] ->
            case ets:lookup(?REP_TO_STATE, RepId) of
                [{RepId, _}] ->
                    ok;
                [] ->
                    Args = [?MODULE, ShardName, DocId],
                    twig:log(error, "~s missing rep state for ~s/~s", Args)
            end;
        [] ->
            Args = [?MODULE, ShardName, DocId],
            twig:log(error, "~s missing rep id for ~s/~s", Args)
    end.


start_scan(St) ->
    {Pid, _Ref} = erlang:spawn_monitor(?MODULE, scan, []),
    St#st{
        scan_pid = Pid,
        scan_requested = false
    }.


monitor_interval() ->
    Base = config:get_integer("couch_replicator", "monitor_interval", 3600),
    Splay = round(length([node() | nodes()]) * 60 * random:uniform()),
    (Base + Splay) * 1000.
