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

-module(couch_replicator_utils).

-export([parse_rep_doc/2]).
-export([open_db/1, close_db/1]).
-export([start_db_compaction_notifier/2, stop_db_compaction_notifier/1]).
-export([replication_id/2]).
-export([sum_stats/2, is_deleted/1]).
-export([mp_parse_doc/2]).
-export([maybe_upgrade_wait/1]).

-export([handle_db_event/3]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator_api_wrap.hrl").
-include("couch_replicator.hrl").
-include_lib("ibrowse/include/ibrowse.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3
]).


parse_rep_doc({Props}, UserCtx) ->
    ProxyParams = parse_proxy_params(get_value(<<"proxy">>, Props, <<>>)),
    Options = make_options(Props),
    case get_value(cancel, Options, false) andalso
        (get_value(id, Options, nil) =/= nil) of
    true ->
        {ok, #rep{options = Options, user_ctx = UserCtx}};
    false ->
        Source = parse_rep_db(get_value(<<"source">>, Props), ProxyParams, Options),
        Target = parse_rep_db(get_value(<<"target">>, Props), ProxyParams, Options),
        Rep = #rep{
            source = Source,
            target = Target,
            options = Options,
            user_ctx = UserCtx,
            doc_id = get_value(<<"_id">>, Props, null)
        },
        {ok, Rep#rep{id = replication_id(Rep)}}
    end.


replication_id(#rep{options = Options} = Rep) ->
    BaseId = replication_id(Rep, ?REP_ID_VERSION),
    {BaseId, maybe_append_options([continuous, create_target], Options)}.


% Versioned clauses for generating replication IDs.
% If a change is made to how replications are identified,
% please add a new clause and increase ?REP_ID_VERSION.

replication_id(#rep{user_ctx = UserCtx} = Rep, 4) ->
    UUID = get_couch_server_uuid(),
    SrcInfo = get_v4_endpoint(UserCtx, Rep#rep.source),
    TgtInfo = get_v4_endpoint(UserCtx, Rep#rep.target),
    maybe_append_filters([UUID, SrcInfo, TgtInfo], Rep);

replication_id(#rep{user_ctx = UserCtx} = Rep, 3) ->
    UUID = get_couch_server_uuid(),
    Src = get_rep_endpoint(UserCtx, Rep#rep.source),
    Tgt = get_rep_endpoint(UserCtx, Rep#rep.target),
    maybe_append_filters([UUID, Src, Tgt], Rep);

replication_id(#rep{user_ctx = UserCtx} = Rep, 2) ->
    {ok, HostName} = inet:gethostname(),
    Port = case (catch mochiweb_socket_server:get(couch_httpd, port)) of
    P when is_number(P) ->
        P;
    _ ->
        % On restart we might be called before the couch_httpd process is
        % started.
        % TODO: we might be under an SSL socket server only, or both under
        % SSL and a non-SSL socket.
        % ... mochiweb_socket_server:get(https, port)
        list_to_integer(config:get("httpd", "port", "5984"))
    end,
    Src = get_rep_endpoint(UserCtx, Rep#rep.source),
    Tgt = get_rep_endpoint(UserCtx, Rep#rep.target),
    maybe_append_filters([HostName, Port, Src, Tgt], Rep);

replication_id(#rep{user_ctx = UserCtx} = Rep, 1) ->
    {ok, HostName} = inet:gethostname(),
    Src = get_rep_endpoint(UserCtx, Rep#rep.source),
    Tgt = get_rep_endpoint(UserCtx, Rep#rep.target),
    maybe_append_filters([HostName, Src, Tgt], Rep).


maybe_append_filters(Base,
        #rep{source = Source, user_ctx = UserCtx, options = Options}) ->
    Base2 = Base ++
        case get_value(filter, Options) of
        undefined ->
            case get_value(doc_ids, Options) of
            undefined ->
                [];
            DocIds ->
                [DocIds]
            end;
        Filter ->
            [filter_code(Filter, Source, UserCtx),
                get_value(query_params, Options, {[]})]
        end,
    couch_util:to_hex(couch_util:md5(term_to_binary(Base2))).


filter_code(Filter, Source, UserCtx) ->
    {DDocName, FilterName} =
    case re:run(Filter, "(.*?)/(.*)", [{capture, [1, 2], binary}]) of
    {match, [DDocName0, FilterName0]} ->
        {DDocName0, FilterName0};
    _ ->
        throw({error, <<"Invalid filter. Must match `ddocname/filtername`.">>})
    end,
    Db = case (catch couch_replicator_api_wrap:db_open(Source, [{user_ctx, UserCtx}])) of
    {ok, Db0} ->
        Db0;
    DbError ->
        DbErrorMsg = io_lib:format("Could not open source database `~s`: ~s",
           [couch_replicator_api_wrap:db_uri(Source), couch_util:to_binary(DbError)]),
        throw({error, iolist_to_binary(DbErrorMsg)})
    end,
    try
        Body = case (catch couch_replicator_api_wrap:open_doc(
            Db, <<"_design/", DDocName/binary>>, [ejson_body])) of
        {ok, #doc{body = Body0}} ->
            Body0;
        DocError ->
            DocErrorMsg = io_lib:format(
                "Couldn't open document `_design/~s` from source "
                "database `~s`: ~s", [DDocName, couch_replicator_api_wrap:db_uri(Source),
                    couch_util:to_binary(DocError)]),
            throw({error, iolist_to_binary(DocErrorMsg)})
        end,
        Code = couch_util:get_nested_json_value(
            Body, [<<"filters">>, FilterName]),
        re:replace(Code, [$^, "\s*(.*?)\s*", $$], "\\1", [{return, binary}])
    after
        couch_replicator_api_wrap:db_close(Db)
    end.


maybe_append_options(Options, RepOptions) ->
    lists:foldl(fun(Option, Acc) ->
        Acc ++
        case get_value(Option, RepOptions, false) of
        true ->
            "+" ++ atom_to_list(Option);
        false ->
            ""
        end
    end, [], Options).


get_rep_endpoint(_UserCtx, #httpdb{url=Url, headers=Headers, oauth=OAuth}) ->
    DefaultHeaders = (#httpdb{})#httpdb.headers,
    case OAuth of
    nil ->
        {remote, Url, Headers -- DefaultHeaders};
    #oauth{} ->
        {remote, Url, Headers -- DefaultHeaders, OAuth}
    end;
get_rep_endpoint(UserCtx, <<DbName/binary>>) ->
    {local, DbName, UserCtx}.


parse_rep_db({Props}, ProxyParams, Options) ->
    Url = maybe_add_trailing_slash(get_value(<<"url">>, Props)),
    {AuthProps} = get_value(<<"auth">>, Props, {[]}),
    {BinHeaders} = get_value(<<"headers">>, Props, {[]}),
    Headers = lists:ukeysort(1, [{?b2l(K), ?b2l(V)} || {K, V} <- BinHeaders]),
    DefaultHeaders = (#httpdb{})#httpdb.headers,
    OAuth = case get_value(<<"oauth">>, AuthProps) of
    undefined ->
        nil;
    {OauthProps} ->
        #oauth{
            consumer_key = ?b2l(get_value(<<"consumer_key">>, OauthProps)),
            token = ?b2l(get_value(<<"token">>, OauthProps)),
            token_secret = ?b2l(get_value(<<"token_secret">>, OauthProps)),
            consumer_secret = ?b2l(get_value(<<"consumer_secret">>, OauthProps)),
            signature_method =
                case get_value(<<"signature_method">>, OauthProps) of
                undefined ->        hmac_sha1;
                <<"PLAINTEXT">> ->  plaintext;
                <<"HMAC-SHA1">> ->  hmac_sha1;
                <<"RSA-SHA1">> ->   rsa_sha1
                end
        }
    end,
    #httpdb{
        url = Url,
        oauth = OAuth,
        headers = lists:ukeymerge(1, Headers, DefaultHeaders),
        ibrowse_options = lists:keysort(1,
            [{socket_options, get_value(socket_options, Options)} |
                ProxyParams ++ ssl_params(Url)]),
        timeout = get_value(connection_timeout, Options),
        http_connections = get_value(http_connections, Options),
        retries = get_value(retries, Options)
    };
parse_rep_db(<<"http://", _/binary>> = Url, ProxyParams, Options) ->
    parse_rep_db({[{<<"url">>, Url}]}, ProxyParams, Options);
parse_rep_db(<<"https://", _/binary>> = Url, ProxyParams, Options) ->
    parse_rep_db({[{<<"url">>, Url}]}, ProxyParams, Options);
parse_rep_db(<<DbName/binary>>, _ProxyParams, _Options) ->
    DbName.


maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(?b2l(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
    $/ ->
        Url;
    _ ->
        Url ++ "/"
    end.


make_options(Props) ->
    Options = lists:ukeysort(1, convert_options(Props)),
    DefWorkers = config:get("replicator", "worker_processes", "4"),
    DefBatchSize = config:get("replicator", "worker_batch_size", "500"),
    DefConns = config:get("replicator", "http_connections", "20"),
    DefTimeout = config:get("replicator", "connection_timeout", "30000"),
    DefRetries = config:get("replicator", "retries_per_request", "10"),
    UseCheckpoints = config:get("replicator", "use_checkpoints", "true"),
    DefCheckpointInterval = config:get("replicator", "checkpoint_interval", "30000"),
    {ok, DefSocketOptions} = couch_util:parse_term(
        config:get("replicator", "socket_options",
            "[{keepalive, true}, {nodelay, false}]")),
    lists:ukeymerge(1, Options, lists:keysort(1, [
        {connection_timeout, list_to_integer(DefTimeout)},
        {retries, list_to_integer(DefRetries)},
        {http_connections, list_to_integer(DefConns)},
        {socket_options, DefSocketOptions},
        {worker_batch_size, list_to_integer(DefBatchSize)},
        {worker_processes, list_to_integer(DefWorkers)},
        {use_checkpoints, list_to_existing_atom(UseCheckpoints)},
        {checkpoint_interval, list_to_integer(DefCheckpointInterval)}
    ])).


convert_options([])->
    [];
convert_options([{<<"cancel">>, V} | R]) ->
    [{cancel, V} | convert_options(R)];
convert_options([{IdOpt, V} | R]) when IdOpt =:= <<"_local_id">>;
        IdOpt =:= <<"replication_id">>; IdOpt =:= <<"id">> ->
    Id = lists:splitwith(fun(X) -> X =/= $+ end, ?b2l(V)),
    [{id, Id} | convert_options(R)];
convert_options([{<<"create_target">>, V} | R]) ->
    [{create_target, V} | convert_options(R)];
convert_options([{<<"continuous">>, V} | R]) ->
    [{continuous, V} | convert_options(R)];
convert_options([{<<"filter">>, V} | R]) ->
    [{filter, V} | convert_options(R)];
convert_options([{<<"query_params">>, V} | R]) ->
    [{query_params, V} | convert_options(R)];
convert_options([{<<"doc_ids">>, null} | R]) ->
    convert_options(R);
convert_options([{<<"doc_ids">>, V} | R]) ->
    % Ensure same behaviour as old replicator: accept a list of percent
    % encoded doc IDs.
    DocIds = [?l2b(couch_httpd:unquote(Id)) || Id <- V],
    [{doc_ids, DocIds} | convert_options(R)];
convert_options([{<<"worker_processes">>, V} | R]) ->
    [{worker_processes, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"worker_batch_size">>, V} | R]) ->
    [{worker_batch_size, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"http_connections">>, V} | R]) ->
    [{http_connections, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"connection_timeout">>, V} | R]) ->
    [{connection_timeout, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"retries_per_request">>, V} | R]) ->
    [{retries, couch_util:to_integer(V)} | convert_options(R)];
convert_options([{<<"socket_options">>, V} | R]) ->
    {ok, SocketOptions} = couch_util:parse_term(V),
    [{socket_options, SocketOptions} | convert_options(R)];
convert_options([{<<"since_seq">>, V} | R]) ->
    [{since_seq, V} | convert_options(R)];
convert_options([{<<"use_checkpoints">>, V} | R]) ->
    [{use_checkpoints, V} | convert_options(R)];
convert_options([{<<"checkpoint_interval">>, V} | R]) ->
    [{checkpoint_interval, couch_util:to_integer(V)} | convert_options(R)];
convert_options([_ | R]) -> % skip unknown option
    convert_options(R).


parse_proxy_params(ProxyUrl) when is_binary(ProxyUrl) ->
    parse_proxy_params(?b2l(ProxyUrl));
parse_proxy_params([]) ->
    [];
parse_proxy_params(ProxyUrl) ->
    #url{
        host = Host,
        port = Port,
        username = User,
        password = Passwd
    } = ibrowse_lib:parse_url(ProxyUrl),
    [{proxy_host, Host}, {proxy_port, Port}] ++
        case is_list(User) andalso is_list(Passwd) of
        false ->
            [];
        true ->
            [{proxy_user, User}, {proxy_password, Passwd}]
        end.


ssl_params(Url) ->
    case ibrowse_lib:parse_url(Url) of
    #url{protocol = https} ->
        Depth = list_to_integer(
            config:get("replicator", "ssl_certificate_max_depth", "3")
        ),
        VerifyCerts = config:get("replicator", "verify_ssl_certificates"),
        CertFile = config:get("replicator", "cert_file", nil),
        KeyFile = config:get("replicator", "key_file", nil),
        Password = config:get("replicator", "password", nil),
        SslOpts = [{depth, Depth} | ssl_verify_options(VerifyCerts =:= "true")],
        SslOpts1 = case CertFile /= nil andalso KeyFile /= nil of
            true ->
                case Password of
                    nil ->
                        [{certfile, CertFile}, {keyfile, KeyFile}] ++ SslOpts;
                    _ ->
                        [{certfile, CertFile}, {keyfile, KeyFile},
                            {password, Password}] ++ SslOpts
                end;
            false -> SslOpts
        end,
        [{is_ssl, true}, {ssl_options, SslOpts1}];
    #url{protocol = http} ->
        []
    end.

ssl_verify_options(Value) ->
    ssl_verify_options(Value, erlang:system_info(otp_release)).

ssl_verify_options(true, OTPVersion) when OTPVersion >= "R14" ->
    CAFile = config:get("replicator", "ssl_trusted_certificates_file"),
    [{verify, verify_peer}, {cacertfile, CAFile}];
ssl_verify_options(false, OTPVersion) when OTPVersion >= "R14" ->
    [{verify, verify_none}];
ssl_verify_options(true, _OTPVersion) ->
    CAFile = config:get("replicator", "ssl_trusted_certificates_file"),
    [{verify, 2}, {cacertfile, CAFile}];
ssl_verify_options(false, _OTPVersion) ->
    [{verify, 0}].


%% New db record has Options field removed here to enable smoother dbcore migration
open_db(#db{name = Name, user_ctx = UserCtx}) ->
    {ok, Db} = couch_db:open(Name, [{user_ctx, UserCtx} | []]),
    Db;
open_db(HttpDb) ->
    HttpDb.


close_db(#db{} = Db) ->
    couch_db:close(Db);
close_db(_HttpDb) ->
    ok.


start_db_compaction_notifier(#db{name = DbName}, Server) ->
    {ok, Pid} = couch_event:link_listener(
            ?MODULE, handle_db_event, Server, [{dbname, DbName}]
        ),
    Pid;
start_db_compaction_notifier(_, _) ->
    nil.


stop_db_compaction_notifier(nil) ->
    ok;
stop_db_compaction_notifier(Listener) ->
    couch_event:stop_listener(Listener).


handle_db_event(DbName, compacted, Server) ->
    gen_server:cast(Server, {db_compacted, DbName}),
    {ok, Server};
handle_db_event(_DbName, _Event, Server) ->
    {ok, Server}.

% Obsolete - remove in next release
sum_stats(S1, S2) ->
    couch_replicator_stats:sum_stats(S1, S2).

mp_parse_doc({headers, H}, []) ->
    case couch_util:get_value("content-type", H) of
    {"application/json", _} ->
        fun (Next) ->
            mp_parse_doc(Next, [])
        end
    end;
mp_parse_doc({body, Bytes}, AccBytes) ->
    fun (Next) ->
        mp_parse_doc(Next, [Bytes | AccBytes])
    end;
mp_parse_doc(body_end, AccBytes) ->
    receive {get_doc_bytes, Ref, From} ->
        From ! {doc_bytes, Ref, lists:reverse(AccBytes)}
    end,
    fun mp_parse_atts/1.

mp_parse_atts(eof) ->
    ok;
mp_parse_atts({headers, _H}) ->
    fun mp_parse_atts/1;
mp_parse_atts({body, Bytes}) ->
    receive {get_bytes, Ref, From} ->
        From ! {bytes, Ref, Bytes}
    end,
    fun mp_parse_atts/1;
mp_parse_atts(body_end) ->
    fun mp_parse_atts/1.

is_deleted(Change) ->
    case couch_util:get_value(<<"deleted">>, Change) of
    undefined ->
        % keep backwards compatibility for a while
        couch_util:get_value(deleted, Change, false);
    Else ->
        Else
    end.

% This is to support hot upgrades for 429 backoff
maybe_upgrade_wait(#httpdb{wait = W} = HttpDb) when is_integer(W) ->
    HttpDb#httpdb{wait = {W, 25}};
maybe_upgrade_wait(HttpDb) ->
    HttpDb.


% DBNext has couch_server:get_uuid() function to get the UUID of the cluster
% DBCore doesn't use that feature. Here it is read directly from config
% setting and used only for forward compatibility
get_couch_server_uuid() ->
    case config:get("couchdb", "uuid") of
        UUID when is_list(UUID) ->
            ?l2b(UUID);
        undefined ->
            undefined
    end.


get_v4_endpoint(UserCtx, #httpdb{} = HttpDb) ->
    {Url, Headers, OAuth} = case get_rep_endpoint(UserCtx, HttpDb) of
        {remote, U, Hds} ->
            {U, Hds, undefined};
        {remote, U, Hds, OA} ->
            {U, Hds, OA}
    end,
    {UserFromHeaders, HeadersWithoutBasicAuth} = remove_basic_auth(Headers),
    {UserFromUrl, Host, NonDefaultPort, Path} = get_v4_url_info(Url),
    User = pick_defined_value([UserFromUrl, UserFromHeaders]),
    {remote, User, Host, NonDefaultPort, Path, HeadersWithoutBasicAuth, OAuth};
get_v4_endpoint(UserCtx, <<DbName/binary>>) ->
    {local, DbName, UserCtx}.


remove_basic_auth(Headers) ->
    case lists:partition(fun is_basic_auth/1, Headers) of
        {[], HeadersWithoutBasicAuth} ->
            {undefined, HeadersWithoutBasicAuth};
        {[{_, "Basic " ++ Base64} | _], HeadersWithoutBasicAuth} ->
            User = get_basic_auth_user(Base64),
            {User, HeadersWithoutBasicAuth}
    end.


is_basic_auth({"Authorization", "Basic " ++ _Base64}) ->
    true;
is_basic_auth(_) ->
    false.


get_basic_auth_user(Base64) ->
    try re:split(base64:decode(Base64), ":", [{return, list}, {parts, 2}]) of
        [User, _Pass] ->
            User;
        _ ->
            undefined
    catch
        % Tolerate invalid B64 values here to avoid crashing replicator
        error:function_clause ->
            undefined
    end.


pick_defined_value(Values) ->
    case [V || V <- Values, V /= undefined] of
        [] ->
            undefined;
        DefinedValues ->
            hd(DefinedValues)
    end.


get_v4_url_info(Url) when is_binary(Url) ->
    get_v4_url_info(binary_to_list(Url));
get_v4_url_info(Url) ->
    case ibrowse_lib:parse_url(Url) of
        {error, invalid_uri} ->
            % Tolerate errors here to avoid a bad user document
            % crashing the replicator
            {undefined, Url, undefined, undefined};
        #url{
            protocol = Schema,
            username = User,
            host = Host,
            port = Port,
            path = Path
        } ->
            NonDefaultPort = get_non_default_port(Schema, Port),
            {User, Host, NonDefaultPort, Path}
    end.


get_non_default_port(https, 443) ->
    default;
get_non_default_port(http, 80) ->
    default;
get_non_default_port(http, 5984) ->
    default;
get_non_default_port(_Schema, Port) ->
    Port.
