-module(couch_replicator_dns).

-include_lib("kernel/src/inet_dns.hrl").
-include_lib("ibrowse/include/ibrowse.hrl").

-export([
    lookup/1
]).

-spec lookup(iolist()) -> {ok, {term(), pos_integer()}} | {error, term()}.

lookup(Url) ->
    case ibrowse_lib:parse_url(Url) of
        #url{host_type = hostname, host = Host} ->
            case inet_res:resolve(Host, any, any) of
                {error, {Error, _}} ->
                    {error, Error};
                {ok, #dns_rec{anlist=[Answer|_As]}} ->
                    #dns_rr{data=Data, ttl=TTL} = Answer,
                    {Data, TTL};
                Else ->
                    twig:log(warn, "Unknown DNS response: ~p", [Else]),
                    {error, Else}
            end;
        #url{} ->
            {error, not_a_hostname};
        Else ->
            {error, Else}
    end.
