-module(eplc).

-export([new_cache/1, delete_cache/1, delete/2, get/2, put/3, put/4, handle_evict/1]).

-include_lib("eunit/include/eunit.hrl").

-record(eplc, { ref               :: reference(),
                dict = dict:new() :: dict(),
                name              :: term() }).

%%
%% @doc create a new collection (local to the current process)
%%
new_cache(Name) ->
    {ok, CollRef} = eplc_server:new_collection(),
    Cache = #eplc{ ref=CollRef, name=Name },
    put_cache(Cache),
    {ok, Cache}.

%%
%% @doc delete a named collection
%%
delete_cache(Name) ->
    {ok, #eplc{ ref=CollRef }} = get_cache(Name),
    eplc_server:delete_collection(CollRef),
    unset_cache(Name).

%%
%% @delete a specific entry in the named collection
%%
delete(Key, CacheName) ->
    {ok, #eplc{ ref=CollRef, dict=Dict }=Cache} = get_cache(CacheName),
    Dict2 = dict:erase(Key, Dict),
    eplc_server:value_evicted(CollRef, Key),
    put_cache(Cache#eplc{ dict=Dict2 }).

%%
%% @doc get a specific entry in a named collection
%% -spec get(Key::term(), CacheName::term()) -> {ok, term()} | notfound.
get(Key, CacheName) ->
    {ok, #eplc{ ref=CollRef, dict=Dict }} = get_cache(CacheName),
    case dict:find(Key, Dict) of
        {ok, Value} ->
            eplc_server:value_used(CollRef, Key),
            {ok, Value};
        error ->
            notfound
    end.

put(Key, Value, CacheName) ->
    Size = erlang:byte_size(term_to_binary(Value)),
    put(Key, Value, Size, CacheName).

put(Key, Value, Size, CacheName) ->
    {ok, #eplc{ ref=CollRef, dict=Dict } = Cache} = get_cache(CacheName),
    case dict:is_key(Key, Dict) of
        true ->
            eplc_server:value_evicted(CollRef, Key);
        false ->
            ok
    end,
    eplc_server:value_cached(CollRef, Key, Size),
    put_cache(Cache#eplc{ dict=dict:store(Key, Value, Dict) }).

handle_evict({evict_spec, CollRef, Key})
  when is_reference(CollRef) ->
    case find_cache(CollRef) of
        {ok, Cache = #eplc{ dict=Dict} } ->
            put_cache( Cache#eplc{ dict=dict:erase(Key, Dict) } );
        none ->
            ok
    end.

get_cache(CacheName) ->
    case erlang:get('eplc:cache') of
        undefined ->
            none;
        Dict ->
            case dict:find(CacheName, Dict) of
                {ok, Cache} -> ok;
                error -> Cache = new_cache(CacheName)
            end,
            {ok, Cache}
    end.

put_cache(#eplc{ name=CacheName }=Cache) ->
    case erlang:get('eplc:cache') of
        undefined ->
            Dict = dict:new();
        Dict ->
            ok
    end,

    erlang:put('eplc:cache', dict:store(CacheName, Cache, Dict)),
    ok.

unset_cache(CacheName) ->
    case erlang:get('eplc:cache') of
        undefined ->
            Dict = dict:new();
        Dict ->
            ok
    end,

    erlang:put('eplc:cache', dict:erase(CacheName, Dict)),
    ok.

find_cache(CollRef) ->
    case erlang:get('eplc:cache') of
        undefined ->
            none;
        Dict ->
            case dict:fold(fun(_,Cache,Acc) ->
                                   if CollRef =:= Cache#eplc.ref ->
                                           [Cache|Acc];
                                      true ->
                                           Acc
                                   end
                           end,
                           [],
                           Dict)
            of
                [Cache] -> {ok, Cache};
                _ -> none
            end
    end.


-ifdef(TEST).

simple_test() ->
    application:start(eplc),

    eplc:new_cache(test1),
    eplc:put("key1", "value1", 1024*1023, test1),
    eplc:put("key2", "value2", 1024*1023, test1),
    eplc:put("key3", "value3", 1024*1023, test1),

    {ok, "value2"} = eplc:get("key2", test1),

    receive
        {cache_evict, Spec1} ->
            eplc:handle_evict(Spec1),
            notfound = eplc:get("key1", test1)
    after 1000 ->
            exit("missing evict")
    end,

    receive
        {cache_evict, Spec2} ->
            eplc:handle_evict(Spec2),
            notfound = eplc:get("key2", test1)
    after 1000 ->
            exit("missing evict")
    end,

    receive
        {cache_evict, _} ->
            exit("unexpected")
    after 1000 ->
            {ok, "value3"} = eplc:get("key3", test1)
    end,

    delete_cache(test1),

    ok.

-endif.
