-module(eplc_server).

-behaviour(gen_server).

%% called via eplc_sup
-export([start_link/0]).

%% gen_server behavior
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%% public API
-export([new_collection/0, delete_collection/1]).
-export([value_cached/3, value_used/2, value_evicted/2]).

%% MODULE IMPLEMENTATION

%% value supposed to be smaller than all other keys. Integers sort before all other
%% kinds of terms, so we choose a large negative integer.
-define(MIN_KEY, -1000000000000000000000).

-record(state, { seq   = 0                :: non_neg_integer(),
                 cache                    :: ets:tid(),
                 order                    :: etd:tid(),
                 colls = dict:new()       :: dict(), %% CollRef -> PID
                 size  = 0                :: non_neg_integer(),
                 limit = 1024*1024        :: non_neg_integer() }).

-record(element, {
          key  :: {reference(), term()},
          seq  :: non_neg_integer(),
          size :: non_neg_integer()
         }).

start_link() ->
    start_link(?MODULE).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

init([_Name]) ->
    Cache = ets:new(eplc_cache, [ordered_set, {keypos, #element.key}]),
    Order = ets:new(eplc_order, [ordered_set, {keypos, #element.seq}]),

    {ok, #state{ cache=Cache, order=Order }}.


%% register cache-holding process
%% while checked in, the server may send PID {eplc_evict, Ref}

new_collection() ->
    gen_server:call(?MODULE, {new_collection, self()}).


delete_collection(CollRef) when is_reference(CollRef) ->
    gen_server:call(?MODULE, {delete_collection, CollRef}).


%% @doc Notify manager that PID cached a value known as REF
value_cached(CollRef, Key, Size) when Key > ?MIN_KEY ->
    gen_server:cast(?MODULE, {value_cached, {CollRef, Key}, Size}).

value_used(CollRef, Key) when Key > ?MIN_KEY ->
    gen_server:cast(?MODULE, {value_used, {CollRef, Key}}).

value_evicted(CollRef, Key) ->
    gen_server:cast(?MODULE, {value_evicted, {CollRef, Key}}).


handle_call({new_collection, PID}, _From, State) ->
    MRef = monitor(process, PID),
    {reply, {ok, MRef}, State#state{ colls = dict:store(MRef, PID, State#state.colls) }};

handle_call({delete_collection, CollRef}, _From, State) ->
    erlang:demonitor(CollRef, [flush]),
    State2 = remove_all_from(CollRef, State),
    {reply, ok, State2}.


handle_info({'DOWN', CollRef, _, _, _}, State) ->
    case dict:is_key(CollRef, State#state.colls) of
        true ->
            State2 = remove_all_from(CollRef, State) ,
            {noreply, State2};
        _ ->
            {noreply, State}
    end;

handle_info(_Msg, State) ->
    error_logger:error_msg("Unexpected message: ~p~n", [_Msg]),
    {noreply, State}.


remove_all_from(CollRef, State) ->
    remove_all_from(CollRef, State, {CollRef, ?MIN_KEY}).

remove_all_from(CollRef, State = #state{cache=Cache, order=Order}, PrevCollKey) ->

    case ets:next(Cache, PrevCollKey) of
        {CollRef, _Key}=CollKey ->
            case ets:lookup(Cache, CollKey) of
                [Element] ->
                    ets:delete_object(Cache,Element),
                    ets:delete_object(Order,Element);
                _ ->
                    ok
            end,
            remove_all_from(CollRef, State, CollKey);
        _ ->
            State#state{ colls= dict:erase(CollRef, State#state.colls) }
    end.

handle_cast({value_cached, {_CollRef,_Key}=CollKey, ElemSize},
            #state{ seq   = Seq,
                    order = Order,
                    cache = Cache,
                    size  = TotalSize } = State) ->

    %% TODO: verify that _CollRef exists in State#state.colls

    Element = #element{ seq=Seq,
                        key=CollKey,
                        size=ElemSize },

    true = ets:insert(Cache, Element),
    true = ets:insert(Order, Element),

    {ok, State2} = evict_some( State#state{ seq   = Seq+1,
                                            size  = TotalSize+ElemSize }),

    {noreply, State2};

handle_cast({value_used, CollKey}, #state{ seq   = Seq,
                                           order = Order,
                                           cache = Cache } = State) ->
    case ets:lookup(Cache, CollKey) of
        [Element] ->
            ets:delete_object(Order, Element),
            true = ets:insert(Order, Element#element{ seq=Seq }),
            true = ets:update_element(Cache, CollKey, {#element.seq, Seq}),
            {noreply, State#state{ seq=Seq+1 }};
        _ ->
            {noreply, State}
    end;

handle_cast({value_evicted, {_Ref,_Key}=CollKey}, State ) ->
    {ok, State2} = do_delete(CollKey, State),
    {noreply, State2}.


evict_some(#state{limit=SizeLimit, size=TotalSize} = State) when TotalSize < SizeLimit ->
    {ok, State};

evict_some(State = #state{ order=Order, cache=Cache, size=TotalSize, colls=Colls } ) ->
    case ets:first(Order) of
        '$end_of_table' ->
            {ok, State};

        Seq ->
            case ets:lookup(Order, Seq) of
                [#element{key= {CollRef, Key}, size=ElemSize} = Element] ->
                    ets:delete_object(Cache,Element),
                    ets:delete_object(Order,Element),
                    case dict:find(CollRef, Colls) of
                        {ok, PID} ->
                            PID ! { cache_evict, {evict_spec, CollRef, Key }};
                        _ ->
                            ok
                    end,
                    evict_some( State#state{ size=TotalSize-ElemSize } )
            end
    end.

do_delete(CollKey, #state{cache=Cache, order=Order} = State) ->
    case ets:lookup(Cache, CollKey) of
        [#element{}=Element] ->
            ets:delete_object(Cache,Element),
            ets:delete_object(Order,Element);
        _ ->
            ok
    end,
    {ok, State}.


