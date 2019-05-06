-module(graphconnectivity).
-export([start/0]).


% Scanning the graph %
scan(0) -> #{};
scan(E) ->
    {ok, [A, B]} = io:fread("", "~d ~d"),
    Map = scan(E-1),
    Bool1 = maps:is_key(A, Map),
    Bool2 = maps:is_key(B, Map),
    if
        Bool1 == true ->
            List = maps:get(A, Map),
            NewList = lists:append(List, [B]),
            Map1 = maps:put(A, NewList, Map);
        true ->
            Map1 = maps:put(A, [B], Map)
    end,
    if
        Bool2 == true ->
            List1 = maps:get(B, Map1),
            NewList1 = lists:append(List1, [A]),
            Map2 = maps:put(B, NewList1, Map1);
        true ->
            Map2 = maps:put(B, [A], Map1)
    end,
    Map2.


% Add Empty sets in AdjList %
addempty(Graph, 0) -> Graph;
addempty(Graph, V) ->
    Graph1 = addempty(Graph, V-1),
    Bool = maps:is_key(V, Graph1),
    if
        Bool == true ->
            Graph2 = Graph1;
        true ->
            Graph2 = maps:put(V, [], Graph1)
    end,
    Graph2.


% Initialise Parent array %
initparent(High, High) ->
    #{High=>High};
initparent(Low, High) ->
    Parent1 = initparent(Low+1, High),
    Parent = maps:put(Low, Low, Parent1),
    Parent.


% Get sub-graph %
getdict(High, High, Graph) ->
    List = maps:get(High, Graph),
    #{High=>List};
getdict(Low, High, Graph) ->
    Map = getdict(Low+1, High, Graph),
    List = maps:get(Low, Graph),
    Map1 = maps:put(Low, List, Map),
    Map1.


% Vertex Partitioning %
distributer(_, _, 0, _, _) -> #{};
distributer(V, P, Curr, Graph, Self) ->
    Map = distributer(V, P, Curr-1, Graph, Self),
    Var = V div P,
    Low = (Curr - 1) * Var + 1,
    if
        Curr == P ->
            High = V;
        true ->
            High = Curr * Var
    end,
    Dict = getdict(Low, High, Graph),
    Pid = spawn(fun() -> child(Self, Dict, Low, High, Curr) end),
    Map1 = maps:put(Curr, Pid, Map),
    Map1.


% Find of DSU %
getparent(A, Parent) ->
    Curr = maps:get(A, Parent),
    if
        Curr /= A ->
            Parent1 = getparent(Curr, Parent),
            Curr1 = maps:get(Curr, Parent1),
            Parent2 = maps:put(A, Curr1, Parent1);
        true ->
            Parent2 = Parent
    end,
    Parent2.


% DSU of a vertex %
update([], Parent, _, _, _) -> Parent;
update([A|List], Parent, Curr, Low, High) ->
    Parent1 = update(List, Parent, Curr, Low, High),
    if
        (Curr > High) or (Curr < Low) or (A > High) or (A < Low) ->
            Parent2 = Parent1;
        true ->
            Par1 = getparent(A, Parent1),
            Par2 = getparent(Curr, Par1),
            P1 = maps:get(A, Par2),
            P2 = maps:get(Curr, Par2),
            Parent2 = maps:put(P1, P2, Par2)
    end,
    Parent2.


% Run DSU locally %
updatepar(High, E, Low, High, Parent) ->
    List = maps:get(High, E),
    Parent1 = update(List, Parent, High, Low, High),
    Parent1;
updatepar(Curr, E, Low, High, Parent) ->
    Parent1 = updatepar(Curr+1, E, Low, High, Parent),
    List = maps:get(Curr, E),
    Parent2 = update(List, Parent1, Curr, Low, High),
    Parent2.


% update cross edge of vertex %
getcross(_, [], _, _, Root) -> Root ! {endreceive};
getcross(Curr, [A|List], Low, High, Root) ->
    getcross(Curr, List, Low, High, Root),
    if
        ((Curr > High) or (Curr < Low) or (A > High) or (A < Low)) and (Curr < A) ->
            Root ! {mergecross, Curr, A};
        true ->
            []
    end.


% update cross of component %
updatecross(Root, E, High, Low, High) ->
    List = maps:get(High, E),
    getcross(High, List, Low, High, Root);
updatecross(Root, E, Curr, Low, High) ->
    updatecross(Root, E, Curr+1, Low, High),
    List = maps:get(Curr, E),
    getcross(Curr, List, Low, High, Root).


% wait for synchroniaztion %
waitforok() ->
    receive
        {proceed} ->
            [];
        [] ->
            waitforok()
    end.


% processes %
child(Root, E, Low, High, _) ->
    Parent = initparent(Low, High),
    Parent1 = updatepar(Low, E, Low, High, Parent),
    Root ! {local, Parent1},
    waitforok(),
    updatecross(Root, E, Low, Low, High).


% collect parent array at main %
collect(0) -> #{};
collect(P) ->
    Parent = collect(P-1),
    receive
        {local, Parent1} ->
            Parent2 = maps:merge(Parent, Parent1);
        [] ->
            Parent2 = Parent
    end,
    Parent2.


% update cross edges globally %
globalupdate(Parent, 0, _) -> Parent;
globalupdate(Parent, P, I) ->
    %io:fwrite("gu ~w~n", [P]),
    if
        I == 1 ->
            Parent1 = globalupdate(Parent, P-1, 1);
        true ->
            Parent1 = Parent
    end,
    receive
        {mergecross, A, B} ->
            Par1 = getparent(A, Parent1),
            P1 = maps:get(A, Par1),
            Par2 = getparent(B, Par1),
            P2 = maps:get(B, Par2),
            Par3 = maps:put(P1, P2, Par2),
            Par = globalupdate(Par3, P, 0);
        {endreceive} ->
            Par = Parent1;
        [] ->
            Par = Parent1
    end,
    Par.


% broadcast %
broadcast(0, _) -> [];
broadcast(Curr, ChildIds) ->
    broadcast(Curr-1, ChildIds),
    Pid = maps:get(Curr, ChildIds),
    Pid ! {proceed}.


% updateparents %
updateparents(Parent, 0) -> Parent;
updateparents(Parent, Curr) ->
    Parent1 = updateparents(Parent, Curr-1),
    Parent2 = getparent(Curr, Parent1),
    Parent2.


% Main Function %
start() ->
    {ok, [V, E, P]} = io:fread("", "~d ~d ~d"),
    Graph1 = scan(E),
    Graph = addempty(Graph1, V),
    io:fwrite("scanned\n"),
    Self = self(),

    statistics(runtime),
    statistics(wall_clock),

    ChildIds = distributer(V, P, P, Graph, Self),
    %io:fwrite("distributed\n"),
    Start = os:timestamp(),
    Parent = collect(P),
    %io:fwrite("formed parent\n"),
    broadcast(P, ChildIds),
    %io:fwrite("broadcasted\n"),
    Parent1 = globalupdate(Parent, P, 1),
    %io:fwrite("updated\n"),
    Parent2 = updateparents(Parent1, V),

    {_, Time1} = statistics(runtime),
    {_, Time2} = statistics(wall_clock),
    U1 = Time1 * 1000,
    U2 = Time2 * 1000,
    io:format("Code time=~p (~p) microseconds~n",
    [U1,U2]).
    %io:fwrite("~w~n", [Parent2]).
