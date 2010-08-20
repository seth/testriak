-module(tr).

-compile([export_all]).

-define(riak_ip, "127.0.0.1").
-define(riak_port, 8081).
-define(bucket, <<"STUFF">>).

-include_lib("eunit/include/eunit.hrl").

riak_client() ->
    {ok, Pid} = riakc_pb_socket:start(?riak_ip, ?riak_port),
    pong = riakc_pb_socket:ping(Pid),
    Pid.

create_stuff(DS, Id, Item) ->
    {Obj, {stuff, Items}} =
        case riakc_pb_socket:get(DS, ?bucket, Id) of
            {error, notfound} ->
                S = {stuff, []},
                {riakc_obj:new(?bucket, Id, term_to_binary(S),
                               "application/octet-stream"), S};
            {ok, O} ->
                {O, binary_to_term(riakc_obj:get_value(O))}
        end,
    NewStuff = {stuff, [Item|Items]},
    ObjToPut = riakc_obj:update_value(Obj, term_to_binary(NewStuff)),
    case riakc_pb_socket:put(DS, ObjToPut, [{w, 3}]) of
        ok ->
            error_logger:info_report({created_stuff, Item}),
            ok;
        Reason ->
            {error, Reason}
    end.

get_stuff(DS, Id) ->
    case riakc_pb_socket:get(DS, ?bucket, Id) of
        {error, notfound} ->
            {error, notfound};
        {ok, O} ->
            binary_to_term(riakc_obj:get_value(O))
    end.


% eunit tests

rt_1_test() ->
    Pid = riak_client(),
    Id = <<"123">>,
    ok = riakc_pb_socket:delete(Pid, ?bucket, Id),
    ok = create_stuff(Pid, Id, [1, 2, 3]),
    ?assertMatch({stuff, [[1, 2, 3]]}, get_stuff(Pid, Id)).

rt_2_test() ->
    Pid = riak_client(),
    Id = <<"abc">>,
    ok = riakc_pb_socket:delete(Pid, ?bucket, Id),
    ok = create_stuff(Pid, Id, [a, b, c]),
    ?assertMatch({stuff, [[a, b, c]]}, get_stuff(Pid, Id)).

rt_3_test() ->
    Pid = riak_client(),
    Id = <<"xyz">>,
    ok = riakc_pb_socket:delete(Pid, ?bucket, Id),
    ok = create_stuff(Pid, Id, [x, y, z]),
    ?assertMatch({stuff, [[x, y, z]]}, get_stuff(Pid, Id)).
