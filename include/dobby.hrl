% transaction
-type transaction() :: term().

% an identifier
-type idenfier() :: binary().

% JSONable datatypes for metadata
-type jsonable() :: integer() |
                    float() |
                    list(jsonable()) |
                    binary() |
                    #{binary() => jsonable()}.

% metadata as an argument to functions
-type metadata() :: jsonable() |
                    fun((jsonable()) -> jsonable()) |
                    nochange |
                    delete.

% identifier as an argument to functions
-type endpoint() :: identifier() | {identifier(), metadata()}.

% link as an argument to functions
-type link() :: {endpoint(), endpoint(), metadata()}.

% publish options
-type publish_option() :: 'persistent' | 'message'.

% error reasons
-type reason() :: term().

% search function
-type search_fun() :: fun((identifier(),
                           IdMetadata :: jsonable(),
                           LinkMetadata :: jsonable(),
                           Acc0 :: term()) ->
                            {search_control(), Acc1 :: term()} |
                            {search_control(), search_fun(), Acc1 :: term()}).

% search control
-type search_control() :: continue | skip | stop.

% search options
-type search_options() :: breadth | depth | {max_depth, non_neg_integer()}.

% database representation of an identifier (vertex)
-record(identifier, {
    id :: identifier(),
    metadata = null :: jsonable(),
    links = #{} :: #{identifier() => jsonable()}
}).

% options for all API functions
-record(options, {
    publish = message :: persistent | message,
    traversal = breadth :: breadth | depth,
    max_depth = 0 :: non_neg_integer(),
    delta_fun = fun dby_options:delta_default/2 :: fun(),
    delivery_fun = fun dby_options:delivery_default/1 :: fun()
}).
