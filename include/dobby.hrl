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
-type reason() :: {mnesia_error, term()} | {badarg, term()}.

% database representation of an identifier (vertex)
-record(identifier, {
    id :: identifier(),
    metadata = null :: jsonable(),
    links = #{} :: #{identifier() => jsonable()}
}).

