import builtins as _cellar_rpc_builtins

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IncrementRequest(_message.Message):
    __slots__ = ("delta", "operation_id")
    DELTA_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    delta: int
    operation_id: str
    def __init__(self, delta: _Optional[int] = ..., operation_id: _Optional[str] = ...) -> None: ...

class IncrementResponse(_message.Message):
    __slots__ = ("count", "out_of_range", "operation_conflict")
    COUNT_FIELD_NUMBER: _ClassVar[int]
    OUT_OF_RANGE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_CONFLICT_FIELD_NUMBER: _ClassVar[int]
    count: Count
    out_of_range: OutOfRange
    operation_conflict: OperationConflict
    def __init__(self, count: _Optional[_Union[Count, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., out_of_range: _Optional[_Union[OutOfRange, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., operation_conflict: _Optional[_Union[OperationConflict, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ...) -> None: ...

class Count(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class OutOfRange(_message.Message):
    __slots__ = ("reason",)
    REASON_FIELD_NUMBER: _ClassVar[int]
    reason: str
    def __init__(self, reason: _Optional[str] = ...) -> None: ...

class OperationConflict(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetCountRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetCountResponse(_message.Message):
    __slots__ = ("count",)
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: Count
    def __init__(self, count: _Optional[_Union[Count, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ...) -> None: ...
