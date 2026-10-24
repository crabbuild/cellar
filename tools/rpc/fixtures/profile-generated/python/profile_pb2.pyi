import builtins as _cellar_rpc_builtins

import datetime

from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import wrappers_pb2 as _wrappers_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AllTypes(_message.Message):
    __slots__ = ("signed32", "unsigned32", "zigzag32", "fixed_unsigned32", "fixed_signed32", "signed64", "unsigned64", "zigzag64", "fixed_unsigned64", "fixed_signed64", "float32", "float64", "enabled", "text", "data", "state", "present", "selected_text", "selected_child", "packed", "unpacked", "children", "by_name", "by_ID", "timestamp", "duration")
    class State(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATE_UNSPECIFIED: _ClassVar[AllTypes.State]
        STATE_READY: _ClassVar[AllTypes.State]
        STATE_FAILED: _ClassVar[AllTypes.State]
    STATE_UNSPECIFIED: AllTypes.State
    STATE_READY: AllTypes.State
    STATE_FAILED: AllTypes.State
    class Child(_message.Message):
        __slots__ = ("name", "recursive")
        NAME_FIELD_NUMBER: _ClassVar[int]
        RECURSIVE_FIELD_NUMBER: _ClassVar[int]
        name: str
        recursive: AllTypes
        def __init__(self, name: _Optional[str] = ..., recursive: _Optional[_Union[AllTypes, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ...) -> None: ...
    class ByNameEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: AllTypes.Child
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[AllTypes.Child, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ...) -> None: ...
    class ByIDEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: bool
        def __init__(self, key: _Optional[int] = ..., value: _Optional[bool] = ...) -> None: ...
    SIGNED32_FIELD_NUMBER: _ClassVar[int]
    UNSIGNED32_FIELD_NUMBER: _ClassVar[int]
    ZIGZAG32_FIELD_NUMBER: _ClassVar[int]
    FIXED_UNSIGNED32_FIELD_NUMBER: _ClassVar[int]
    FIXED_SIGNED32_FIELD_NUMBER: _ClassVar[int]
    SIGNED64_FIELD_NUMBER: _ClassVar[int]
    UNSIGNED64_FIELD_NUMBER: _ClassVar[int]
    ZIGZAG64_FIELD_NUMBER: _ClassVar[int]
    FIXED_UNSIGNED64_FIELD_NUMBER: _ClassVar[int]
    FIXED_SIGNED64_FIELD_NUMBER: _ClassVar[int]
    FLOAT32_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    PRESENT_FIELD_NUMBER: _ClassVar[int]
    SELECTED_TEXT_FIELD_NUMBER: _ClassVar[int]
    SELECTED_CHILD_FIELD_NUMBER: _ClassVar[int]
    PACKED_FIELD_NUMBER: _ClassVar[int]
    UNPACKED_FIELD_NUMBER: _ClassVar[int]
    CHILDREN_FIELD_NUMBER: _ClassVar[int]
    BY_NAME_FIELD_NUMBER: _ClassVar[int]
    BY_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    signed32: int
    unsigned32: int
    zigzag32: int
    fixed_unsigned32: int
    fixed_signed32: int
    signed64: int
    unsigned64: int
    zigzag64: int
    fixed_unsigned64: int
    fixed_signed64: int
    float32: float
    float64: float
    enabled: bool
    text: str
    data: bytes
    state: AllTypes.State
    present: int
    selected_text: str
    selected_child: AllTypes.Child
    packed: _containers.RepeatedScalarFieldContainer[int]
    unpacked: _containers.RepeatedScalarFieldContainer[int]
    children: _containers.RepeatedCompositeFieldContainer[AllTypes.Child]
    by_name: _containers.MessageMap[str, AllTypes.Child]
    by_ID: _containers.ScalarMap[int, bool]
    timestamp: _timestamp_pb2.Timestamp
    duration: _duration_pb2.Duration
    def __init__(self, signed32: _Optional[int] = ..., unsigned32: _Optional[int] = ..., zigzag32: _Optional[int] = ..., fixed_unsigned32: _Optional[int] = ..., fixed_signed32: _Optional[int] = ..., signed64: _Optional[int] = ..., unsigned64: _Optional[int] = ..., zigzag64: _Optional[int] = ..., fixed_unsigned64: _Optional[int] = ..., fixed_signed64: _Optional[int] = ..., float32: _Optional[float] = ..., float64: _Optional[float] = ..., enabled: _Optional[bool] = ..., text: _Optional[str] = ..., data: _Optional[bytes] = ..., state: _Optional[_Union[AllTypes.State, str]] = ..., present: _Optional[int] = ..., selected_text: _Optional[str] = ..., selected_child: _Optional[_Union[AllTypes.Child, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., packed: _Optional[_Iterable[int]] = ..., unpacked: _Optional[_Iterable[int]] = ..., children: _Optional[_Iterable[_Union[AllTypes.Child, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]]] = ..., by_name: _Optional[_Mapping[str, AllTypes.Child]] = ..., by_ID: _Optional[_Mapping[int, bool]] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ...) -> None: ...

class RuntimeMappings(_message.Message):
    __slots__ = ("wrapped64", "wrapped_bytes", "wrapped_double", "wrapper_list", "wrapper_map", "object", "objects", "objects_by_name", "wrapped_choice", "object_choice", "explicit_value", "explicit_list", "wrapped_float", "wrapped_unsigned64", "wrapped32", "wrapped_unsigned32", "wrapped_bool", "wrapped_string")
    class WrapperMapEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: bool
        value: _wrappers_pb2.Int64Value
        def __init__(self, key: _Optional[bool] = ..., value: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ...) -> None: ...
    class ObjectsByNameEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Struct
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Struct, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ...) -> None: ...
    WRAPPED64_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_BYTES_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_DOUBLE_FIELD_NUMBER: _ClassVar[int]
    WRAPPER_LIST_FIELD_NUMBER: _ClassVar[int]
    WRAPPER_MAP_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    OBJECTS_FIELD_NUMBER: _ClassVar[int]
    OBJECTS_BY_NAME_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_CHOICE_FIELD_NUMBER: _ClassVar[int]
    OBJECT_CHOICE_FIELD_NUMBER: _ClassVar[int]
    EXPLICIT_VALUE_FIELD_NUMBER: _ClassVar[int]
    EXPLICIT_LIST_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_FLOAT_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_UNSIGNED64_FIELD_NUMBER: _ClassVar[int]
    WRAPPED32_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_UNSIGNED32_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_BOOL_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_STRING_FIELD_NUMBER: _ClassVar[int]
    wrapped64: _wrappers_pb2.Int64Value
    wrapped_bytes: _wrappers_pb2.BytesValue
    wrapped_double: _wrappers_pb2.DoubleValue
    wrapper_list: _containers.RepeatedCompositeFieldContainer[_wrappers_pb2.Int64Value]
    wrapper_map: _containers.MessageMap[bool, _wrappers_pb2.Int64Value]
    object: _struct_pb2.Struct
    objects: _containers.RepeatedCompositeFieldContainer[_struct_pb2.Struct]
    objects_by_name: _containers.MessageMap[str, _struct_pb2.Struct]
    wrapped_choice: _wrappers_pb2.Int64Value
    object_choice: _struct_pb2.Struct
    explicit_value: _struct_pb2.Value
    explicit_list: _struct_pb2.ListValue
    wrapped_float: _wrappers_pb2.FloatValue
    wrapped_unsigned64: _wrappers_pb2.UInt64Value
    wrapped32: _wrappers_pb2.Int32Value
    wrapped_unsigned32: _wrappers_pb2.UInt32Value
    wrapped_bool: _wrappers_pb2.BoolValue
    wrapped_string: _wrappers_pb2.StringValue
    def __init__(self, wrapped64: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapped_bytes: _Optional[_Union[_wrappers_pb2.BytesValue, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapped_double: _Optional[_Union[_wrappers_pb2.DoubleValue, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapper_list: _Optional[_Iterable[_Union[_wrappers_pb2.Int64Value, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]]] = ..., wrapper_map: _Optional[_Mapping[bool, _wrappers_pb2.Int64Value]] = ..., object: _Optional[_Union[_struct_pb2.Struct, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., objects: _Optional[_Iterable[_Union[_struct_pb2.Struct, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]]] = ..., objects_by_name: _Optional[_Mapping[str, _struct_pb2.Struct]] = ..., wrapped_choice: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., object_choice: _Optional[_Union[_struct_pb2.Struct, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., explicit_value: _Optional[_Union[_struct_pb2.Value, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., explicit_list: _Optional[_Union[_struct_pb2.ListValue, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapped_float: _Optional[_Union[_wrappers_pb2.FloatValue, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapped_unsigned64: _Optional[_Union[_wrappers_pb2.UInt64Value, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapped32: _Optional[_Union[_wrappers_pb2.Int32Value, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapped_unsigned32: _Optional[_Union[_wrappers_pb2.UInt32Value, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapped_bool: _Optional[_Union[_wrappers_pb2.BoolValue, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ..., wrapped_string: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping[_cellar_rpc_builtins.str, _cellar_rpc_builtins.object]]] = ...) -> None: ...

class ScalarChoice(_message.Message):
    __slots__ = ("number", "flag")
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    FLAG_FIELD_NUMBER: _ClassVar[int]
    number: int
    flag: bool
    def __init__(self, number: _Optional[int] = ..., flag: _Optional[bool] = ...) -> None: ...
