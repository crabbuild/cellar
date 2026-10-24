"""Check live Protobuf codec descriptors against the native artifact shapes.

No Protobuf dependency is imported until a registered RPC codec is checked.
Descriptor objects supplied by the pinned Python runtime are immutable; memoize
them per application catalog so recursive/shared types are visited only once.
"""


class CodecCatalog:
    def __init__(self, contracts):
        self._contracts = contracts
        self._checked = set()
        self._files = set()
        self._optional = set()
        self._methods = {
            digest: {method["name"]: method for method in contract["methods"]}
            for digest, contract in contracts.items()
        }

    def _file(self, descriptor):
        if descriptor in self._files:
            return
        from google.protobuf.descriptor_pb2 import FileDescriptorProto
        data = descriptor.serialized_pb
        if len(data) > 4 * 1024 * 1024 or len(self._files) >= 256:
            raise TypeError("RPC codec file exceeds limit")
        file = FileDescriptorProto.FromString(data)
        if file.syntax != "proto3":
            raise TypeError("RPC codecs require proto3")
        pending = [(file.package, message) for message in file.message_type]
        remaining = 65_536
        nodes = 4096
        while pending:
            prefix, message = pending.pop()
            nodes -= 1
            remaining -= len(message.field)
            if nodes < 0 or remaining < 0:
                raise TypeError("RPC codec file exceeds schema limits")
            name = prefix + "." + message.name
            for field in message.field:
                if field.proto3_optional:
                    self._optional.add(name + "." + field.name)
            pending.extend((name, child) for child in message.nested_type)
        self._files.add(descriptor)

    def verify(self, method):
        from google.protobuf.descriptor import Descriptor, EnumDescriptor
        from google.protobuf.message_factory import GetMessageClass
        # The descriptor's registered codec class must be the one actually
        # used for FromString/serialization. Copying a DESCRIPTOR attribute
        # onto an older class cannot establish this association.
        if (GetMessageClass(method.input.DESCRIPTOR) is not method.input
                or GetMessageClass(method.output.DESCRIPTOR) is not method.output):
            raise TypeError("RPC message class differs from its codec descriptor")
        contract = self._contracts.get(method.digest)
        native = self._methods.get(method.digest, {}).get(method.name)
        if (contract is None or native is None
                or method.input.DESCRIPTOR.full_name != native["input"]
                or method.output.DESCRIPTOR.full_name != native["output"]):
            raise TypeError("RPC codec method differs from native registration")
        pending = [method.input.DESCRIPTOR, method.output.DESCRIPTOR]
        seen = set()
        remaining = 65_536
        while pending:
            descriptor = pending.pop()
            key = (method.digest, descriptor)
            if key in self._checked or key in seen:
                continue
            seen.add(key)
            if len(seen) > 4096:
                raise TypeError("RPC codec graph exceeds type limit")
            self._file(descriptor.file)
            expected = contract["types"].get(descriptor.full_name)
            if isinstance(descriptor, EnumDescriptor):
                if descriptor.is_closed:
                    raise TypeError("RPC enums must retain unknown numeric values")
                actual = ["enum", [[value.name, value.number] for value in descriptor.values]]
            elif isinstance(descriptor, Descriptor):
                remaining -= len(descriptor.fields)
                if remaining < 0:
                    raise TypeError("RPC codec graph exceeds field limit")
                rows = []
                for field in sorted(descriptor.fields, key=lambda field: field.number):
                    if field.has_default_value or field.GetOptions().jstype:
                        raise TypeError("RPC codec uses unsupported field options")
                    value = field
                    cardinality = int(field.is_repeated)
                    map_key = 0
                    if field.message_type is not None and field.message_type.GetOptions().map_entry:
                        entry = field.message_type
                        if (not field.is_repeated or entry.containing_type is not descriptor
                                or len(entry.fields) != 2):
                            raise TypeError("RPC codec map has invalid entry structure")
                        key_field, value = entry.fields
                        if (key_field.name != "key" or key_field.number != 1
                                or value.name != "value" or value.number != 2
                                or key_field.is_repeated or value.is_repeated
                                or key_field.containing_oneof or value.containing_oneof):
                            raise TypeError("RPC codec map has invalid key/value structure")
                        cardinality = 2
                        map_key = key_field.type
                    reference = value.message_type or value.enum_type
                    if reference is not None:
                        pending.append(reference)
                    group = field.containing_oneof
                    group = group.name if group and field.full_name not in self._optional else ""
                    rows.append([field.number, field.name, value.type, cardinality,
                                 cardinality == 0 and field.has_presence, group,
                                 cardinality == 1 and field.is_packed,
                                 reference.full_name if reference else "", map_key])
                actual = ["message", rows]
            else:
                raise TypeError("RPC codecs must use Protobuf descriptors")
            if actual != expected:
                raise TypeError("RPC codec shape differs from native registration")
        self._checked.update(seen)
        return contract["limits"]
