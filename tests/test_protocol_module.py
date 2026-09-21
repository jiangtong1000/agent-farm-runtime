"""protocol/ is the single source of the on-disk contract (D5)."""
import dataclasses

from agent_farm_runtime import protocol, provenance
from agent_farm_runtime.models import Receipt


def test_protocol_version_has_one_source():
    assert provenance.PROTOCOL_VERSION is protocol.PROTOCOL_VERSION == 4
    assert provenance.runtime_identity()["protocol_version"] == protocol.PROTOCOL_VERSION


def test_receipt_field_lists_match_the_dataclass():
    names = {f.name for f in dataclasses.fields(Receipt)}
    assert set(protocol.RECEIPT_REQUIRED_FIELDS) | set(protocol.RECEIPT_OPTIONAL_FIELDS) == names
    required = {f.name for f in dataclasses.fields(Receipt) if f.default is dataclasses.MISSING}
    assert required == set(protocol.RECEIPT_REQUIRED_FIELDS)


def test_reexports_are_the_same_objects():
    from agent_farm_runtime.models import TaskState
    from agent_farm_runtime.transitions import LEGAL_TRANSITIONS
    assert protocol.TaskState is TaskState and protocol.LEGAL_TRANSITIONS is LEGAL_TRANSITIONS
