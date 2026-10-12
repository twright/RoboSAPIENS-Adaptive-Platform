"""Classic reference journal records and configurable application vocabulary."""
from rpio_observation.core import SCHEMA_VERSION
from rpio_observation.errors import ObservationError


def tagged(name, value=None):
    result = {"$tag": name}
    if value is not None:
        result["payload"] = value
    return result


def reference(ref):
    return {"store_epoch": ref.store_epoch,
            "global_write_sequence": ref.global_write_sequence, "key": ref.key}


def references(refs):
    unique = []
    for ref in refs:
        value = reference(ref)
        if value not in unique:
            unique.append(value)
    if not unique:
        return tagged("Nothing")
    return tagged("One", unique[0]) if len(unique) == 1 else tagged("Several", unique)


def phase_record(activation, step, refs, admitted):
    return {"schema_version": SCHEMA_VERSION, "$tag": "Phase", "payload": {
        "activation": activation, "step": step, "knowledge": references(refs),
        "arrival": {"journal_time": admitted}}}


def knowledge_record(ref, typed, writer, admitted):
    if not isinstance(typed, dict) or typed.get("$tag") != ref.key:
        raise ObservationError(f"Knowledge codec must tag value as {ref.key!r}")
    if not isinstance(writer, dict) or not isinstance(writer.get("$tag"), str):
        raise ObservationError("Knowledge writer must be a tagged union")
    return {"schema_version": SCHEMA_VERSION, "$tag": "Knowledge", "payload": {
        "version": {"ref": reference(ref), "value": typed, "written_by": writer},
        "arrival": {"journal_time": admitted}}}


class JsonCodec:
    """Default JSON vocabulary. Supply an application codec to match typed TC roots.

    Codecs encode types only: they cannot substitute other versions for the
    refs selected by the actual callback's Knowledge accesses.
    """
    def knowledge(self, key, value):
        return tagged(key, value)

    def trigger(self, kind, topic, message):
        return tagged("Received", {"kind": kind, "topic": topic, "message": message})

    def emitted(self, kind, topic, message):
        return {"kind": kind, "topic": topic, "message": message}

    def outcome(self, wire):
        return wire
