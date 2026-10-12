"""Activation outcomes: the fixed journal vocabulary an observed callback returns."""


class BudgetExhausted:
    """Abort/not-applied reason: the time or resource budget ran out."""


def _reason(reason):
    if isinstance(reason, BudgetExhausted):
        return {"$tag": "BudgetExhausted"}
    if isinstance(reason, str):
        return {"$tag": "Failed", "payload": reason}
    raise TypeError("reason must be a string or BudgetExhausted()")


class Outcome:
    """Base class. Subclasses name their tag path and fields (Knowledge objects
    the activation read or wrote, plus ``reason`` where the vocabulary has one)."""
    _tags = ()
    _fields = ()

    def __init__(self, *args, **kwargs):
        names = dict(zip(self._fields, args), **kwargs)
        if sorted(names) != sorted(self._fields) or len(args) > len(self._fields):
            raise TypeError(f"{type(self).__name__}{self._fields} got {args} {kwargs}")
        self.__dict__.update(names)

    def wire(self, resolve):
        """The JSON-ready form; ``resolve(obj)`` yields an acquired object's exact reference."""
        payload = {f: (_reason(self.reason) if f == "reason" else resolve(getattr(self, f)))
                   for f in self._fields}
        node = {"$tag": self._tags[-1]}
        if payload:
            node["payload"] = payload
        for tag in reversed(self._tags[:-1]):
            node = {"$tag": tag, "payload": node}
        return node


def _outcome(name, tags, *fields):
    return type(name, (Outcome,), {"_tags": tags, "_fields": fields})


Monitored = _outcome("Monitored", ("Monitored",), "observation")
Nominal = _outcome("Nominal", ("Analysed", "Nominal"))
Anomaly = _outcome("Anomaly", ("Analysed", "Anomaly"), "evidence")
Generated = _outcome("Generated", ("Planned", "Generated"), "plan")
NoPlan = _outcome("NoPlan", ("Planned", "NoPlan"))
Accepted = _outcome("Accepted", ("Legitimised", "Accepted"), "plan", "legitimate_result")
Rejected = _outcome("Rejected", ("Legitimised", "Rejected"), "plan", "legitimate_result")
Applied = _outcome("Applied", ("Executed", "Applied"), "plan", "effect")
NotApplied = _outcome("NotApplied", ("Executed", "NotApplied"), "plan", "reason")
Aborted = _outcome("Aborted", ("Aborted",), "reason")
