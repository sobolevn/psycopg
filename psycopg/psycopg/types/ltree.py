"""
PostgreSQL ltree type wrapper
"""

# Copyright (C) 2020-2021 The Psycopg Team


import re
from typing import Any, List, Optional, overload, Sequence, Tuple, Type, Union
from functools import total_ordering
from collections import namedtuple

from .. import pq
from .. import postgres
from ..abc import Buffer, AdaptContext
from ..adapt import Dumper, Loader
from .._typeinfo import TypeInfo

re_ltree = re.compile(r"^[a-zA-Z0-9_]+$")
re_lquery = re.compile(r"^[a-zA-Z0-9_\|]+$")


@total_ordering
class Ltree(Tuple[str, ...]):
    """Wrapper for the Ltree data type."""

    __slots__ = ()

    def __new__(cls, *args: Any) -> "Ltree":
        def _label(s: Any) -> Optional[str]:
            if s is None or s == "":
                return None
            if isinstance(s, str):
                if re_ltree.match(s):
                    return s
                else:
                    raise ValueError("ltree label not valid: %s" % s)
            else:
                return _label(str(s))

        labels: List[Optional[str]] = []

        for arg in args:
            if isinstance(arg, str):
                labels.extend(_label(i) for i in arg.split("."))
            elif isinstance(arg, Sequence):
                labels.extend(_label(i) for i in arg)
            else:
                labels.append(_label(arg))

        return tuple.__new__(cls, (i for i in labels if i is not None))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Ltree):
            return tuple.__eq__(self, other)
        elif isinstance(other, str):
            return str(self) == other
        else:
            return self.__eq__(Ltree(other))

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, Ltree):
            return tuple.__lt__(self, other)
        elif isinstance(other, str):
            return str(self) < other
        else:
            return self.__lt__(Ltree(other))

    def __hash__(self) -> int:
        return hash(self)

    def __add__(self, other: Any) -> "Ltree":
        return Ltree(self, other)

    def __radd__(self, other: Any) -> "Ltree":
        return Ltree(other, self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)!r})"

    def __str__(self) -> str:
        return ".".join(str(i) for i in self)

    @overload
    def __getitem__(self, x: int) -> str:
        ...

    @overload
    def __getitem__(self, x: slice) -> "Ltree":
        ...

    def __getitem__(self, x: Union[int, slice]) -> Union[str, "Ltree"]:
        if not isinstance(x, slice):
            return tuple.__getitem__(self, x)
        else:
            return Ltree(tuple.__getitem__(self, x))


class Star(namedtuple("Star", "min max")):
    def __new__(
        cls, min: Optional[int] = None, max: Optional[int] = None
    ) -> "Star":
        min = None if not min else int(min)
        max = None if not max else int(max)
        self = super(Star, cls).__new__(cls, min, max)
        return self

    re_star = re.compile(
        r"""
        ^ (?:
              (\*)
            | (?: \* \{ (\d+) \} )
            | (?: \* \{ (\d*) , (\d*) \} )
        ) $""",
        re.VERBOSE,
    )

    @classmethod
    def parse(cls, s: str) -> "Optional[Star]":
        m = cls.re_star.match(s)
        if m is None:
            return None

        if m.group(1):
            min = max = None
        elif m.group(2):
            min = max = int(m.group(2))
        else:
            _min = m.group(3)
            min = int(_min) if _min else None
            _max = m.group(4)
            max = int(_max) if _max else None

        return cls(min, max)

    def merge(self, other: "Star") -> "Star":
        min = ((self.min or 0) + (other.min or 0)) or None
        max = (
            None
            if (self.max is None or other.max is None)
            else self.max + other.max
        )
        return Star(min, max)

    def __str__(self) -> str:
        if self.min is None and self.max is None:
            return "*"
        if self.min is not None and self.max is not None:
            if self.min == self.max:
                return "*{%d}" % (self.min,)
            else:
                return "*{%d,%d}" % (self.min, self.max)
        if self.min is not None:
            return "*{%d,}" % (self.min,)
        if self.max is not None:
            return "*{,%d}" % (self.max,)

        assert False, "wat?"


LqueryLabel = Union[Star, str]


class Lquery(Tuple[LqueryLabel]):
    """Wrapper for the Lquery data type."""

    __slots__ = ()

    def __new__(cls, *args: Any) -> "Lquery":
        def _label(s: Any) -> Optional[LqueryLabel]:
            if s is None or s == "":
                return None
            if isinstance(s, str):
                if re_lquery.match(s):
                    return s

                star = Star.parse(s)
                if star is not None:
                    return star

                raise ValueError("lquery label not valid: %s" % s)
            else:
                return _label(str(s))

        labels: List[Optional[LqueryLabel]] = []

        for arg in args:
            if isinstance(arg, str):
                labels.extend(_label(i) for i in arg.split("."))
            elif isinstance(arg, Sequence):
                labels.extend(_label(i) for i in arg)
            else:
                labels.append(_label(arg))

        return tuple.__new__(cls, cls._merge_labels(labels))

    @classmethod
    def _merge_labels(
        cls, labels: Sequence[Optional[LqueryLabel]]
    ) -> List[LqueryLabel]:
        rv: List[LqueryLabel] = []
        for i in labels:
            if i is None:
                continue
            if not rv:
                rv.append(i)
                continue
            if isinstance(rv[-1], Star) and isinstance(i, Star):
                rv[-1] = rv[-1].merge(i)
            else:
                rv.append(i)

        return rv

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Lquery):
            return tuple.__eq__(self, other)
        elif isinstance(other, str):
            return str(self) == other
        else:
            return self.__eq__(Lquery(other))

    def __hash__(self) -> int:
        return hash(self)

    def __add__(self, other: Any) -> "Lquery":
        return Lquery(self, other)

    def __radd__(self, other: Any) -> "Lquery":
        return Lquery(other, self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({str(self)!r})"

    def __str__(self) -> str:
        return ".".join(str(i) for i in self)

    @overload
    def __getitem__(self, x: int) -> str:
        ...

    @overload
    def __getitem__(self, x: slice) -> "Lquery":
        ...

    def __getitem__(
        self, x: Union[int, slice]
    ) -> Union[LqueryLabel, "Lquery"]:
        if not isinstance(x, slice):
            return tuple.__getitem__(self, x)
        else:
            return Lquery(tuple.__getitem__(self, x))


class BaseLtreeDumper(Dumper):

    format = pq.Format.TEXT

    def dump(self, obj: Ltree) -> bytes:
        return str(obj).encode("utf8")


class LtreeLoader(Loader):

    format = pq.Format.TEXT

    def load(self, data: Buffer) -> Ltree:
        if isinstance(data, memoryview):
            data = bytes(data)
        return Ltree(data.decode("utf8"))


def register_adapters(
    info: TypeInfo, context: Optional[AdaptContext] = None
) -> None:

    info.register(context)

    adapters = context.adapters if context else postgres.adapters

    # Generate and register a customized text dumper
    dumper: Type[BaseLtreeDumper] = type(
        "LtreeDumper", (BaseLtreeDumper,), {"_oid": info.oid}
    )
    adapters.register_dumper(Ltree, dumper)

    # register the text loader on the oid
    adapters.register_loader(info.oid, LtreeLoader)
