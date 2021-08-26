from string import ascii_lowercase, ascii_uppercase, digits

import pytest
from psycopg.types import TypeInfo
from psycopg.types.ltree import Ltree, Star, Lquery
from psycopg.types.ltree import register_ltree, register_lquery


class TestInit:
    def test_from_string(self):
        t = Ltree("foo.bar.baz")
        assert type(t) is Ltree
        assert len(t) == 3
        assert t == "foo.bar.baz"

    def test_from_args(self):
        t = Ltree("foo", 42, None, "baz")
        assert type(t) is Ltree
        assert len(t) == 3
        assert t == "foo.42.baz"

    def test_from_sequence(self):
        t = Ltree(["foo", 42, None, "baz"])
        assert type(t) is Ltree
        assert len(t) == 3
        assert t == "foo.42.baz"

    def test_from_ltree(self):
        t = Ltree("foo.bar.baz")
        t = Ltree(t)
        assert type(t) is Ltree
        assert len(t) == 3
        assert t == "foo.bar.baz"

    def test_from_sequences(self):
        t = Ltree("foo.bar", "baz.qux")
        assert len(t) == 4
        assert t == "foo.bar.baz.qux"

        t = Ltree("foo.bar", ["baz", None, 42])
        assert len(t) == 4
        assert t == "foo.bar.baz.42"

    def test_empty(self):
        t = Ltree()
        assert type(t) is Ltree
        len(t) == 0
        assert not t

        t = Ltree(None)
        assert type(t) is Ltree
        assert not t

        t = Ltree("")
        assert type(t) is Ltree
        assert not t

        t = Ltree([])
        assert type(t) is Ltree
        assert not t

    def test_valid(self):
        valid = ascii_lowercase + ascii_uppercase + digits + "_"
        for c in valid:
            t = Ltree(c)
            assert type(t) is Ltree
            assert t == c

        for i in range(256):
            c = chr(i)
            if c == ".":
                continue
            if c not in valid:
                try:
                    Ltree(c)
                except ValueError:
                    continue
                else:
                    assert False, "didn't raise: %s" % c


class TestRepr:
    def test_repr(self):
        assert type(repr(Ltree())) is str
        assert type(repr(Ltree("a"))) is str
        assert type(repr(Ltree("a.b"))) is str

        assert repr(Ltree()) == "Ltree('')"
        assert repr(Ltree("a")) == "Ltree('a')"
        assert repr(Ltree("a", "b")) == "Ltree('a.b')"

    def test_str(self):
        assert type(str(Ltree())) is str
        assert type(str(Ltree("a"))) is str
        assert type(str(Ltree("a", "b"))) is str

        assert str(Ltree()) == ""
        assert str(Ltree("a")) == "a"
        assert str(Ltree("a", "b")) == "a.b"


class TestOps:
    def test_eq(self):
        assert Ltree("a.b") == Ltree("a.b")
        assert Ltree("a.b") == "a.b"
        assert "a.b" == Ltree("a.b")

        assert Ltree("") == Ltree()
        assert Ltree("") == ()
        assert Ltree() == ""

    def test_ne(self):
        assert Ltree("a.b") != Ltree("a.b.c")
        assert "a.b" != Ltree("a.b.c")
        assert Ltree("a.b") != "a.b.c"

    def test_gt(self):
        assert Ltree("a.b") < Ltree("a.b.c")
        assert Ltree("a.b") <= Ltree("a.b.c")
        assert Ltree("a.b") < Ltree("a.c")
        assert Ltree("a.b") <= Ltree("a.c")
        assert Ltree("aa.b") > Ltree("a.c")
        assert Ltree("aa.b") >= Ltree("a.c")

    def test_add(self):
        assert Ltree("a.b") + "c" == Ltree("a.b.c")
        assert Ltree("a.b") + "" == Ltree("a.b")
        assert (Ltree("a.b") + None) == Ltree("a.b")
        assert Ltree("a.b") + 42 == Ltree("a.b.42")
        assert Ltree("a.b") + ["c", None, 42] == Ltree("a.b.c.42")

    def test_radd(self):
        assert "c" + Ltree("a.b") == Ltree("c.a.b")
        assert "" + Ltree("a.b") + "" == Ltree("a.b")
        assert (None + Ltree("a.b")) == Ltree("a.b")
        assert 42 + Ltree("a.b") + "" == Ltree("42.a.b")
        assert ["c", None, 42] + Ltree("a.b") == Ltree("c.42.a.b")

    def test_getitem(self):
        assert type(Ltree("a")[0]) is str

        t = Ltree("foo.bar.baz")
        assert t[0] == "foo"
        assert t[1] == "bar"
        assert t[2] == "baz"
        assert t[-1] == "baz"
        assert t[-2] == "bar"
        assert t[-3] == "foo"

        for i in [3, 4, 5, -4, -5]:
            try:
                t[i]
            except IndexError:
                pass
            else:
                assert False

    def test_slice(self):
        assert type(Ltree()[1:2]) is Ltree
        assert type(Ltree()[slice(1, 2)]) is Ltree

        t = Ltree("foo.bar.baz")
        assert t[0:3] == "foo.bar.baz"
        assert t[1:2] == "bar"
        assert t[1:] == "bar.baz"
        assert t[:-1] == "foo.bar"
        assert t[0:0] == ""


class TestStar:
    def test_parse(self):
        for a, b in [
            ("*", Star(None, None)),
            ("*{2}", Star(2, 2)),
            ("*{9999}", Star(9999, 9999)),
            ("*{,2}", Star(None, 2)),
            ("*{2,}", Star(2, None)),
            ("*{2,4}", Star(2, 4)),
            ("*{222,444}", Star(222, 444)),
        ]:
            try:
                s = Star.parse(a)
            except ValueError as e:
                assert False, "error parsing %r: %s" % (a, e)
            assert s == b

    def test_bad_parse(self):
        for (a,) in [
            ("*{}",),
            ("*{,,2}",),
            ("*{-9999}",),
            ("*{,2,}",),
            ("*{2,,}",),
        ]:
            try:
                s = Star.parse(a)
            except ValueError as e:
                assert False, "error parsing %r: %s" % (a, e)
            assert s is None

    def test_star_fusion_init(self):
        q = Lquery("foo.*.*.bar")
        assert q == "foo.*.bar"
        assert len(q) == 3

    def test_star_fusion_join(self):
        q = Lquery(["foo", "*{1}", "*{1}", "bar"])
        assert str(q) == "foo.*{2}.bar"

        q = Lquery(["foo", "*{1,2}", "*{3,}", "bar"])
        assert str(q) == "foo.*{4,}.bar"

    def test_rfusion(self):
        q = Lquery(["foo"])
        q += "bar"
        assert str(q) == "foo.bar"
        q += "*{2}"
        assert str(q) == "foo.bar.*{2}"
        q += "*{1}"
        assert str(q) == "foo.bar.*{3}"
        q += "baz"
        assert str(q) == "foo.bar.*{3}.baz"
        q += "*{1,1}"
        assert str(q) == "foo.bar.*{3}.baz.*{1}"
        q += "*{1}"
        assert str(q) == "foo.bar.*{3}.baz.*{2}"

    def test_or(self):
        assert str(Lquery(["foo|bar"])) == "foo|bar"


class TestAdaptLtree:
    def test_adapt(self, conn, ltree):
        info = TypeInfo.fetch(conn, "ltree")
        assert info
        register_ltree(info, conn)
        assert conn.adapters.types[info.oid].name == "ltree"

        cur = conn.execute("select null::ltree, ''::ltree, 'a.b'::ltree")
        assert cur.fetchone() == (None, Ltree(), Ltree("a.b"))

    samp = [Ltree(""), Ltree("a.b.c.d")]

    @pytest.mark.parametrize("t", samp)
    def test_roundtrip(self, ltree, conn, t):
        register_ltree(TypeInfo.fetch(conn, "ltree"), conn)
        cur = conn.cursor()
        t1 = cur.execute("select %s", [t]).fetchone()[0]
        assert type(t) is type(t1)
        assert t == t1

    def test_roundtrip_array(self, ltree, conn):
        register_ltree(TypeInfo.fetch(conn, "ltree"), conn)
        samp1 = conn.execute("select %s", (self.samp,)).fetchone()[0]
        assert samp1 == self.samp


class TestAdaptLquery:
    def test_adapt(self, conn, ltree):
        info = TypeInfo.fetch(conn, "lquery")
        assert info
        register_lquery(info, conn)
        assert conn.adapters.types[info.oid].name == "lquery"

        cur = conn.execute("select null::lquery, 'a.*.b'::lquery")
        assert cur.fetchone() == (None, Lquery("a.*.b"))

    samp = [Lquery("a"), Lquery("a.*{,3}.c.*.d")]

    @pytest.mark.parametrize("t", samp)
    def test_roundtrip(self, ltree, conn, t):
        register_lquery(TypeInfo.fetch(conn, "lquery"), conn)
        cur = conn.cursor()
        t1 = cur.execute("select %s", [t]).fetchone()[0]
        assert type(t) is type(t1)
        assert t == t1

    def test_roundtrip_array(self, ltree, conn):
        register_lquery(TypeInfo.fetch(conn, "lquery"), conn)
        samp1 = conn.execute("select %s", (self.samp,)).fetchone()[0]
        assert samp1 == self.samp
