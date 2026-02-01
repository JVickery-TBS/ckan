# encoding: utf-8
import re

from datetime import date, datetime
import pytest
import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.exc import ProgrammingError

import ckanext.datastore.backend.postgres as db
import ckanext.datastore.backend.postgres as postgres_backend
import ckanext.datastore.helpers as datastore_helpers


class TestTypeGetters(object):
    def test_get_list(self):
        get_list = datastore_helpers.get_list
        assert get_list(None) is None
        assert get_list([]) == []
        assert get_list("") == []
        assert get_list("foo") == ["foo"]
        assert get_list("foo, bar") == ["foo", "bar"]
        assert get_list('foo_"bar, baz') == ['foo_"bar', "baz"]
        assert get_list('"foo", "bar"') == ["foo", "bar"]
        assert get_list(u"foo, bar") == ["foo", "bar"]
        assert get_list(["foo", "bar"]) == ["foo", "bar"]
        assert get_list([u"foo", u"bar"]) == ["foo", "bar"]
        assert get_list(["foo", ["bar", "baz"]]) == ["foo", ["bar", "baz"]]

    def test_is_single_statement(self):
        singles = [
            "SELECT * FROM footable",
            'SELECT * FROM "bartable"',
            'SELECT * FROM "bartable";',
            'SELECT * FROM "bart;able";',
            "select 'foo'||chr(59)||'bar'",
        ]

        multiples = [
            "SELECT * FROM abc; SET LOCAL statement_timeout to"
            "SET LOCAL statement_timeout to; SELECT * FROM abc",
            'SELECT * FROM "foo"; SELECT * FROM "abc"',
        ]

        for single in singles:
            assert postgres_backend.is_single_statement(single) is True

        for multiple in multiples:
            assert postgres_backend.is_single_statement(multiple) is False

    @pytest.mark.ckan_config(
        "ckan.datastore.default_fts_index_field_types", "text tsvector")
    def test_should_fts_index_field_type(self):
        indexable_field_types = ["tsvector", "text"]

        non_indexable_field_types = [
            "nested",
            "timestamp",
            "date",
            "_text",
            "text[]",
        ]

        for indexable in indexable_field_types:
            assert (
                datastore_helpers.should_fts_index_field_type(indexable)
                is True
            )

        for non_indexable in non_indexable_field_types:
            assert (
                datastore_helpers.should_fts_index_field_type(non_indexable)
                is False
            )


@pytest.mark.ckan_config("ckan.plugins", "datastore")
@pytest.mark.usefixtures("clean_datastore", "with_plugins")
class TestGetTables(object):
    def test_get_table_names(self):
        engine = db.get_write_engine()
        session = orm.scoped_session(orm.sessionmaker(bind=engine))
        create_tables = [
            "CREATE TABLE test_a (id_a text)",
            "CREATE TABLE test_b (id_b text)",
            'CREATE TABLE "TEST_C" (id_c text)',
            'CREATE TABLE test_d ("α/α" integer)',
        ]
        for create_table_sql in create_tables:
            session.execute(sa.text(create_table_sql))

        test_cases = [
            (u"SELECT * FROM test_a", ["test_a"]),
            (u"SELECT * FROM public.test_a", ["test_a"]),
            (u'SELECT * FROM "TEST_C"', ["TEST_C"]),
            (u'SELECT * FROM public."TEST_C"', ["TEST_C"]),
            (u"SELECT * FROM pg_catalog.pg_database", ["pg_database"]),
            (u"SELECT rolpassword FROM pg_roles", ["pg_authid"]),
            (
                u"""SELECT p.rolpassword
                FROM pg_roles p
                JOIN test_b b
                ON p.rolpassword = b.id_b""",
                ["pg_authid", "test_b"],
            ),
            (
                u"""SELECT id_a, id_b, id_c
                FROM (
                    SELECT *
                    FROM (
                        SELECT *
                        FROM "TEST_C") AS c,
                        test_b) AS b,
                    test_a AS a""",
                ["test_a", "test_b", "TEST_C"],
            ),
            (u"INSERT INTO test_a VALUES ('a')", ["test_a"]),
            (u'SELECT "α/α" FROM test_d', ["test_d"]),
            (u'SELECT "α/α" FROM test_d WHERE "α/α" > 1000', ["test_d"]),
        ]

        context = {"connection": session.connection()}
        for case in test_cases:
            assert sorted(
                datastore_helpers.get_table_and_function_names_from_sql(context, case[0])[0]
            ) == sorted(case[1])


@pytest.mark.ckan_config("ckan.plugins", "datastore")
@pytest.mark.usefixtures("clean_datastore", "with_plugins")
class TestGetFunctions(object):
    def test_get_function_names(self):

        engine = db.get_write_engine()
        session = orm.scoped_session(orm.sessionmaker(bind=engine))
        create_tables = [
            u"CREATE TABLE test_a (id int, period date, subject_id text, result decimal)",
            u"CREATE TABLE test_b (name text, subject_id text)",
        ]
        for create_table_sql in create_tables:
            session.execute(sa.text(create_table_sql))

        test_cases = [
            (u"SELECT max(id) from test_a", ["max"]),
            (u"SELECT count(distinct(id)) FROM test_a", ["count", "distinct"]),
            (u"SELECT trunc(avg(result),2) FROM test_a", ["trunc", "avg"]),
            (u"SELECT trunc(avg(result),2), avg(result) FROM test_a", ["trunc", "avg"]),
            (u"SELECT * from pg_settings", ["pg_show_all_settings"]),
            (u"SELECT * from pg_settings UNION SELECT * from pg_settings", ["pg_show_all_settings"]),
            (u"SELECT * from (SELECT * FROM pg_settings) AS tmp", ["pg_show_all_settings"]),
            (u"SELECT query_to_xml('SELECT max(id) FROM test_a', true, true , '')", ["query_to_xml"]),
            (u"select $$'$$, query_to_xml($X$SELECT table_name FROM information_schema.tables$X$,true,true,$X$$X$), $$'$$", ["query_to_xml"])
        ]

        context = {"connection": session.connection()}
        for case in test_cases:
            assert sorted(
                datastore_helpers.get_table_and_function_names_from_sql(context, case[0])[1]
            ) == sorted(case[1])

    def test_get_function_names_custom_function(self):

        engine = db.get_write_engine()
        session = orm.scoped_session(orm.sessionmaker(bind=engine))
        create_tables = [
            u"""CREATE FUNCTION add(integer, integer) RETURNS integer
                AS 'select $1 + $2;'
                    LANGUAGE SQL
                        IMMUTABLE
                            RETURNS NULL ON NULL INPUT;
            """
        ]
        for create_table_sql in create_tables:
            session.execute(sa.text(create_table_sql))

        context = {"connection": session.connection()}

        sql = "SELECT add(1, 2);"

        assert datastore_helpers.get_table_and_function_names_from_sql(context, sql)[1] == ["add"]

    def test_get_function_names_crosstab(self):
        """
        Crosstab functions need to be enabled in the database by executing the following using
        a super user:

            CREATE extension tablefunc;

        """

        engine = db.get_write_engine()
        session = orm.scoped_session(orm.sessionmaker(bind=engine))
        create_tables = [
            u"CREATE TABLE test_a (id int, period date, subject_id text, result decimal)",
            u"CREATE TABLE test_b (name text, subject_id text)",
        ]
        for create_table_sql in create_tables:
            session.execute(sa.text(create_table_sql))

        test_cases = [
            (u"""SELECT *
                FROM crosstab(
                    'SELECT extract(month from period)::text, test_b.name, trunc(avg(result),2)
                     FROM test_a, test_b
                     WHERE test_a.subject_id = test_b.subject_id')
                     AS final_result(month text, subject_1 numeric,subject_2 numeric);""",
                ['crosstab', 'final_result', 'extract', 'trunc', 'avg']),
        ]

        context = {"connection": session.connection()}
        try:
            for case in test_cases:
                assert sorted(
                    datastore_helpers.get_table_and_function_names_from_sql(context, case[0])[1]
                ) == sorted(case[1])
        except ProgrammingError as e:
            if bool(re.search("function crosstab(.*) does not exist", str(e))):
                pytest.skip("crosstab functions not enabled in DataStore database")


def test_datastore_bucket_histogram():
    dbh = datastore_helpers.datastore_bucket_histogram
    HistogramBar = datastore_helpers.HistogramBar

    assert dbh([
        {"id": "all_null", "buckets": [], "edges": [], "nulls": 6, "type": "int4"},
        {"id": "one", "buckets": [6], "edges": [1], "nulls": 0, "type": "numeric"},
    ]) == {
        "all_null": [],
        "one": [HistogramBar(1.0, 1.0, 1, 1)],
    }

    assert dbh([
        {
            "id": "short",
            "buckets": [4, 2, 0, 1],
            "edges": [2, 3, 4, 5],
            "nulls": 0,
            "type": "int4",
        },
        {
            "id": "nums",
            "buckets": [2, 0, 1, 4],
            "edges": [-4, 2, 8, 14, 20],
            "nulls": 0,
            "type": "numeric",
        },
    ]) == {
        "short": [
            HistogramBar(.25, 1, 2, 2),
            HistogramBar(.25, .5, 3, 3),
            HistogramBar(.25, 0, 4, 4),
            HistogramBar(.25, .25, 5, 5),
        ],
        "nums": [
            HistogramBar(.25, .5, -4, 2),
            HistogramBar(.25, 0, 2, 8),
            HistogramBar(.25, .25, 8, 14),
            HistogramBar(.25, 1, 14, 20),
        ],
    }

    assert dbh([
        {
            "id": "days",
            "buckets": [6, 4, 0, 8],
            "edges": [
                date(2026, 1, 1),
                date(2026, 1, 7),
                date(2026, 1, 9),
                date(2026, 1, 15),
                date(2026, 1, 16),
            ],
            "nulls": 0,
            "type": "date",
        },
        {
            "id": "ts",
            "buckets": [6, 4, 0, 8],
            "edges": [
                datetime(2026, 1, 1),
                datetime(2026, 1, 7),
                datetime(2026, 1, 9),
                datetime(2026, 1, 15),
                datetime(2026, 1, 17),
            ],
            "nulls": 0,
            "type": "timestamp",
        }
    ]) == {
        "days": [
            HistogramBar(0.375, 0.25, date(2026, 1, 1), date(2026, 1, 6)),
            HistogramBar(0.125, 0.5, date(2026, 1, 7), date(2026, 1, 8)),
            HistogramBar(0.375, 0.0, date(2026, 1, 9), date(2026, 1, 14)),
            HistogramBar(0.125, 1.0, date(2026, 1, 15), date(2026, 1, 16)),
        ],
        "ts": [
            HistogramBar(0.375, 0.25, datetime(2026, 1, 1), datetime(2026, 1, 7)),
            HistogramBar(0.125, 0.5, datetime(2026, 1, 7), datetime(2026, 1, 9)),
            HistogramBar(0.375, 0.0, datetime(2026, 1, 9), datetime(2026, 1, 15)),
            HistogramBar(0.125, 1.0, datetime(2026, 1, 15), datetime(2026, 1, 17)),
        ],
    }

