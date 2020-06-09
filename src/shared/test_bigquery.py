import pytest
import re
from google.cloud import bigquery
from .bigquery import BigQuery
from .bigquery import QueryBuilder


@pytest.fixture()
def check():
    def _c(builder, expected):
        assert re.sub('\n| +', ' ', str(builder).strip()) == re.sub('\n| +', ' ', str(expected).strip())
    return _c


def test_bigquery_querybuilder_werkz():
    BigQuery.querybuilder(from_='test', select=['id', 'name'])


def test_build_insert_query(check):
    builder = QueryBuilder(
        table='test',
        insert=[
            {'id': 1, 'name': 'Darth Maul'},
            {'id': 2, 'name': 'Darth Sidius'},
            {'id': 3, 'name': 'Darth Vader'}
        ]
    )

    exp = """INSERT INTO test (id, name) VALUES (1,'Darth Maul'), (2,'Darth Sidius'), (3,'Darth Vader')"""
    check(builder, exp)


def test_build_basic_select(check):
    builder = QueryBuilder(
        from_='test',
        select=['id', ('field', 'alias')]
    )
    check(builder, 'SELECT id, field AS alias FROM test')


def test_build_from_alias(check):
    builder = QueryBuilder(
        select=['id'],
        from_=('test', 't')
    )
    check(builder, 'SELECT id FROM test AS t')


def test_build_from_list(check):
    builder = QueryBuilder(
        select=['id', 't.*'],
        from_=['tbl1', ('test', 't')]
    )
    check(builder, 'SELECT id, t.* FROM tbl1, test AS t')


def test_build_from_builder(check):
    check(QueryBuilder(
        from_=['tst',
               (QueryBuilder(
                   from_=('tst2'),
                   select=['tmp', 'fld2']
               ), 't2')],
        select=['tst.*', ('t2.fld2', 'f2')]
    ), 'SELECT tst.*, t2.fld2 AS f2 FROM tst, (SELECT tmp, fld2 FROM tst2 ) AS t2')


def test_build_select_with_where_constant(check):
    builder = QueryBuilder(
        from_='test',
        select=['id', ('field', 'alias')],
        where=['const = 1']
    )
    check(builder, 'SELECT id, field AS alias FROM test WHERE const = 1')


def test_build_select_with_where_params(check):
    builder = QueryBuilder(
        from_='test',
        select=['id', ('field', 'alias')],
        where=[('var = @variable', ('variable', 'a string')),
               ('int_value = @int', ('int', 24))]
    )
    check(builder, 'SELECT id, field AS alias FROM test WHERE var = @variable AND int_value = @int')

    assert builder()[1] == [
        bigquery.ScalarQueryParameter('variable', 'STRING', 'a string'),
        bigquery.ScalarQueryParameter('int', 'INT64', 24),
        bigquery.ScalarQueryParameter('variable', 'STRING', 'a string'),
        bigquery.ScalarQueryParameter('int', 'INT64', 24),
    ]


def test_build_union_strings(check):
    check(QueryBuilder(
        union=('distinct', ['SELECT 1 as id, "Mickey Mouse" AS "name"',
                            'SELECT 2, "Donald Duck"'])
    ), 'SELECT 1 as id, "Mickey Mouse" AS "name" UNION DISTINCT SELECT 2, "Donald Duck"')


def test_build_union_querybuilders(check):
    check(QueryBuilder(
        union=('distinct', [
            QueryBuilder(
                select=['id', 'name'],
                from_=['tbl1', ('long_table_name', 'ltn')]
            ),
            QueryBuilder(
                select=[(1, 'id'), '"Donald Duck"']
            )
        ])
    ), 'SELECT id, name FROM tbl1, long_table_name AS ltn\n UNION DISTINCT SELECT 1 AS id, "Donald Duck"')


def test_build_order(check):
    check(QueryBuilder(
        from_='tbl',
        select='id',
        order=['id', ('ts', 'desc')]
    ), 'SELECT i, d FROM tbl ORDER BY id ASC, ts DESC')


def test_build_limit(check):
    check(QueryBuilder(
        from_='tbl',
        select='id',
        limit=75
    ), 'SELECT i, d FROM tbl LIMIT 75')
