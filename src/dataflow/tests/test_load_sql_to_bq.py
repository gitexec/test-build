import pytest
import pymysql

import logging

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)


@pytest.mark.skip(reason='. Needs template pre-launched; hard-coded to local')
def test_it_load_sql_to_bq(env, run, bigquery):
    conn = pymysql.connect(host='mysql',
                           user='root',
                           db='pyr_bluesun_local')

    with conn.cursor() as curs:
        curs.execute('TRUNCATE TABLE tree_users')
        conn.commit()

        sql = """
        INSERT INTO tree_users (id, first_name, last_name, created_date, birth_date) VALUES
        (1, 'mace', 'windu', '2001-10-01', '1900-01-01'),
        (2, 'chancellor', 'palpatine', '1976-01-01', NULL),
        (3, 'obi-wan', 'kenobi', '1978-01-01', NULL);
        """
        curs.execute(sql)

        conn.commit()

    run('load_sql_to_bq',
        client='bluesun',
        env='local',
        bq_table='{}:pyr_bluesun_local.tree_users'.format(env['project']),
        table='tree_users', key_field='id')

    rs = bigquery.query('SELECT * FROM `{}.{}.{}`'.format(env['project'], 'pyr_bluesun_local', 'tree_users'))

    assert len(rs) == 3
