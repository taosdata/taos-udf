import taos
import pytest
import datetime

rows = 3
times = 1;

@pytest.fixture(scope="module", name="conn")
def setup():
    conn = taos.connect()
    conn = taos.connect()
    try:
        conn.execute('drop function udf1')
    except:
        pass
    try:
        conn.execute('drop function udf2')
    except:
        pass
    try:
        conn.execute('drop database udf')
    except:
        pass
    conn.execute('create database udf')
    conn.select_db('udf')
    conn.execute('create table t(ts timestamp, i int)')
    sql = 'insert into t values'
    for i in range(rows):
        sql = sql + '(now + {}a, {})'.format(i, i)
    conn.execute(sql)
    conn.execute("create function udf1 as './udf1.py' outputtype int language 'python'")
    conn.execute("create aggregate function udf2 as './udf2.py' outputtype int bufSize 128 language 'python'")

    yield conn

    conn.execute('drop function udf1');
    conn.execute('drop function udf2');
    conn.execute('drop database udf')
    conn.close()

def test_udf(conn):
    t1 = datetime.datetime.now()
    for i in range(times):
        q = conn.query('select udf1(i) from udf.t')
        r = q.fetch_all()
    t2 = datetime.datetime.now()
    print('scalar time:', t2-t1)
    assert len(r) == rows
    assert len(r[0]) == 1
    for i in range(rows):
        assert r[i][0] == i
    
    print("aggregate test")
    t3 = datetime.datetime.now()
    for i in range(times):
        q2 = conn.query('select udf2(i) from udf.t')
        r2 = q2.fetch_all()
    t4 = datetime.datetime.now()
    print('agg time:', t4-t3)
    assert len(r2) == 1
    assert len(r2[0]) == 1
    assert r2[0][0] == 3
