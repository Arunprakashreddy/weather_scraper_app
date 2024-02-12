import connection as connection
from mysql.connector import (connection)


def get_connection(user, password, host, type, db):
    con = None
    if type == 'mysql':
        con = connection.MySQLConnection(user=user,
                                         password=password,
                                         host=host,
                                         database= db)
    if con == None:
        print("unable to create connection to database")
    else:
        print("connection created....!")
    return con


def close_connection(con):
    con.close()
    print("connection closed....!")


def close_cursor(cur):
    cur.close()
    print("cursor closed....!")


def get_cursor(con):
    print("creating cursor....")
    return con.cursor()


def commit_connection(con):
    con.commit()
    print("commited transaction....!")


def execute_select_query(con, select_query):
    cur = None
    try:
        cur = get_cursor(con)
        cur.execute(select_query)
        r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
        return r
    except Exception:
        print("unable to execute select query")
    finally:
        close_cursor(cur)


def execute_update_query(con, update_query):
    cur = None
    try:
        cur = get_cursor(con)
        cur.execute(update_query)
        commit_connection(con)
    except Exception:
        print("unable to execute update query")
    finally:
        close_cursor(cur)


def batch_update_query(con, update_query, data):
    cur = None
    try:
        cur = get_cursor(con)
        cur.executemany(update_query, data)
        commit_connection(con)
    except Exception:
        print("unable to execute update query")
    finally:
        close_cursor(cur)
