

def pg_options(url, table, user, password):
    return {
        "url": url,
        "dbtable": table,
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }