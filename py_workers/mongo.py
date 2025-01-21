from pymongo import MongoClient

def db_from_config(config, verbose=False):
    """
    Initialize db if necessary: create the sole non-admin user
    """
    if config["database"].get("srv, False") is True:
        conn_string = "mongodb+srv://"
    else:
        conn_string = "mongodb://"

    use_auth = False
    if (
        config["database"]["username"] is not None
        and config["database"]["password"] is not None
    ):
        conn_string += f"{config['database']['username']}:{config['database']['password']}@"
        use_auth = True

    conn_string += f"{config['database']['host']}"
    if config["database"]["srv"] is not True:
        conn_string += f":{config['database']['port']}"

    conn_string += f"/{config['database']['name']}?directConnection=true"

    if use_auth:
        conn_string += f"&authSource=admin"

    if config["database"]["replica_set"] is not None:
        conn_string += f"&replicaSet={config['database']['replica_set']}"

    if config["database"]["max_pool_size"] is not None:
        conn_string += f"&maxPoolSize={config['database']['max_pool_size']}"

    client = MongoClient(conn_string)

    db = client[config["database"]["name"]]
    return db
