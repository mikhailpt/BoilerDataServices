import sqlite3


class DBConnection:
    def __init__(self, conn: sqlite3.Connection, db_path: str):
        self._conn = conn
        self.db_path = db_path

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        return self._conn.commit()

    def close(self):
        return self._conn.close()

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)


def init_db(db_path: str = "opc_readings.db") -> DBConnection:
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.execute('''
        CREATE TABLE IF NOT EXISTS servers (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL UNIQUE,
            url         TEXT    NOT NULL UNIQUE,
            description TEXT,
            is_active   INTEGER NOT NULL DEFAULT 1,
            created_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS devices (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            server_id   INTEGER NOT NULL,
            device_key  TEXT    NOT NULL,
            object_name TEXT    NOT NULL,
            description TEXT,
            is_active   INTEGER NOT NULL DEFAULT 1,
            created_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (server_id) REFERENCES servers(id),
            UNIQUE (server_id, device_key)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS device_params (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id   INTEGER NOT NULL,
            node_id     TEXT    NOT NULL,
            param_name  TEXT    NOT NULL,
            description TEXT,
            unit        TEXT,
            data_type   TEXT    NOT NULL DEFAULT "string",
            is_active   INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (device_id) REFERENCES devices(id),
            UNIQUE (device_id, node_id)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS readings (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            device_param_id  INTEGER NOT NULL,
            value            TEXT,
            server_timestamp TEXT    NOT NULL,
            FOREIGN KEY (device_param_id) REFERENCES device_params(id)
        )
    ''')

    conn.execute('CREATE INDEX IF NOT EXISTS idx_readings_param    ON readings(device_param_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_readings_ts       ON readings(server_timestamp)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_devices_server    ON devices(server_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_params_device     ON device_params(device_id)')

    conn.commit()
    return DBConnection(conn, db_path)


def recreate_connection(db_conn: DBConnection) -> DBConnection:
    db_path = db_conn.db_path
    try:
        db_conn.close()
    except Exception:
        pass
    return init_db(db_path)