import sqlite3


def init_db(db_path: str = "opc_readings.db") -> sqlite3.Connection:
    """
    Создаёт структуру БД из четырёх таблиц:

        servers
          └─ devices          (FK → servers)
                └─ device_params  (FK → devices)
                     └─ readings       (FK → device_params)

    Таблица servers        — характеристики OPC UA серверов
    Таблица devices        — приборы, зарегистрированные на каждом сервере
                              (имя, объект OPC UA, описание)
    Таблица device_params  — параметры/узлы каждого прибора,
                              обнаруженные через OPC UA browsing
    Таблица readings       — показания: ссылка на параметр прибора +
                              значение + серверная метка времени
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")

    # ------------------------------------------------------------------
    # 1. Серверы
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # 2. Приборы — ссылаются на сервер
    #    device_key  — идентификатор в системе мониторинга
    #    object_name — имя объекта на OPC UA сервере (для browsing)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # 3. Параметры (узлы OPC UA) каждого прибора
    #    node_id    — строковое представление NodeId
    #    param_name — BrowseName узла
    #    description, unit — считываются из свойств OPC UA при первом обходе
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # 4. Показания — только ссылка на параметр + значение + серверное время
    #    server_timestamp — берётся из DataValue.ServerTimestamp OPC UA,
    #                       при недоступности — клиентское UTC-время
    # ------------------------------------------------------------------
    conn.execute('''
        CREATE TABLE IF NOT EXISTS readings (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            device_param_id  INTEGER NOT NULL,
            value            TEXT,
            server_timestamp TEXT    NOT NULL,
            FOREIGN KEY (device_param_id) REFERENCES device_params(id)
        )
    ''')

    # ------------------------------------------------------------------
    # Индексы для ускорения выборок
    # ------------------------------------------------------------------
    conn.execute('CREATE INDEX IF NOT EXISTS idx_readings_param    ON readings(device_param_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_readings_ts       ON readings(server_timestamp)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_devices_server    ON devices(server_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_params_device     ON device_params(device_id)')

    conn.commit()
    return conn
