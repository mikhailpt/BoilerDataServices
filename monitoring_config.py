"""
Конфигурация мониторинга OPC UA.

Структура:
  - Список серверов (name, url, description)
  - Для каждого сервера — список приборов, за которыми нужно следить
    (key — уникальный идентификатор прибора в системе,
     object_name — имя объекта на OPC UA сервере,
     description — произвольное описание)
"""

MONITORING_CONFIG = [
    {
        "name": "LocalServer",
        "url": "opc.tcp://localhost:4840",
        "description": "Локальный тестовый OPC UA сервер",
        "devices": [
            {
                "key": "ViessmannBoiler",
                "object_name": "ViessmannBoiler",
                "description": "Котёл Viessmann (Vcontrold)"
            },
            # {
            #     "key": "TRM138_Controller",
            #     "object_name": "TRM138_8Ch_Regulator",
            #     "description": "Temperature"
            # },
            {
                "key": "AirAnalyserBonerP23_08",
                "object_name": "AirAnalyserBonerP23_08",
                "description": "Газовый анализатор Бонэр Тест-1 П.23-08"
            },
	    {
                "key": "AirAnalyserBonerP20_001",
                "object_name": "AirAnalyserBonerP20_001",
                "description": "Газовый анализатор Бонэр Тест-1 П.20-001"
            },
        ]
    },
    {
        "name": "PLCServer",
        "url": "opc.tcp://192.168.0.100:4840",
        "description": "OPC UA сервер на ПЛК",
        "devices": [
            {
                "key": "PLC_PRG",
                "object_name": "PLC_PRG",
                "description": "Контроллер горелки на ПЛК"
            },
            {
                "key": "Gas",
                "object_name": "Gas",
                "description": "Встроенный газоанализатор на ПЛК"
            }
        ]
    }
]
