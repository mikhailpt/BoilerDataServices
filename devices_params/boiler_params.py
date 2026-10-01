from asyncua import ua

BOILER_PARAMS = {
    # Формат: "NodeName": {
    #     "command": "vcontrold command",
    #     "data_type": ua.VariantType.XXX,
    #     "default": начальное_значение,
    #     "writable": False,
    #     "unit": "°C",
    #     "description": "Описание"
    # }

    "FlueGasTemperature": {
        "command": "getTempAbgas",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "°C",
        "description": "Температура дымовых газов"
    },

    "BoilerTemperatureReturn": {
        "command": "getTempRueck",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "°C",
        "description": "Температура теплоносителя на входе"
    },

    "BoilerTemperature": {
        "command": "getTempKist",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "°C",
        "description": "Температура теплоносителя на выходе"
    },

    "BoilerWantedTemperature": {
        "command": "getTempKsoll",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "°C",
        "description": "Температура теплоносителя заданная пользователем котла"
    },

    "Sensor9Temperature": {
        "command": "getTempSensor9",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "°C",
        "description": "Температура с датчика 9"
    }
}