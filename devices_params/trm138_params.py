from asyncua import ua

TRM138_PARAMS = {
    "Ch1_Temperature": {
        "command": "read_temp_fp:1",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "°C",
        "description": "Температура канала 1 (датчик)"
    },
    # "Ch1_SetPoint": {
    #     "command": "read_set_point:1",
    #     "data_type": ua.VariantType.Int16,
    #     "default": 25,
    #     "writable": True,
    #     "unit": "°C",
    #     "description": "Уставка канала 1"
    # },
    # "Ch1_Hysteresis": {
    #     "command": "read_hyst:1",
    #     "data_type": ua.VariantType.Int16,
    #     "default": 1,
    #     "writable": True,
    #     "unit": "°C",
    #     "description": "Гистерезис канала 1"
    # },
    # "Ch1_Output": {
    #     "command": "read_out_state:1",
    #     "data_type": ua.VariantType.Boolean,
    #     "default": False,
    #     "writable": True,
    #     "unit": "",
    #     "description": "Состояние выхода канала 1 (реле)"
    # },
    # "Ch1_Error": {
    #     "command": "read_error:1",
    #     "data_type": ua.VariantType.Int16,
    #     "default": 0,
    #     "writable": False,
    #     "unit": "",
    #     "description": "Код ошибки датчика канала 1"
    # },
    # "Ch2_Temperature": {
    #     "command": "read_temp_fp:2",
    #     "data_type": ua.VariantType.Float,
    #     "default": 0.0,
    #     "writable": False,
    #     "unit": "°C",
    #     "description": "Температура канала 2"
    # },
    # "Ch2_SetPoint": {
    #     "command": "read_set_point:2",
    #     "data_type": ua.VariantType.Int16,
    #     "default": 25,
    #     "writable": True,
    #     "unit": "°C",
    #     "description": "Уставка канала 2"
    # },
    # "Ch2_Output": {
    #     "command": "read_out_state:2",
    #     "data_type": ua.VariantType.Boolean,
    #     "default": False,
    #     "writable": True,
    #     "unit": "",
    #     "description": "Состояние выхода канала 2"
    # },
}