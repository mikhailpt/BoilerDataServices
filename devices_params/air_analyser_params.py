from asyncua import ua

AIR_ANALYSER_BONERP20_001_PARAMS = {
    "O2": {
        "command": "O2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация кислорода"
    },
    "H2": {
        "command": "H2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация водорода"
    },
    "CO": {
        "command": "CO",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация угарного газа"
    },
    "CO2": {
        "command": "CO2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация углекислого газа"
    },
    "CH4": {
        "command": "CH4",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация метана"
    },
    "SO2": {
        "command": "SO2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация диоксида серы"
    },
    "NO": {
        "command": "NO",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация оксида азота"
    },
    "NO2": {
        "command": "NO2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация диоксида азота"
    },
    "N2O": {
        "command": "N2O",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация закиси азота"
    },
    "InternalTemp": {
        "command": "Tv",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "°C",
        "description": "Внутренняя температура прибора"
    },
    "Pressure": {
        "command": "P",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "мм.рт.ст",
        "description": "Давление"
    }
}

AIR_ANALYSER_BONERP23_08_PARAMS = {
    "O2": {
        "command": "O2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация кислорода"
    },
    "H2": {
        "command": "H2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация водорода"
    },
    "CO": {
        "command": "CO",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация угарного газа"
    },
    "CO2": {
        "command": "CO2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация углекислого газа"
    },
    "CH4": {
        "command": "CH4",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "%",
        "description": "Концентрация метана"
    },
    "COн": {
        "command": "COн",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация угарного газа в ppm"
    },
    "COнн": {
        "command": "COнн",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация угарного газа в ppm v2"
    },
    "NO": {
        "command": "NO",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация оксида азота"
    },
    "NO2": {
        "command": "NO2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация диоксида азота"
    },
    "SO2": {
        "command": "SO2",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация диоксида серы"
    },
    "H2S": {
        "command": "H2S",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация сероводорода"
    },
    "H2н": {
        "command": "H2н",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "ppm",
        "description": "Концентрация водорода в ppm"
    },
    "InternalTemp": {
        "command": "Tv",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "°C",
        "description": "Внутренняя температура прибора"
    },
    "Pressure": {
        "command": "P",
        "data_type": ua.VariantType.Float,
        "default": 0.0,
        "writable": False,
        "unit": "мм.рт.ст",
        "description": "Давление"
    }
}