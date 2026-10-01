DEVICES = {
    "ViessmannBoiler": {
        "params": "devices_params.boiler_params.BOILER_PARAMS",
        "client" : "devices_clients.VcontroldClient.VcontroldTelnetClient",
        "object_name": "ViessmannBoiler",
        "enabled": True
    },

    "TRM138_Controller": {
        "params": "devices_params.trm138_params.TRM138_PARAMS",
        "client": "devices_clients.trm138_client.TRM138Client",
        "object_name": "TRM138_8Ch_Regulator",
        "enabled": False
    },

    "AirAnalyserBonerP20_001": {
        "params": "devices_params.air_analyser_params.AIR_ANALYSER_BONERP20_001_PARAMS",
        "client": "devices_clients.air_analyser_client.AirAnalyserBonerP20_001_Client",
        "object_name": "AirAnalyserBonerP20_001",
        "enabled": False
    },

    "AirAnalyserBonerP23_08": {
        "params": "devices_params.air_analyser_params.AIR_ANALYSER_BONERP23_08_PARAMS",
        "client": "devices_clients.air_analyser_client.AirAnalyserBonerP23_08_Client",
        "object_name": "AirAnalyserBonerP23_08",
        "enabled": True
    }
}
