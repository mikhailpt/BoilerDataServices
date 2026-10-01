import serial
import serial.tools.list_ports

def findPort():
    for port in serial.tools.list_ports.comports():
        if port.vid == 1027 and port.pid == 24577:
            print(f"Авто-обнаружен порт газоанализатора: {port.device}")
            return
    print("Порт газоанализатора не найден. Проверьте подключение.")

findPort()