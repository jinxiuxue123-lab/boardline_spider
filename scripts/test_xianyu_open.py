from xianyu_mobile.device import AndroidDevice
from xianyu_mobile.app import XianyuApp


def main():
    device = AndroidDevice(serial="246bff5d")
    device.connect()

    app = XianyuApp(device)
    app.start()

    device.screenshot("xianyu_home.png")
    print("已打开闲鱼，并截图到 xianyu_home.png")


if __name__ == "__main__":
    main()