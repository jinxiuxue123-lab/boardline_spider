from xianyu_mobile.device import AndroidDevice
from xianyu_mobile.app import XianyuApp


def main():
    device = AndroidDevice(serial="246bff5d")
    device.connect()

    app = XianyuApp(device)
    app.start()
    app.ensure_home()

    ok = app.open_publish_page()
    device.screenshot("xianyu_publish_entry_check.png")

    if ok:
        print("已找到发布入口，并截图到 xianyu_publish_entry_check.png")
    else:
        print("没找到发布入口，已截图到 xianyu_publish_entry_check.png")


if __name__ == "__main__":
    main()