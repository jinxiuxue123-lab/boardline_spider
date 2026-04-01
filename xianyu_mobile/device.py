import time
import uiautomator2 as u2


class AndroidDevice:
    def __init__(self, serial: str | None = None):
        self.serial = serial
        self.d = None

    def connect(self):
        self.d = u2.connect(self.serial) if self.serial else u2.connect()
        self.d.implicitly_wait(10.0)
        return self.d

    def screenshot(self, path: str):
        if not self.d:
            raise RuntimeError("设备未连接")
        self.d.screenshot(path)

    def click_text_if_exists(self, text: str, timeout: int = 3) -> bool:
        if not self.d:
            raise RuntimeError("设备未连接")
        obj = self.d(text=text)
        if obj.wait(timeout=timeout):
            obj.click()
            return True
        return False

    def click_desc_if_exists(self, desc: str, timeout: int = 3) -> bool:
        if not self.d:
            raise RuntimeError("设备未连接")
        obj = self.d(description=desc)
        if obj.wait(timeout=timeout):
            obj.click()
            return True
        return False

    def set_text_by_focused(self, text: str):
        if not self.d:
            raise RuntimeError("设备未连接")
        self.d.send_keys(text, clear=True)

    def wait(self, seconds: float):
        time.sleep(seconds)