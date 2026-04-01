from xianyu_mobile.device import AndroidDevice


class XianyuApp:
    PACKAGE_NAME = "com.taobao.idlefish"

    def __init__(self, device: AndroidDevice):
        self.device = device
        self.d = device.d

    def start(self):
        self.d.app_start(self.PACKAGE_NAME)
        self.device.wait(6)

    def stop(self):
        self.d.app_stop(self.PACKAGE_NAME)

    def ensure_home(self):
        for _ in range(5):
            if (
                self.d(text="首页").exists
                or self.d(text="消息").exists
                or self.d(text="我的").exists
                or self.d(text="会玩").exists
            ):
                return True
            self.d.press("back")
            self.device.wait(1)
        return False

    def open_publish_page(self) -> bool:
        # 先尝试常见文字入口
        text_candidates = [
            "发布",
            "发闲置",
            "卖闲置",
            "去发布",
            "我要卖",
        ]

        for text in text_candidates:
            obj = self.d(text=text)
            if obj.exists:
                obj.click()
                self.device.wait(4)
                return True

        # 再尝试描述入口
        desc_candidates = [
            "发布",
            "发闲置",
            "卖闲置",
        ]

        for desc in desc_candidates:
            obj = self.d(description=desc)
            if obj.exists:
                obj.click()
                self.device.wait(4)
                return True

        # 再尝试底部中间“+”类按钮
        resource_like_candidates = [
            self.d(descriptionContains="发布"),
            self.d(descriptionContains="闲置"),
            self.d(textContains="发布"),
            self.d(textContains="闲置"),
        ]

        for obj in resource_like_candidates:
            if obj.exists:
                obj.click()
                self.device.wait(4)
                return True

        return False