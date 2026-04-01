from xianyu_mobile.device import AndroidDevice
from xianyu_mobile.app import XianyuApp


class XianyuPublisher:
    def __init__(self, device: AndroidDevice):
        self.device = device
        self.d = device.d
        self.app = XianyuApp(device)

    def open_app_and_publish_page(self):
        self.app.start()
        self.app.ensure_home()

        ok = self.app.open_publish_page()
        if not ok:
            raise RuntimeError("没有找到发布入口，请检查闲鱼当前页面")

    def fill_basic_fields(self, title: str, price: str, description: str):
        # 这里只做第一版通用文本填充
        # 不同版本闲鱼的字段名字可能不同，后面根据你手机实际页面再细化选择器

        title_candidates = ["标题", "填写标题和品牌更容易卖出", "宝贝标题"]
        price_candidates = ["价格", "请输入价格"]
        desc_candidates = ["描述", "宝贝描述", "分享宝贝细节"]

        self._fill_first_match(title_candidates, title)
        self._fill_first_match(price_candidates, price)
        self._fill_first_match(desc_candidates, description)

    def _fill_first_match(self, labels: list[str], value: str):
        for label in labels:
            if self.d(text=label).exists:
                self.d(text=label).click()
                self.device.wait(1)
                self.device.set_text_by_focused(value)
                self.device.wait(1)
                self.d.press("back")
                self.device.wait(1)
                return True
        return False