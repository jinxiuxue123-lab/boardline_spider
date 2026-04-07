import os
import json
import re
import time
import requests
from requests.exceptions import ConnectionError, ReadTimeout, SSLError, Timeout

CATEGORY_TITLE_RULES = {
    "滑雪板": {
        "category_terms": ["单板滑雪板"],
        "selling_points": ["稳定支撑", "灵活操控", "全山地", "公园刻滑", "进阶训练"],
        "scenes": ["单双板雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "固定器": {
        "category_terms": ["滑雪固定器"],
        "selling_points": ["快穿系统", "稳定包裹", "轻量支撑", "调节便捷", "雪场使用"],
        "scenes": ["单双板雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "滑雪鞋": {
        "category_terms": ["单板雪鞋"],
        "selling_points": ["包裹支撑", "保暖舒适", "穿脱便捷", "雪场使用", "稳定发力"],
        "scenes": ["单双板雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "滑雪镜": {
        "category_terms": ["雪镜"],
        "selling_points": ["一体式镜片", "防风防雪", "佩戴舒适", "视野清晰", "雪场使用"],
        "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "滑雪头盔": {
        "category_terms": ["滑雪头盔"],
        "selling_points": ["轻量防护", "可调节", "佩戴稳固", "雪场使用", "冬季防护"],
        "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "滑雪服": {
        "category_terms": ["滑雪服", "滑雪外套", "滑雪裤"],
        "selling_points": ["防风保暖", "雪场穿搭", "冬季滑雪装备", "舒适活动", "户外穿着"],
        "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "滑雪帽衫和中间层": {
        "category_terms": ["滑雪中层", "滑雪帽衫", "滑雪打底"],
        "selling_points": ["防风保暖", "舒适内搭", "雪场穿搭", "轻便保暖", "户外穿着"],
        "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "儿童装备": {
        "category_terms": ["儿童滑雪装备"],
        "selling_points": ["儿童雪场使用", "轻便保暖", "舒适穿着", "稳定支撑", "冬季装备"],
        "scenes": ["儿童雪场使用", "冬季滑雪装备", "儿童款"],
    },
    "手套": {
        "category_terms": ["滑雪手套"],
        "selling_points": ["防风保暖", "抓握稳固", "雪场使用", "冬季防寒", "佩戴舒适"],
        "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "帽子护脸": {
        "category_terms": ["滑雪护脸", "滑雪帽子", "滑雪面罩"],
        "selling_points": ["防风保暖", "冬季防寒", "雪场使用", "佩戴舒适", "面部防护"],
        "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
    },
    "袜子以及周边配件": {
        "category_terms": ["滑雪袜", "滑雪周边配件", "滑雪配件"],
        "selling_points": ["雪场使用", "穿戴便捷", "冬季装备", "配件补充", "日常备用"],
        "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
    },
}


def detect_children_subcategory(name: str) -> dict:
    normalized = (name or "").upper()

    if any(token in normalized for token in ("BALACLAVA", "HOOD", "NECK WARMER", "BEANIE", "FACE WARMER", "MASK")):
        return {
            "category_terms": ["儿童滑雪护脸", "儿童滑雪帽子"],
            "selling_points": ["护脸保暖", "防风防寒", "佩戴舒适", "雪场保暖"],
            "scenes": ["儿童雪场使用", "冬季户外穿戴", "儿童款"],
        }

    if any(token in normalized for token in ("BASE LAYER", "FIRST LAYER", "FLEECE", "MID LAYER", "LAYER SET")):
        return {
            "category_terms": ["儿童滑雪打底", "儿童滑雪中层"],
            "selling_points": ["贴身保暖", "舒适内搭", "轻便穿着", "雪场打底"],
            "scenes": ["儿童雪场使用", "冬季打底穿搭", "儿童款"],
        }

    if any(token in normalized for token in ("PACK", "BAG", "BACKPACK")):
        return {
            "category_terms": ["儿童滑雪包", "儿童滑雪配件"],
            "selling_points": ["轻便收纳", "外出携带", "日常通勤", "雪场出行"],
            "scenes": ["儿童雪场出行", "冬季外出装备", "儿童款"],
        }

    if any(token in normalized for token in ("BINDING", "BINDINGS")):
        return {
            "category_terms": ["儿童滑雪固定器"],
            "selling_points": ["稳定包裹", "轻量支撑", "调节便捷", "顺畅入门"],
            "scenes": ["儿童雪场使用", "单双板雪场使用", "儿童款"],
        }

    if any(token in normalized for token in ("BOOT", "BOOTS", "STEP ON")):
        return {
            "category_terms": ["儿童单板雪鞋"],
            "selling_points": ["包裹支撑", "保暖舒适", "穿脱便捷", "雪场练习"],
            "scenes": ["儿童雪场使用", "单板雪场使用", "儿童款"],
        }

    if re.search(r"(?<!\d)(1[0-7]\d)(?!\d)", normalized) or any(token in normalized for token in ("BOARD", "CUB-X", "GROM")):
        return {
            "category_terms": ["儿童单板滑雪板"],
            "selling_points": ["灵活操控", "稳定练习", "轻松上板", "进阶训练"],
            "scenes": ["儿童雪场使用", "单板雪场使用", "儿童款"],
        }

    if any(token in normalized for token in ("JACKET", "PANTS", "BIB", "SUIT")):
        return {
            "category_terms": ["儿童滑雪服"],
            "selling_points": ["防风保暖", "舒适活动", "雪场穿搭", "户外穿着"],
            "scenes": ["儿童雪场使用", "冬季雪场穿搭", "儿童款"],
        }

    return CATEGORY_TITLE_RULES["儿童装备"]


def detect_midlayer_subcategory(name: str) -> dict:
    normalized = (name or "").upper()

    if any(token in normalized for token in ("JACKET", "ANORAK", "COACH JACKET", "SHELL")):
        return {
            "category_terms": ["滑雪外套", "滑雪夹克"],
            "selling_points": ["防风保暖", "雪场穿搭", "户外穿着", "舒适活动"],
            "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
        }

    if any(token in normalized for token in ("PANTS", "PANT", "BIB", "OVERALL")):
        return {
            "category_terms": ["滑雪裤", "滑雪背带裤"],
            "selling_points": ["防风保暖", "活动舒适", "雪场穿搭", "户外穿着"],
            "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
        }

    if any(token in normalized for token in ("BASE LAYER", "FIRST LAYER", "INNER", "UNDER", "THERMAL")):
        return {
            "category_terms": ["滑雪打底", "滑雪内层"],
            "selling_points": ["贴身保暖", "舒适内搭", "雪场打底", "轻便穿着"],
            "scenes": ["雪场使用", "冬季打底穿搭", "男女通用"],
        }

    if any(token in normalized for token in ("MID LAYER", "FLEECE", "VEST", "INSULATOR", "INSULATED")):
        return {
            "category_terms": ["滑雪中层", "滑雪保暖层"],
            "selling_points": ["轻便保暖", "舒适内搭", "活动灵活", "雪场叠穿"],
            "scenes": ["雪场使用", "冬季滑雪装备", "男女通用"],
        }

    if any(token in normalized for token in ("HOODIE", "HOODY", "CREW", "SWEATSHIRT", "ZIP UP", "ZIPUP")):
        return {
            "category_terms": ["滑雪帽衫", "滑雪卫衣"],
            "selling_points": ["舒适穿着", "轻便保暖", "雪场穿搭", "日常外穿"],
            "scenes": ["雪场使用", "冬季外穿搭配", "男女通用"],
        }

    return CATEGORY_TITLE_RULES["滑雪帽衫和中间层"]


def get_ai_provider() -> str:
    return (os.getenv("AI_PROVIDER") or "gemini").strip().lower()


def get_gemini_api_key() -> str:
    return (os.getenv("GEMINI_API_KEY") or "").strip()


def get_gemini_model() -> str:
    return (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip()


def get_openai_compatible_base_url() -> str:
    base_url = (os.getenv("OPENAI_BASE_URL") or "https://api.n1n.ai").strip().rstrip("/")
    if base_url.endswith("/v1"):
        return base_url
    return f"{base_url}/v1"


def get_openai_compatible_api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def get_openai_compatible_model() -> str:
    return (os.getenv("OPENAI_MODEL") or "gpt-5.4-mini").strip()


def http_post_with_retry(url: str, **kwargs) -> requests.Response:
    max_retries = int(kwargs.pop("max_retries", 3) or 3)
    retriable_statuses = {502, 503, 504, 524}
    delay = 1.0
    last_error = None
    last_response = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(url, **kwargs)
            if resp.status_code not in retriable_statuses:
                return resp
            last_response = resp
            last_error = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
        except (SSLError, ConnectionError, ReadTimeout, Timeout) as exc:
            last_error = exc

        if attempt >= max_retries:
            break
        time.sleep(delay)
        delay *= 2

    if last_response is not None:
        return last_response
    raise last_error or RuntimeError("请求失败")


def call_gemini(prompt: str) -> str:
    gemini_api_key = get_gemini_api_key()
    gemini_model = get_gemini_model()
    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent"

    if not gemini_api_key:
        raise ValueError("没有检测到 GEMINI_API_KEY，请先 export GEMINI_API_KEY")

    headers = {
        "Content-Type": "application/json"
    }

    params = {
        "key": gemini_api_key
    }

    data = {
        "contents": [
            {
                "parts": [
                    {"text": prompt}
                ]
            }
        ]
    }

    resp = http_post_with_retry(
        gemini_url,
        headers=headers,
        params=params,
        json=data,
        timeout=60,
    )

    if resp.status_code != 200:
        raise RuntimeError(f"Gemini错误: {resp.text}")

    result = resp.json()

    candidates = result.get("candidates", [])
    if not candidates:
        raise RuntimeError(f"Gemini返回为空: {result}")

    parts = candidates[0].get("content", {}).get("parts", [])
    if not parts:
        raise RuntimeError(f"Gemini返回缺少parts: {result}")

    text = "".join(part.get("text", "") for part in parts).strip()
    if not text:
        raise RuntimeError(f"Gemini返回空文本: {result}")

    return text


def call_openai_compatible(prompt: str) -> str:
    api_key = get_openai_compatible_api_key()
    if not api_key:
        raise ValueError("没有检测到 OPENAI_API_KEY，请先 export OPENAI_API_KEY")

    base_url = get_openai_compatible_base_url()
    model = get_openai_compatible_model()
    url = f"{base_url}/chat/completions"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    data = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
        "temperature": 0.8,
    }

    resp = http_post_with_retry(
        url,
        headers=headers,
        json=data,
        timeout=60,
    )

    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI兼容接口错误: {resp.text}")

    result = resp.json()
    choices = result.get("choices") or []
    if not choices:
        raise RuntimeError(f"OpenAI兼容接口返回为空: {result}")

    message = choices[0].get("message") or {}
    text = str(message.get("content") or "").strip()
    if not text:
        raise RuntimeError(f"OpenAI兼容接口返回空文本: {result}")

    return text


def call_ai_text(prompt: str) -> str:
    provider = get_ai_provider()
    if provider in ("openai", "openai_compatible", "n1n", "n1n_openai"):
        return call_openai_compatible(prompt)
    return call_gemini(prompt)


def extract_json_block(text: str) -> dict:
    text = text.strip()

    # 去掉 markdown 代码块
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    # 提取第一个 {...}
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"未找到JSON内容: {text}")

    json_text = match.group(0)
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        repaired = repair_json_control_chars(json_text)
        return json.loads(repaired)


def extract_json_payload(text: str):
    text = str(text or "").strip()
    text = re.sub(r"^```json\s*", "", text)
    text = re.sub(r"^```\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    for pattern in (r"\[.*\]", r"\{.*\}"):
        match = re.search(pattern, text, re.DOTALL)
        if not match:
            continue
        json_text = match.group(0)
        try:
            return json.loads(json_text)
        except json.JSONDecodeError:
            repaired = repair_json_control_chars(json_text)
            return json.loads(repaired)
    raise ValueError(f"未找到JSON内容: {text}")


def repair_json_control_chars(json_text: str) -> str:
    chars = []
    in_string = False
    escape = False
    for ch in json_text:
        if escape:
            chars.append(ch)
            escape = False
            continue
        if ch == "\\":
            chars.append(ch)
            escape = True
            continue
        if ch == '"':
            chars.append(ch)
            in_string = not in_string
            continue
        if in_string:
            if ch == "\n":
                chars.append("\\n")
                continue
            if ch == "\r":
                chars.append("\\r")
                continue
            if ch == "\t":
                chars.append("\\t")
                continue
        chars.append(ch)
    return "".join(chars)


def normalize_ai_text(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def weighted_text_length(value: str) -> int:
    total = 0
    for ch in str(value or ""):
        total += 1 if ord(ch) < 128 else 2
    return total


def append_description_footer(description: str) -> str:
    base = normalize_ai_text(description)
    if not base:
        return ""

    footer_lines = [
        "正品全新，支持验货核验，假货按平台规则处理。",
        "闲鱼交易售出后不退不换，下单前细节可先沟通确认。",
        "默认顺丰到付发出，发货速度和包装这边会尽量安排稳妥。",
        "店里闲鱼常年上架商品过千，个别款式价格偶尔会有调整延迟；如果你对这件感兴趣，可以直接来问，我再帮你确认现在还有没有合适空间。",
    ]
    footer = "\n".join(footer_lines)
    return f"{base}\n\n{footer}".strip()


def build_material(product: dict) -> dict:
    name = (product.get("name") or "").strip()
    if not name:
        raise ValueError("商品 name 为空，无法生成 AI 内容")

    prompt = f"""
你是闲鱼商品发布助手。

请根据下面商品原始名称，生成适合闲鱼发布的内容。

商品原始名称：
{name}

要求：
1. 输出 JSON，不要输出任何解释文字
2. title 是适合闲鱼的中文标题，18到30字
3. description 是适合闲鱼的中文描述，分四段：
   【商品信息】
   【商品特点】
   【适合人群】
   【下单说明】
4. 不要夸张，不要虚构没有提供的参数
5. 不要写“全网最低、官方授权、100%正品、假一赔十”等风险词
6. cover_title 是封面主标题，简短
7. cover_subtitle 是封面副标题，简短

严格按这个 JSON 格式返回：
{{
  "title": "标题",
  "description": "描述",
  "cover_title": "封面主标题",
  "cover_subtitle": "封面副标题"
}}
"""

    raw = call_ai_text(prompt)
    data = extract_json_block(raw)

    return {
        "title": data.get("title", "").strip(),
        "description": data.get("description", "").strip(),
        "cover": {
            "title": data.get("cover_title", "").strip(),
            "subtitle": data.get("cover_subtitle", "").strip(),
        }
    }


def build_xianyu_prompts(product: dict) -> tuple[str, str]:
    name = (product.get("name") or "").strip()
    category = (product.get("category") or "").strip()
    stock = (product.get("stock") or "").strip()
    price = (product.get("final_price_cny") or "").strip()
    attributes = product.get("attributes") or {}
    if not name:
        raise ValueError("商品 name 为空，无法生成闲鱼文案")

    attr_lines = []
    for key, value in attributes.items():
        key = str(key or "").strip()
        value = str(value or "").strip()
        if key and value:
            attr_lines.append(f"- {key}: {value}")
    attr_block = "\n".join(attr_lines) if attr_lines else "- 无明确属性"
    category_rule = CATEGORY_TITLE_RULES.get(category, {})
    if category == "儿童装备":
        category_rule = detect_children_subcategory(name)
    if category == "滑雪帽衫和中间层":
        category_rule = detect_midlayer_subcategory(name)
    category_terms = "、".join(category_rule.get("category_terms") or ["保留真实商品品类"])

    title_prompt = f"""
你是闲鱼商品发布助手。请根据给定商品信息，输出适合闲鱼发布的标题和商品详情。

商品分类：
{category or "未分类"}

网站原始标题：
{name}

商品属性：
{attr_block}

库存信息：
{stock or "暂无"}

参考售价（人民币）：
{price or "未知"}

生成规则：
1. 只输出 JSON，不要解释
2. title 目标是提升搜索覆盖和点击率，适合闲鱼、淘宝等电商平台
3. title 结构固定为：年份 + 品牌 + 型号 + 商品品类 + 核心卖点 + 适用场景/人群
4. 必须保留原始标题中的年份、品牌、型号，不得篡改
5. 必须明确写出商品品类；当前商品更可能属于这些品类词之一：{category_terms}
6. 可补充 1 到 2 个真实通用卖点，但请优先根据原始标题、商品属性、库存信息自行判断，不要机械重复固定词
7. 可补充一个真实适用场景或人群，也请优先根据商品信息自然生成，不要每次都套用相同中文短语
8. 不得编造官方未确认参数，不得添加夸张营销词，如“顶级”“最强”“全网最低”“神器”“100%防护”
9. 避免关键词重复堆砌，标题要自然顺畅
10. title 要适合闲鱼发布，不超过 60 个字符，并尽量写到接近 60 个字符单位
11. 在不虚构、不夸张、不重复堆砌的前提下，尽量把标题补充完整，优先补搜索价值高的真实信息
12. 如果商品信息不足，就优先保证：年份 + 品牌 + 型号 + 商品品类，再补充真实卖点和适用场景
13. 同类商品不要总是重复使用相同的中文尾部短语，尽量根据具体商品做自然变化

严格按下面 JSON 返回：
{{
  "title": "标题"
}}
"""

    description_prompt = f"""
你是长期经营滑雪装备的专业卖家，要为闲鱼生成商品简介。

商品分类：
{category or "未分类"}

网站原始标题：
{name}

商品属性：
{attr_block}

库存信息：
{stock or "暂无"}

参考售价（人民币）：
{price or "未知"}

生成规则：
1. 只输出 JSON，不要解释
2. 只限制文案结构和边界，不限制具体表达
3. 文案整体控制在 4 到 5 段，不要写成僵硬模板，但要自然覆盖这些内容：
   自然开头、隐性卖家背书、1到2个核心卖点、基于商品标题的性能介绍、规格参数、发货与价格说明
4. 开头不要固定写法，不要每次都写“到了一批”或“今天整理到”。要像一个长期做雪具的人在自然发货架商品，可从现货、长度/尺码、这款适合谁、雪季使用场景切入
5. 卖家人设不要直白自夸，不要单独写“我是多年卖家”。要自然体现这些信息中的至少 2 个：长期做滑雪装备、现货在库、顺丰直发、可以给专业建议、支持细节确认
6. 商品卖点只写最有价值的 1 到 2 个点，不要硬凑。必须根据商品标题、分类、已识别属性、库存真实推断，不得编造官方未确认参数
7. 卖点表达要更像懂货的人在介绍，不要堆砌空泛术语，不要像参数表或广告词
8. 必须根据商品标题输出对应产品的性能介绍，优先结合标题里的型号、系列、定位、适用场景、软硬取向、玩法方向来写，像懂装备的人在解释这款东西的实际表现；如果信息不足，也要基于标题做保守、真实的性能归纳，不要跳过这一部分
9. 规格参数要写出真实的尺码、长度、库存、适用对象、成色、颜色等可确认信息，不要虚构没有提供的字段
10. 发货和价格说明里要体现顺丰直发、专业建议、价格不是普通闲置思路，但不要写成强硬营销，也不要提赠品或送小礼品
11. 语言风格要像长期做雪具的专业卖家，面向雪友和玩家，自然、直接、可信，像真人在卖货，不像客服或广告文案
12. 可以偶尔自然使用“雪友”“玩家”“兄弟”中的一个称呼，但不要每段都叫人，不强制出现
13. 不要使用“亲”“宝贝”等淘宝客服腔
14. 不要使用“顶级”“最强”“全网最低”“神器”“100%防护”“官方授权”“绝对正品”等夸张营销词
15. 不要模板化堆砌，不要让多篇文案只有几个词不同
16. 文案必须适合手机阅读：每段 1 到 3 句，句子不要过长，不要写成一整坨大段文字
17. 各段之间必须用空行分隔
18. 规格参数部分尽量用短行列出，例如：
   品牌：xxx
   型号：xxx
   长度/尺码：xxx
   库存：xxx
   适用对象：xxx
   成色：xxx
19. 返回的 description 必须使用真实换行，不要输出字面量 \\n 字符串

严格按下面 JSON 返回：
{{
  "description": "详情介绍"
}}
"""

    return title_prompt, description_prompt


def build_xianyu_copy(product: dict) -> dict:
    title_prompt, description_prompt = build_xianyu_prompts(product)
    raw = call_ai_text(title_prompt)
    data = extract_json_block(raw)
    raw_desc = call_ai_text(description_prompt)
    data_desc = extract_json_block(raw_desc)
    return {
        "title": normalize_ai_text(data.get("title", "")),
        "description": append_description_footer(data_desc.get("description", "")),
    }


def build_xianyu_title(product: dict) -> str:
    title_prompt, _ = build_xianyu_prompts(product)
    raw = call_ai_text(title_prompt)
    data = extract_json_block(raw)
    return normalize_ai_text(data.get("title", ""))


def build_xianyu_description(product: dict) -> str:
    _, description_prompt = build_xianyu_prompts(product)
    raw = call_ai_text(description_prompt)
    data = extract_json_block(raw)
    return append_description_footer(data.get("description", ""))


def build_taobao_main_image_bundle(product: dict, title: str = "") -> dict:
    name = str(product.get("name") or "").strip()
    category = str(product.get("category") or "").strip()
    stock = str(product.get("stock") or "").strip()
    final_price_cny = str(product.get("final_price_cny") or "").strip()
    attributes = product.get("attributes") or {}
    attr_lines = []
    for key, value in attributes.items():
        key = str(key or "").strip()
        value = str(value or "").strip()
        if key and value:
            attr_lines.append(f"- {key}: {value}")
    attr_block = "\n".join(attr_lines) if attr_lines else "- 无明确属性"
    prompt = f"""
你是淘宝滑雪装备视觉助手。请基于商品标题和商品信息，为这件商品输出：
1. 第1张点击主图专用型号词
2. 第2张核心卖点图专用的三类信息：
   - 适合人群
   - 风格定位
   - 软硬取向
3. 第3张性能参数图专用的四类信息：
   - 板型/类型
   - 软硬/弹性
   - 适合地形
   - 适合水平

商品原始名称：
{name}

淘宝标题：
{title or "未生成标题"}

商品分类：
{category or "未分类"}

库存信息：
{stock or "暂无"}

参考售价（人民币）：
{final_price_cny or "未知"}

商品属性：
{attr_block}

要求：
1. main_image_model_text 只用于第1张点击主图。
2. 只保留型号核心词，不要品牌、不要年份、不要品类词。
3. 例如：BURTON CUSTOM X BOARD -> CUSTOM X；RIDE PEACE SEEKER -> PEACE SEEKER。
4. 如果型号由两个或三个词组成，可以完整保留，如 PEACE SEEKER、CUSTOM X。
5. 使用中文理解，但输出的型号词保持商品原有语言，不要乱翻译。
6. target_audience / style_positioning / flex_feel 只用于第2张核心卖点图。
7. 这三个字段必须简短、明确、适合上图，不要写成长段句子。
8. board_profile / performance_feel / terrain_focus / skill_level 只用于第3张性能参数图。
9. 这四个字段必须简短、明确、适合上图，不要写成长段句子。
10. 不得编造明显无法确认的硬参数，但可以根据商品标题、分类和常见定位做合理归纳。
11. 输出中文短语即可。

严格按下面 JSON 返回：
{{
  "main_image_model_text": "型号核心词",
  "target_audience": "适合人群",
  "style_positioning": "风格定位",
  "flex_feel": "软硬取向",
  "board_profile": "板型/类型",
  "performance_feel": "软硬/弹性",
  "terrain_focus": "适合地形",
  "skill_level": "适合水平"
}}
"""
    raw = call_ai_text(prompt)
    data = extract_json_block(raw)
    return {
        "main_image_model_text": normalize_ai_text(data.get("main_image_model_text", "")),
        "target_audience": normalize_ai_text(data.get("target_audience", "")),
        "style_positioning": normalize_ai_text(data.get("style_positioning", "")),
        "flex_feel": normalize_ai_text(data.get("flex_feel", "")),
        "board_profile": normalize_ai_text(data.get("board_profile", "")),
        "performance_feel": normalize_ai_text(data.get("performance_feel", "")),
        "terrain_focus": normalize_ai_text(data.get("terrain_focus", "")),
        "skill_level": normalize_ai_text(data.get("skill_level", "")),
    }


def build_taobao_title_bundle(product: dict) -> dict:
    name = str(product.get("name") or "").strip()
    category = str(product.get("category") or "").strip()
    stock = str(product.get("stock") or "").strip()
    final_price_cny = str(product.get("final_price_cny") or "").strip()
    attributes = product.get("attributes") or {}
    attr_lines = []
    for key, value in attributes.items():
        key = str(key or "").strip()
        value = str(value or "").strip()
        if key and value:
            attr_lines.append(f"- {key}: {value}")
    attr_block = "\n".join(attr_lines) if attr_lines else "- 无明确属性"
    prompt = f"""
你是淘宝滑雪装备发布助手。请为这件商品生成两个标题：
1. 商品标题：最多 30 个汉字，最多 60 个字符
2. 导购标题：最多 15 个汉字，最多 30 个字符，结构优先为 品牌 + 品类词 + 卖点

商品原始名称：
{name}

商品分类：
{category or "未分类"}

库存信息：
{stock or "暂无"}

参考售价（人民币）：
{final_price_cny or "未知"}

商品属性：
{attr_block}

规则：
1. 商品标题适合淘宝发布，优先结构为：品牌 + 型号核心词 + 适用场景/定位 + 类目。
2. 要学会提炼型号核心词，不要机械照抄整串英文。
3. 例如：BURTON YOUTH CUSTOM SMALLS 这类名称，品牌保留 Burton，型号核心词优先提炼为 Custom，不要把 Youth Smalls 全部机械塞进标题。
4. 适用场景/定位要根据商品名称、分类、属性、库存做真实归纳，比如全山地、公园、进阶、儿童/青少年等。
5. 类目必须明确写出，例如滑雪板就写滑雪板，固定器就写固定器，滑雪鞋就写滑雪鞋。
6. 商品标题不能堆砌关键词，不能虚构信息，不能超过 60 个字符。
7. 商品标题必须尽量写满，目标长度为 58 到 60 个字符；低于 58 个字符时，优先补充真实的适用场景、定位、人群或卖点信息。
8. 导购标题单独生成，不是商品标题简单截断。
9. 导购标题控制在 30 个字符内，尽量控制在 15 个汉字以内。
10. 导购标题结构优先：品牌 + 品类词 + 卖点。
11. 卖点要真实、简短、适合展示，不要写成长句。
12. 商品标题和导购标题都不要使用夸张营销词，如“最强”“顶级”“神器”“全网最低”。
13. 不得编造官方未确认参数。

严格按下面 JSON 返回：
{{
  "taobao_title": "商品标题",
  "taobao_guide_title": "导购标题"
}}
"""
    raw = call_ai_text(prompt)
    data = extract_json_block(raw)
    taobao_title = normalize_ai_text(data.get("taobao_title", ""))
    taobao_guide_title = normalize_ai_text(data.get("taobao_guide_title", ""))
    if taobao_title and weighted_text_length(taobao_title) < 58:
        retry_prompt = f"""
你刚刚生成的淘宝商品标题长度不达标。请只改写商品标题，不要改导购标题。

商品原始名称：
{name}

商品分类：
{category or "未分类"}

库存信息：
{stock or "暂无"}

商品属性：
{attr_block}

当前商品标题：
{taobao_title}

导购标题：
{taobao_guide_title or "未生成"}

必须满足：
1. 新商品标题长度必须在 58 到 60 个字符之间。
2. 不能超过 60 个字符。
3. 必须在保证真实的前提下补足信息，不允许少于 58 个字符。
4. 结构仍然优先：品牌 + 型号核心词 + 适用场景/定位 + 类目。
5. 优先补充真实的适用场景、定位、人群、玩法、卖点。
6. 不要堆砌重复词，不要编造参数，不要用夸张营销词。
7. 只输出 JSON。

严格按下面 JSON 返回：
{{
  "taobao_title": "改写后的商品标题"
}}
"""
        retry_raw = call_ai_text(retry_prompt)
        retry_data = extract_json_block(retry_raw)
        retry_title = normalize_ai_text(retry_data.get("taobao_title", ""))
        if retry_title and 58 <= weighted_text_length(retry_title) <= 60:
            taobao_title = retry_title
    if taobao_guide_title and weighted_text_length(taobao_guide_title) < 20:
        guide_retry_prompt = f"""
你刚刚生成的淘宝导购标题长度不达标。请只改写导购标题，不要改商品标题。

商品原始名称：
{name}

商品分类：
{category or "未分类"}

库存信息：
{stock or "暂无"}

商品属性：
{attr_block}

商品标题：
{taobao_title or "未生成"}

当前导购标题：
{taobao_guide_title}

必须满足：
1. 新导购标题长度必须在 20 到 30 个字符之间。
2. 不能超过 30 个字符。
3. 必须在保证真实的前提下补足信息，不允许少于 20 个字符。
4. 导购标题结构优先：品牌 + 品类词 + 卖点。
5. 卖点要真实、简短、适合展示，不要写成长句。
6. 不要堆砌重复词，不要编造参数，不要用夸张营销词。
7. 只输出 JSON。

严格按下面 JSON 返回：
{{
  "taobao_guide_title": "改写后的导购标题"
}}
"""
        guide_retry_raw = call_ai_text(guide_retry_prompt)
        guide_retry_data = extract_json_block(guide_retry_raw)
        retry_guide_title = normalize_ai_text(guide_retry_data.get("taobao_guide_title", ""))
        if retry_guide_title and 20 <= weighted_text_length(retry_guide_title) <= 30:
            taobao_guide_title = retry_guide_title
    return {
        "taobao_title": taobao_title,
        "taobao_guide_title": taobao_guide_title,
    }


def _batch_product_block(product: dict) -> str:
    product_id = int(product.get("product_id") or 0)
    name = str(product.get("name") or "").strip()
    category = str(product.get("category") or "").strip()
    stock = str(product.get("stock") or "").strip()
    price = str(product.get("final_price_cny") or "").strip()
    attributes = product.get("attributes") or {}
    attr_lines = []
    for key, value in attributes.items():
        key = str(key or "").strip()
        value = str(value or "").strip()
        if key and value:
            attr_lines.append(f"- {key}: {value}")
    attr_block = "\n".join(attr_lines) if attr_lines else "- 无明确属性"
    return f"""商品ID: {product_id}
商品分类: {category or "未分类"}
网站原始标题: {name}
商品属性:
{attr_block}
库存信息: {stock or "暂无"}
参考售价（人民币）: {price or "未知"}"""


def build_xianyu_titles_batch(products: list[dict]) -> dict[int, str]:
    if not products:
        return {}
    product_blocks = "\n\n---\n\n".join(_batch_product_block(product) for product in products)
    prompt = f"""
你是闲鱼商品发布助手。请一次性为多件商品生成标题。

要求：
1. 只输出 JSON 数组，不要解释，不要 markdown
2. 每个元素格式必须是：
   {{"product_id": 123, "title": "标题"}}
3. title 目标是提升搜索覆盖和点击率，适合闲鱼、淘宝等电商平台
4. title 结构固定为：年份 + 品牌 + 型号 + 商品品类 + 核心卖点 + 适用场景/人群
5. 必须保留原始标题中的年份、品牌、型号，不得篡改
6. 必须明确写出商品品类
7. 可补充 1 到 2 个真实通用卖点与一个真实适用场景/人群，但不得编造参数
8. 不得添加夸张营销词，如“顶级”“最强”“全网最低”“神器”“100%防护”
9. 不超过 60 个字符，并尽量写到接近 60 个字符单位
10. 避免关键词重复堆砌，标题要自然顺畅
11. 数组里必须覆盖我提供的每一个商品ID，且不能漏项

商品列表：
{product_blocks}
"""
    raw = call_ai_text(prompt)
    data = extract_json_payload(raw)
    if not isinstance(data, list):
        raise ValueError(f"批量标题返回格式错误: {data}")
    result = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        product_id = int(item.get("product_id") or 0)
        title = normalize_ai_text(item.get("title", ""))
        if product_id and title:
            result[product_id] = title
    return result


def build_xianyu_descriptions_batch(products: list[dict]) -> dict[int, str]:
    if not products:
        return {}
    product_blocks = "\n\n---\n\n".join(_batch_product_block(product) for product in products)
    prompt = f"""
你是长期经营滑雪装备的专业卖家，要一次性为多件商品生成适合闲鱼的商品简介。

要求：
1. 只输出 JSON 数组，不要解释，不要 markdown
2. 每个元素格式必须是：
   {{"product_id": 123, "description": "详情介绍"}}
3. 文案整体控制在 4 到 5 段，自然覆盖这些内容：
   自然开头、隐性卖家背书、1到2个核心卖点、基于商品标题的性能介绍、规格参数、发货与价格说明
4. 开头不要固定写法，不要每次都写“到了一批”或“今天整理到”
5. 卖家人设要自然体现，不要直白自夸
6. 商品卖点只写最有价值的 1 到 2 个点，不要硬凑，不得编造参数
7. 必须根据每个商品标题输出对应产品的性能介绍，优先结合标题里的型号、系列、定位、适用场景、软硬取向、玩法方向来写，像懂装备的人在解释这款东西的实际表现；如果信息不足，也要基于标题做保守、真实的性能归纳，不要跳过这一部分
8. 规格参数要写出真实的尺码、长度、库存、适用对象、成色、颜色等可确认信息
9. 发货和价格说明里要体现顺丰直发、专业建议、价格不是普通闲置思路，但不要提赠品
10. 不要使用“亲”“宝贝”等淘宝客服腔
11. 不要使用“顶级”“最强”“全网最低”“神器”“100%防护”“官方授权”等夸张营销词
12. 每段 1 到 3 句，各段之间必须用空行分隔
13. description 必须使用真实换行，不要输出字面量 \\n
14. 数组里必须覆盖我提供的每一个商品ID，且不能漏项

商品列表：
{product_blocks}
"""
    raw = call_ai_text(prompt)
    data = extract_json_payload(raw)
    if not isinstance(data, list):
        raise ValueError(f"批量简介返回格式错误: {data}")
    result = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        product_id = int(item.get("product_id") or 0)
        description = append_description_footer(item.get("description", ""))
        if product_id and description:
            result[product_id] = description
    return result


if __name__ == "__main__":
    test_product = {
        "id": 1,
        "name": "25/26 SMITH 4D MAG XL - MIND EXPANDERS / ROSE GOLD MIRROR + STORM ROSE FLASH",
        "image": "data/images/boardline/1098000/main.jpg",
    }

    result = build_material(test_product)
    print(json.dumps(result, ensure_ascii=False, indent=2))
