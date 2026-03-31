import json
import logging

logger = logging.getLogger("PlateResolver")

class PlateResolver:
    """
    题材解析器：正式生产版。
    通过优先级权值与黑名单双重过滤，实现题材的自动识别。
    """
    # 高价值题材优先级（白名单）
    PREFERRED_PLATES = {
        "医药", "创新药", "化学制药", "半导体", "芯片", "存储", "低空经济", "算力", "锂电"
    }

    # 一级泛化词/噪音题材（黑名单）
    JUNK_PLATES = {
        "机器人", "智能电网", "通信", "金融", "电力", "化工", "房地产", 
        "银行", "证券", "央企", "国企", "融资融券", "深股通", "沪股通",
        "热力", "煤炭", "火电", "能源", "水电", "风电", "通用设备", "商业航天", "玻纤",
        "昨日涨停", "昨日曾涨停", "MSCI中国", "创业板综", "同花顺漂亮100"
    }

    @staticmethod
    def clean_name(name: str) -> str:
        if not name: return ""
        for suffix in ["概念", "板块", "指数", "行业", "主题"]:
            name = name.replace(suffix, "")
        return name.strip()

    @classmethod
    def resolve_precise_plate(cls, code: str, raw_plates: list, reason: str = "") -> str:
        """
        解析逻辑:
        1. 优先从 reason (Industry+Concepts) 中提取有效词
        2. 遍历候选名单，优先匹配 PREFERRED_PLATES
        3. 剔除 JUNK_PLATES
        """
        candidates = []
        
        # 1. 尝试从连板底座带的复合原因中深挖
        if reason:
            # 支持多种分隔符
            for p in reason.replace('；', '+').replace('、', '+').split('+'):
                p_clean = cls.clean_name(p)
                if p_clean and p_clean not in cls.JUNK_PLATES and len(p_clean) > 1:
                    candidates.append(p_clean)

        # 2. 遍历原始板块库
        for p in raw_plates:
            p_clean = cls.clean_name(p)
            if p_clean and p_clean not in cls.JUNK_PLATES and len(p_clean) > 1:
                candidates.append(p_clean)

        # 3. 优先级匹配：白名单优先
        for c in candidates:
            if any(p in c for p in cls.PREFERRED_PLATES):
                return c
        
        # 4. 次选：非黑名单第一个词
        if candidates:
            return candidates[0]
        
        # 5. 保底
        return cls.clean_name(raw_plates[0]) if raw_plates else "其他"

if __name__ == "__main__":
    print(PlateResolver.resolve_precise_plate("603538", ["热力"], "医药+减肥药"))
