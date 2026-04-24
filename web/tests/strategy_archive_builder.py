import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web.tests.teacher_alignment_probe import TeacherAlignmentProbe


logger = logging.getLogger(__name__)

DEFAULT_ARCHIVE_ROOT = ROOT_DIR / "strategy_archive"
DEFAULT_ARTICLE_ROOT = ROOT_DIR / "Article"


FAMILY_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "midcycle_resonance_family": {
        "strategy_id": "niepan-midcycle-resonance-family",
        "strategy_key": "midcycle_resonance_family",
        "strategy_name": "中周期共振主升确认",
        "setup_type": "中周期共振与细分家族联动",
        "core_thesis": "在题材轮动环境中，不追日内最热，而是围绕已经跨周期走强的细分家族做主升确认与分歧修复。",
        "watch_window": "文章日前后 5 个交易日，优先跟踪家族内部最先转强与分歧后再确认的样本。",
        "market_features": {
            "required": ["市场处于轮动或修复走强", "不是单一日内主线的极致一致"],
            "optional": ["热点快速切换给中周期强方向留出定价空窗", "高标负反馈未全面失控"],
        },
        "theme_features": {
            "required": ["细分方向具备家族联动", "方向在更长周期已经出现持续新高或放量突破"],
            "optional": ["主线表层不一定最热", "行业逻辑具备持续性而非单日刺激"],
        },
        "stock_features": {
            "required": ["启动确认或主升中段", "低点抬高或分歧后再转强"],
            "optional": ["中军与弹性股同时存在", "存在游资龙和机构中军分层"],
        },
        "amount_features": {
            "required": ["近 5 日成交额高于 20 日均额", "关键转强日不缩量"],
            "optional": ["启动日前后出现一次放量突破", "尾段量能回落时要分清是良性换手还是衰竭"],
        },
        "chip_features": {
            "required": ["主筹成本低于现价", "获利盘占优"],
            "optional": ["筹码集中在主升前平台区", "高位套牢盘压力有限"],
        },
        "entry_conditions": [
            "家族内多股持续走强，目标股出现放量突破或分歧修复再确认。",
            "筹码优势和量能扩张同时成立。",
            "没有被更强新主线彻底替代。",
        ],
        "exit_conditions": [
            "尾段爆量但不再创新高。",
            "高位长阴或炸板后次日无法修复。",
            "家族联动明显降温并被新主线替代。",
        ],
        "veto_conditions": [
            "只剩消息刺激，没有中周期趋势基础。",
            "高位爆量长阴后无法修复。",
            "家族只剩单票脉冲，跟风全面掉队。",
        ],
        "risk_flags": ["高位分歧放大", "家族联动转弱", "尾段爆量衰竭"],
        "execution_hints": [
            "优先做家族中仍处于启动确认或分歧修复再确认的位置。",
            "中军更多承担趋势锚点，弹性股承担利润弹性。",
        ],
    },
    "dragon_second_stage_family": {
        "strategy_id": "niepan-dragon-second-stage-family",
        "strategy_key": "dragon_second_stage_family",
        "strategy_name": "总龙与二阶段补涨接力",
        "setup_type": "总龙、伴生股、二阶段补涨",
        "core_thesis": "围绕已经确立辨识度的总龙，寻找伴生股、反包打开空间的样本和主升二阶段补涨承接。",
        "watch_window": "围绕总龙确立后的 3 到 8 个交易日，重点看伴生股和二阶段补涨承接。",
        "market_features": {
            "required": ["市场处于修复走强或双主线并行", "高标仍有赚钱效应"],
            "optional": ["单一题材不必绝对垄断", "高位分歧可控而非全面退潮"],
        },
        "theme_features": {
            "required": ["存在总龙锚点", "伴生补涨与主升二阶段样本开始接力"],
            "optional": ["双主线并行", "空间票负责打开高度"],
        },
        "stock_features": {
            "required": ["总龙/伴生/补涨/空间先导分工清晰", "样本具备反包或二阶段承接特征"],
            "optional": ["总龙与伴生股同步扩容", "断板反包打开高度"],
        },
        "amount_features": {
            "required": ["伴生股近 5 日成交额扩张", "反包或突破日有有效承接"],
            "optional": ["总龙与伴生股量能同步放大", "补涨股不是纯情绪缩量板"],
        },
        "chip_features": {
            "required": ["筹码主成本仍低于现价", "高位获利盘尚可流动"],
            "optional": ["前一波筹码没有被完全砸乱", "补涨股前平台套牢盘可消化"],
        },
        "entry_conditions": [
            "总龙继续稳住高度，伴生股或补涨股放量突破、反包或承接转强。",
            "高标负反馈可控，没有全面退潮。",
            "补涨股获得新增成交额承接。",
        ],
        "exit_conditions": [
            "总龙和伴生股同步高位分歧。",
            "空间票反包失效或被监管压制。",
            "补涨股连续冲高不创新高，题材切回新方向。",
        ],
        "veto_conditions": [
            "高位一致加速直接挑战监管。",
            "伴生股只剩情绪冲板，没有成交额承接。",
            "总龙炸板且次日无修复。",
        ],
        "risk_flags": ["高位监管风险", "总龙断板塌缩", "伴生补涨失败"],
        "execution_hints": [
            "总龙更多是情绪锚点，交易性价比往往在伴生补涨和二阶段承接。",
            "空间票的意义在于打开高度，不等于适合追最末端一致。",
        ],
    },
    "trend_extension_family": {
        "strategy_id": "niepan-trend-extension-family",
        "strategy_key": "trend_extension_family",
        "strategy_name": "趋势突破与主升延续",
        "setup_type": "板块趋势突破与主升延续",
        "core_thesis": "围绕板块或细分方向的放量突破、趋势延续与中军承接，做顺势而为而不是末端追涨。",
        "watch_window": "以板块指数或方向突破前后 5 到 20 个交易日为核心观察窗口。",
        "market_features": {
            "required": ["市场允许趋势板块持续运行", "方向具备中期赚钱效应"],
            "optional": ["指数不一定同步强", "趋势行情常在情绪退潮后悄悄展开"],
        },
        "theme_features": {
            "required": ["板块或细分方向出现放量突破", "方向持续时间足够长而非单日脉冲"],
            "optional": ["多品种共振", "期货或产业逻辑辅助强化"],
        },
        "stock_features": {
            "required": ["趋势股、容量中军或早期龙头先行", "启动确认后沿均线主升"],
            "optional": ["板块指数与核心股同步突破", "先手样本在初期就拉开涨幅"],
        },
        "amount_features": {
            "required": ["突破日明显放量", "趋势延续期量能不塌陷"],
            "optional": ["中军成交额扩张", "分歧日缩量不破结构"],
        },
        "chip_features": {
            "required": ["筹码重心上移", "主筹成本低于现价"],
            "optional": ["突破前平台筹码被充分换手", "大级别获利盘稳定"],
        },
        "entry_conditions": [
            "方向或板块指数放量突破关键位置。",
            "核心股在突破初期就体现辨识度与承接。",
            "趋势延续过程中分歧不破结构。",
        ],
        "exit_conditions": [
            "趋势板块阶段性见顶，核心股放量滞涨。",
            "板块指数跌回突破位下方。",
            "核心股强度明显落后于板块。",
        ],
        "veto_conditions": [
            "只看到末端加速，没有经历初期放量突破。",
            "板块趋势不连贯，纯靠单票冲高。",
            "题材持续性不足。",
        ],
        "risk_flags": ["趋势末端追高", "板块见顶回撤", "容量股滞涨"],
        "execution_hints": [
            "这类策略核心是顺趋势，不是追最后一棒。",
            "板块指数和核心股同步突破，比单票异动更可靠。",
        ],
    },
    "rotation_low_suction_family": {
        "strategy_id": "niepan-rotation-low-suction-family",
        "strategy_key": "rotation_low_suction_family",
        "strategy_name": "轮动市分歧低吸与回流修复",
        "setup_type": "轮动试错、低吸回流、分歧修复",
        "core_thesis": "在轮动试错环境中，不追一致高潮，而是在分歧、回踩、回流修复里寻找低吸和再确认机会。",
        "watch_window": "以题材分歧后的 1 到 3 个交易日修复窗口为主。",
        "market_features": {
            "required": ["市场处于轮动试错", "没有单一绝对主线统治全场"],
            "optional": ["新老题材频繁切换", "高标高度有限但仍有局部修复机会"],
        },
        "theme_features": {
            "required": ["方向存在回流修复可能", "不是彻底退潮的废线"],
            "optional": ["逆周期方向阶段性对冲", "主线内部轮动而非总量扩张"],
        },
        "stock_features": {
            "required": ["分歧整理、预热观察或低点抬高", "回踩后存在再确认机会"],
            "optional": ["断板后再转强", "低吸比追板更占优"],
        },
        "amount_features": {
            "required": ["分歧日不能彻底放弃量能", "修复日有重新放量迹象"],
            "optional": ["近 5 日量能温和扩张", "回踩缩量、转强放量"],
        },
        "chip_features": {
            "required": ["筹码没有被完全打散", "主筹成本仍有优势"],
            "optional": ["平台压缩后重心抬升", "回流修复时套牢盘压力不大"],
        },
        "entry_conditions": [
            "题材经历分歧后出现修复信号。",
            "目标股回踩不破结构，低点继续抬高。",
            "修复日量能重新放出。",
        ],
        "exit_conditions": [
            "修复失败，回流只维持半日或一日。",
            "分歧后低点被放量跌破。",
            "市场切换到更强新方向。",
        ],
        "veto_conditions": [
            "已经确认进入全面退潮。",
            "回流只是弱脉冲，没有量能和承接。",
            "高位一致后的首次大分歧直接去接飞刀。",
        ],
        "risk_flags": ["修复失败二次杀", "轮动太快来不及兑现", "假修复真出货"],
        "execution_hints": [
            "低吸和修复确认优先于追一致高潮。",
            "轮动市的关键是等待分歧后的正确承接，而不是追第一根冲高。",
        ],
    },
    "high_level_emotion_family": {
        "strategy_id": "niepan-high-level-emotion-family",
        "strategy_key": "high_level_emotion_family",
        "strategy_name": "高位情绪、断板反包与退潮甄别",
        "setup_type": "高位抱团、断板反包、退潮风险管理",
        "core_thesis": "围绕高位抱团、断板反包、情绪冰点与退潮甄别来做高风险高收益博弈，核心是识别边界而不是无脑追高。",
        "watch_window": "高标分歧、断板反包和冰点次日的 1 到 3 个交易日。",
        "market_features": {
            "required": ["情绪票仍有局部抱团", "空间高度和监管形成博弈"],
            "optional": ["退潮加速与弱修复反复切换", "指数与情绪背离"],
        },
        "theme_features": {
            "required": ["高位抱团或断板反包成为主要赚钱形态", "方向内部互卷强烈"],
            "optional": ["主流缺失，靠高位样本维系风险偏好", "题材内部胜负切换极快"],
        },
        "stock_features": {
            "required": ["高位样本、空间票或断板反包票", "阶段处于高位加速或分歧整理"],
            "optional": ["情绪锚点作用强", "反包样本打开新空间"],
        },
        "amount_features": {
            "required": ["高位样本不能失去承接", "反包或修复必须有量能验证"],
            "optional": ["一致加速末端常见爆量", "量能衰减往往先于情绪瓦解"],
        },
        "chip_features": {
            "required": ["高位筹码仍能流动", "不能被连续一字或监管锁死"],
            "optional": ["断板后筹码重新分配成功", "抱团样本仍有获利盘愿意承接"],
        },
        "entry_conditions": [
            "断板反包、冰点回流或高位抱团修复得到量能确认。",
            "高位样本的情绪锚点仍在。",
            "市场没有进入全面退潮不可接阶段。",
        ],
        "exit_conditions": [
            "高位一致加速后失控。",
            "炸板失败且次日没有修复。",
            "冰点后弱修复再度转弱。",
        ],
        "veto_conditions": [
            "全面退潮时去追高位一致。",
            "空间票挑战监管过猛。",
            "把弱修复误当成新周期启动。",
        ],
        "risk_flags": ["监管风险", "退潮加速", "弱修复骗炮", "高位炸板"],
        "execution_hints": [
            "这类策略的核心不是多做，而是做对边界、做对退出。",
            "断板反包和冰点回流可以做，但必须先确认承接，不做无脑顶。",
        ],
    },
    "framework_meta_family": {
        "strategy_id": "niepan-framework-meta-family",
        "strategy_key": "framework_meta_family",
        "strategy_name": "方法论与市场框架认知",
        "setup_type": "底层框架、跟随最强、进化路径",
        "core_thesis": "这类文章不是单次交易 setup，而是老师对趋势、龙头、量化时代进化路径和跟随最强者的方法论总结。",
        "watch_window": "不对应单一交易窗口，更适合作为策略解释层和学习底稿。",
        "market_features": {
            "required": ["文章以框架总结为主，不以单次买卖为主"],
            "optional": ["会借多个历史案例说明方法", "会强调跟随最强和先手吃后手"],
        },
        "theme_features": {
            "required": ["更关注如何识别主线与强势方向", "不依赖单一题材"],
            "optional": ["可能引用商业航天、有色、算力等做例证"],
        },
        "stock_features": {
            "required": ["股票只是案例样本，不是单一主策略对象"],
            "optional": ["更偏对成功形态的抽象"],
        },
        "amount_features": {
            "required": ["强调放量突破、承接、趋势强度等原则"],
            "optional": ["不一定对应具体单日量能阈值"],
        },
        "chip_features": {
            "required": ["对筹码没有固定单次 setup 要求"],
            "optional": ["可为后续策略族提供解释层"],
        },
        "entry_conditions": ["不作为直接自动执行策略，更多作为策略族解释层与分类依据。"],
        "exit_conditions": ["不适用单次出场条件，需由落地策略族承接。"],
        "veto_conditions": ["不能把纯方法论文章误当作单日交易策略。"],
        "risk_flags": ["信息过泛", "不能直接下单", "需要映射到具体策略族"],
        "execution_hints": ["系统层面把这类内容当作元规则，而不是直接信号。"],
    },
}


CASE_ASSIGNMENTS: Dict[str, Dict[str, Any]] = {
    "2025-12-02": {
        "primary_family": "trend_extension_family",
        "secondary_family": "midcycle_resonance_family",
        "family_confidence": 0.83,
        "why_this_family": "文章明确用商业航天板块突破历史新高、有色涨价多品种放量突破来说明趋势的力量，核心是板块突破后的主升延续。",
        "matched_features": ["板块放量突破", "趋势主升延续", "多方向共振", "先手吃后手"],
        "missing_but_optional_features": ["单日最强主线不一定明确", "没有固定单一标的承接"],
    },
    "2026-01-31": {
        "primary_family": "midcycle_resonance_family",
        "secondary_family": "trend_extension_family",
        "family_confidence": 0.88,
        "why_this_family": "文章围绕黄金白银等涨价线强调从 1 月初放量突破到阶段见顶的全过程，更像中周期共振后的主升确认。",
        "matched_features": ["中周期共振", "放量突破", "趋势主升", "先手识别而非末端追高"],
        "missing_but_optional_features": ["家族内部游资与机构分层不明显"],
    },
    "2026-02-05": {
        "primary_family": "high_level_emotion_family",
        "secondary_family": "rotation_low_suction_family",
        "family_confidence": 0.95,
        "why_this_family": "文章直接把当前环境拆成高位抱团、非流畅连板趋势票、断板反包三类高位情绪形态，核心是情绪边界和高位博弈。",
        "matched_features": ["轮动试错", "高位抱团", "断板反包", "非流畅连板"],
        "missing_but_optional_features": ["单一绝对主线缺失", "更偏形态分类而非产业逻辑"],
    },
    "2026-02-21": {
        "primary_family": "framework_meta_family",
        "secondary_family": None,
        "family_confidence": 0.42,
        "why_this_family": "文章主体是问道篇，更多在讲小白进阶、跟随最强和量化时代进化路径，不是单次交易策略。",
        "matched_features": ["方法论总结", "跟随最强", "龙头与席位学习", "元规则"],
        "missing_but_optional_features": ["缺少明确单次入场点", "缺少单一交易日兑现规则"],
    },
    "2026-02-23": {
        "primary_family": "framework_meta_family",
        "secondary_family": None,
        "family_confidence": 0.35,
        "why_this_family": "当前抽取出的样本和文章结构都偏弱，更适合作为方法论或杂项档案，而不是强行定义为交易 setup。",
        "matched_features": ["方法论/杂项", "低信号文章", "不适合作为独立下单策略"],
        "missing_but_optional_features": ["缺少稳定样本股", "缺少清晰主线与买卖点"],
    },
    "2026-03-02": {
        "primary_family": "trend_extension_family",
        "secondary_family": "rotation_low_suction_family",
        "family_confidence": 0.67,
        "why_this_family": "市场重心开始从涨价与化工等方向延展，核心更像已有强势方向的延续和切换，而不是单次高位博弈。",
        "matched_features": ["方向延续", "化工/有色相关延展", "趋势承接"],
        "missing_but_optional_features": ["单一明确龙头并不突出"],
    },
    "2026-03-06": {
        "primary_family": "rotation_low_suction_family",
        "secondary_family": "trend_extension_family",
        "family_confidence": 0.62,
        "why_this_family": "这一天更像轮动中的前置观察和分歧后的再确认窗口，适合低吸和回流修复思维，而不是追最热。",
        "matched_features": ["轮动市", "分歧后的再确认", "不追最热", "前置观察"],
        "missing_but_optional_features": ["明确的单一修复主线不足"],
    },
    "2026-03-08": {
        "primary_family": "midcycle_resonance_family",
        "secondary_family": "trend_extension_family",
        "family_confidence": 0.97,
        "why_this_family": "文章明确围绕钨家族的两年线、年线、季线、双月线和短线窗口共振，典型的中周期共振主升确认。",
        "matched_features": ["跨周期共振", "细分家族联动", "非日内最热", "主筹成本低位", "量能扩张"],
        "missing_but_optional_features": ["当日并非表层最热主线"],
    },
    "2026-03-13": {
        "primary_family": "dragon_second_stage_family",
        "secondary_family": "high_level_emotion_family",
        "family_confidence": 0.96,
        "why_this_family": "文章把豫能控股定义为总龙，把汉缆、杭电、江钨、章源等放进伴生补涨和主升二阶段框架，核心是总龙接力体系。",
        "matched_features": ["总龙", "伴生补涨", "主升二阶段", "断板反包打开空间"],
        "missing_but_optional_features": ["并不要求单一板块独占市场"],
    },
    "2026-03-15": {
        "primary_family": "dragon_second_stage_family",
        "secondary_family": "high_level_emotion_family",
        "family_confidence": 0.82,
        "why_this_family": "周末衔接文延续了总龙、双主线并行和主升二阶段的藏宝图思路，更像对 2026-03-13 策略的延展。",
        "matched_features": ["双主线并行", "总龙锚点", "二阶段补涨"],
        "missing_but_optional_features": ["具体入场点更多依赖盘中二次确认"],
    },
    "2026-03-16": {
        "primary_family": "high_level_emotion_family",
        "secondary_family": "dragon_second_stage_family",
        "family_confidence": 0.91,
        "why_this_family": "文章核心是弱修复骗炮、电力送人头、冰点成立和退潮加速，明显是高位情绪与退潮甄别，而不是继续做总龙接力。",
        "matched_features": ["退潮加速", "弱修复甄别", "高位互卷", "冰点判断"],
        "missing_but_optional_features": ["新的明确主线并未成立"],
    },
    "2026-03-17": {
        "primary_family": "high_level_emotion_family",
        "secondary_family": None,
        "family_confidence": 0.9,
        "why_this_family": "文章反复强调量化在高位轮动中的收割、阶段离场和人声鼎沸时离开化工，本质是高位情绪末端和风险管理。",
        "matched_features": ["高位情绪末端", "量化收割", "人声鼎沸时离场", "风险管理"],
        "missing_but_optional_features": ["不强调单次正向介入，更强调空仓和退出"],
    },
    "2026-03-19": {
        "primary_family": "high_level_emotion_family",
        "secondary_family": "rotation_low_suction_family",
        "family_confidence": 0.86,
        "why_this_family": "文章核心在于指数下跌加速、情绪与指数背离、反周期方向切换与高位接盘教训，属于高位情绪末端与风险识别。",
        "matched_features": ["高位情绪风险", "题材切换", "反周期方向", "先手与后手博弈"],
        "missing_but_optional_features": ["单一具体买点并不突出"],
    },
}


CASE_INSTANCE_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "2026-03-08": {
        "teacher_strategy_summary": "在题材轮动环境下，不追日内最热，而是围绕中周期共振的钨家族做主升确认与分歧回流。",
        "stock_overrides": {
            "章源钨业": {"role_label": "核心弹性", "entry_style": "突破确认", "exit_style": "加速后分批兑现", "risk_points": ["高位波动放大", "尾段转一致后容易日内大分歧"]},
            "江钨装备": {"role_label": "游资龙头", "entry_style": "分歧修复再确认", "exit_style": "修复失败即走，转强续持", "risk_points": ["高波动", "加速后回撤大"]},
            "翔鹭钨业": {"role_label": "后排弹性", "entry_style": "跟随强化", "exit_style": "后排跟风减弱时先兑现", "risk_points": ["辨识度弱于核心", "更容易被龙头虹吸"]},
            "中钨高新": {"role_label": "趋势中军", "entry_style": "中军趋势承接", "exit_style": "趋势走弱分批减仓", "risk_points": ["弹性弱于前排", "更多是趋势持有而非打板情绪"]},
        },
    },
    "2026-03-13": {
        "teacher_strategy_summary": "在轮动修复环境中，围绕豫能控股总龙与涨价线并行，优先识别总龙伴生补涨、反包打开空间和主升二阶段承接。",
        "stock_overrides": {
            "豫能控股": {"role_label": "总龙", "entry_style": "总龙分歧回流或主升承接", "exit_style": "高位分歧放量先减，失守核心承接位离场", "risk_points": ["高位监管风险", "总龙断板后的情绪塌缩"]},
            "汉缆股份": {"role_label": "伴生补涨", "entry_style": "补涨放量突破", "exit_style": "伴生失去承接先兑现", "risk_points": ["被总龙虹吸", "补涨高度受限"]},
            "杭电股份": {"role_label": "空间先导", "entry_style": "反包打开高度", "exit_style": "反包失效或被监管压制即走", "risk_points": ["高波动", "空间票监管风险"]},
            "横店影视": {"role_label": "高位情绪样本", "entry_style": "谨慎观察为主", "exit_style": "一致加速后优先兑现", "risk_points": ["挑战监管", "高位加速失败"]},
            "江钨装备": {"role_label": "涨价线游资龙", "entry_style": "分歧整理后的再确认", "exit_style": "修复失败或跌破重心离场", "risk_points": ["涨价线分歧加大", "高位波动放大"]},
            "章源钨业": {"role_label": "涨价线机构中军", "entry_style": "主升承接", "exit_style": "趋势走弱分批兑现", "risk_points": ["弹性不足时被短线资金切走", "量能回落后转钝"]},
        },
    },
}


class StrategyArchiveBuilder:
    def __init__(
        self,
        teacher: str = "niepan",
        archive_root: Optional[Path] = None,
        article_root: Optional[Path] = None,
        probe: Optional[TeacherAlignmentProbe] = None,
    ) -> None:
        self.teacher = teacher
        self.archive_root = archive_root or DEFAULT_ARCHIVE_ROOT
        self.article_root = article_root or DEFAULT_ARTICLE_ROOT
        self.archive_root.mkdir(parents=True, exist_ok=True)
        self.probe = probe or TeacherAlignmentProbe(teacher=teacher)

    def build_case_archive(self, date_str: str) -> Dict[str, Any]:
        report = self.probe.run(date_str, write_snapshot=False)
        article_text = self._load_article_text(date_str)
        assignment = self._get_case_assignment(date_str)
        family = self._get_family_definition(assignment["primary_family"])
        instance = CASE_INSTANCE_OVERRIDES.get(date_str, {})
        market_context = self._build_market_context(report)
        stock_cases = self._build_stock_cases(report, instance)

        entry_playbook = self._default_entry_playbook(family)
        exit_playbook = self._default_exit_playbook(family)
        teacher_summary = instance.get("teacher_strategy_summary") or family["core_thesis"]
        next_event = self.find_next_event_date(date_str)

        archive = {
            "strategy_id": family["strategy_id"],
            "strategy_name": family["strategy_name"],
            "setup_type": family["setup_type"],
            "article_date": date_str,
            "effective_trade_date": report["effective_date"],
            "teacher": self.teacher,
            "primary_family": assignment["primary_family"],
            "secondary_family": assignment.get("secondary_family"),
            "family_confidence": assignment["family_confidence"],
            "why_this_family": assignment["why_this_family"],
            "matched_features": assignment["matched_features"],
            "missing_but_optional_features": assignment["missing_but_optional_features"],
            "teacher_strategy_summary": teacher_summary,
            "market_context": market_context,
            "entry_playbook": entry_playbook,
            "exit_playbook": exit_playbook,
            "entry_conditions": entry_playbook["confirm_signals"],
            "exit_conditions": exit_playbook["take_profit_signals"],
            "stock_cases": stock_cases,
            "risk_flags": self._collect_risk_flags(stock_cases, family, exit_playbook),
            "reference_stocks": [
                {"stock_name": stock["stock_name"], "code6": stock["code6"], "role": stock["role_label"]}
                for stock in stock_cases
            ],
            "execution_hints": family["execution_hints"],
            "system_mapping": self._build_system_mapping(family),
            "template_relation": "reuse_template",
            "template_parent_strategy_id": family["strategy_id"],
            "template_notes": self._template_notes(assignment["primary_family"], article_text),
            "article_excerpt_hint": self._article_excerpt_hint(article_text),
            "next_event": next_event,
            "peer_dates_within_family": [],
        }
        return archive

    def write_case_archive(self, archive: Dict[str, Any]) -> Tuple[Path, Path]:
        json_path = self.archive_root / f"strategy_case_{archive['article_date']}_{self.teacher}.json"
        md_path = self.archive_root / f"strategy_case_{archive['article_date']}_{self.teacher}.md"
        json_path.write_text(json.dumps(archive, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(self.build_markdown_archive(archive), encoding="utf-8")
        return json_path, md_path

    def build_markdown_archive(self, archive: Dict[str, Any]) -> str:
        lines = [f"# Strategy Case {archive['article_date']} ({self.teacher})", ""]
        lines.append("## 策略归类")
        lines.append(
            f"- 主策略族: {archive['primary_family']} | 策略名: {archive['strategy_name']} | 置信度: {archive['family_confidence']}"
        )
        if archive.get("secondary_family"):
            lines.append(f"- 次策略族: {archive['secondary_family']}")
        lines.append(f"- 归类原因: {archive['why_this_family']}")
        lines.append("")
        lines.append("## 当天命中的策略特征")
        for item in archive["matched_features"]:
            lines.append(f"- 命中: {item}")
        for item in archive["missing_but_optional_features"]:
            lines.append(f"- 可缺省但未完全满足: {item}")
        lines.append("")
        lines.append("## 介入点")
        lines.append(f"- 观察窗口: {archive['entry_playbook']['watch_window']}")
        for item in archive["entry_playbook"]["preconditions"]:
            lines.append(f"- 前提: {item}")
        for item in archive["entry_playbook"]["confirm_signals"]:
            lines.append(f"- 确认: {item}")
        for item in archive["entry_playbook"]["veto_signals"]:
            lines.append(f"- 否决: {item}")
        lines.append("")
        lines.append("## 兑现点")
        for item in archive["exit_playbook"]["take_profit_signals"]:
            lines.append(f"- 止盈: {item}")
        for item in archive["exit_playbook"]["reduce_signals"]:
            lines.append(f"- 减仓: {item}")
        for item in archive["exit_playbook"]["fail_fast_signals"]:
            lines.append(f"- 失败退出: {item}")
        lines.append("")
        lines.append("## 个股角色")
        for stock in archive["stock_cases"]:
            lines.append(
                f"- {stock['stock_name']} {stock['code6']} | 定位 {stock['role_label']} | 阶段 {stock['phase']} | "
                f"形态 {'、'.join(stock['shape_tags']) or '无'} | 介入 {stock['entry_style']} | 兑现 {stock['exit_style']}"
            )
        lines.append("")
        lines.append("## 和同策略其他日期的差异")
        if archive["peer_dates_within_family"]:
            lines.append(f"- 同策略其他日期: {'、'.join(archive['peer_dates_within_family'])}")
        else:
            lines.append("- 当前是该策略族的唯一日期样本。")
        lines.append(f"- 本次摘要: {archive['teacher_strategy_summary']}")
        lines.append("")
        lines.append("## 系统执行映射")
        sm = archive["system_mapping"]
        lines.append(f"- 市场标签: {'、'.join(sm['market_tags'])}")
        lines.append(f"- 题材标签: {'、'.join(sm['theme_tags'])}")
        lines.append(f"- 个股标签: {'、'.join(sm['stock_tags'])}")
        lines.append(f"- 触发条件: {'、'.join(sm['triggers'])}")
        lines.append(f"- 否决条件: {'、'.join(sm['vetoes'])}")
        return "\n".join(lines)

    def build_family_archive(self, family_key: str, cases: List[Dict[str, Any]]) -> Dict[str, Any]:
        family = self._get_family_definition(family_key)
        primary_cases = [case for case in cases if case["primary_family"] == family_key]
        secondary_cases = [case for case in cases if case.get("secondary_family") == family_key]
        return {
            "strategy_key": family["strategy_key"],
            "strategy_id": family["strategy_id"],
            "strategy_name": family["strategy_name"],
            "setup_type": family["setup_type"],
            "core_thesis": family["core_thesis"],
            "market_features": family["market_features"],
            "theme_features": family["theme_features"],
            "stock_features": family["stock_features"],
            "amount_features": family["amount_features"],
            "chip_features": family["chip_features"],
            "entry_conditions": family["entry_conditions"],
            "exit_conditions": family["exit_conditions"],
            "veto_conditions": family["veto_conditions"],
            "risk_flags": family["risk_flags"],
            "execution_hints": family["execution_hints"],
            "watch_window": family["watch_window"],
            "example_dates": [case["article_date"] for case in primary_cases],
            "reference_stocks": self._collect_family_reference_stocks(primary_cases),
            "primary_case_count": len(primary_cases),
            "secondary_case_count": len(secondary_cases),
        }

    def write_family_archive(self, family_archive: Dict[str, Any]) -> Tuple[Path, Path]:
        key = family_archive["strategy_key"]
        json_path = self.archive_root / f"strategy_family_{key}.json"
        md_path = self.archive_root / f"strategy_family_{key}.md"
        json_path.write_text(json.dumps(family_archive, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(self.build_family_markdown(family_archive), encoding="utf-8")
        return json_path, md_path

    def build_family_markdown(self, family_archive: Dict[str, Any]) -> str:
        lines = [f"# Strategy Family {family_archive['strategy_key']} ({self.teacher})", ""]
        lines.append("## 核心定义")
        lines.append(f"- 策略名: {family_archive['strategy_name']}")
        lines.append(f"- 核心假设: {family_archive['core_thesis']}")
        lines.append(f"- 观察窗口: {family_archive['watch_window']}")
        lines.append("")
        lines.append("## 市场特征")
        for item in family_archive["market_features"]["required"]:
            lines.append(f"- 必要: {item}")
        for item in family_archive["market_features"]["optional"]:
            lines.append(f"- 可选增强: {item}")
        lines.append("")
        lines.append("## 题材特征")
        for item in family_archive["theme_features"]["required"]:
            lines.append(f"- 必要: {item}")
        for item in family_archive["theme_features"]["optional"]:
            lines.append(f"- 可选增强: {item}")
        lines.append("")
        lines.append("## 个股 / 量能 / 筹码特征")
        for item in family_archive["stock_features"]["required"]:
            lines.append(f"- 个股必要: {item}")
        for item in family_archive["amount_features"]["required"]:
            lines.append(f"- 量能必要: {item}")
        for item in family_archive["chip_features"]["required"]:
            lines.append(f"- 筹码必要: {item}")
        lines.append("")
        lines.append("## 介入与兑现")
        for item in family_archive["entry_conditions"]:
            lines.append(f"- 介入: {item}")
        for item in family_archive["exit_conditions"]:
            lines.append(f"- 兑现: {item}")
        for item in family_archive["veto_conditions"]:
            lines.append(f"- 否决: {item}")
        lines.append("")
        lines.append("## 典型日期与样本")
        lines.append(f"- 日期: {'、'.join(family_archive['example_dates']) or '无'}")
        for item in family_archive["reference_stocks"][:12]:
            lines.append(f"- {item['stock_name']} {item['code6']} | {item['role']}")
        return "\n".join(lines)

    def write_catalog(self, family_archives: List[Dict[str, Any]], cases: List[Dict[str, Any]]) -> Path:
        payload = {
            "teacher": self.teacher,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "families": [
                {
                    "strategy_key": item["strategy_key"],
                    "strategy_id": item["strategy_id"],
                    "strategy_name": item["strategy_name"],
                    "setup_type": item["setup_type"],
                    "example_dates": item["example_dates"],
                }
                for item in family_archives
            ],
            "date_to_family": {
                case["article_date"]: {
                    "primary_family": case["primary_family"],
                    "secondary_family": case.get("secondary_family"),
                }
                for case in cases
            },
            "family_to_dates": {
                item["strategy_key"]: [case["article_date"] for case in cases if case["primary_family"] == item["strategy_key"]]
                for item in family_archives
            },
        }
        path = self.archive_root / f"strategy_catalog_{self.teacher}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def write_index(self, cases: List[Dict[str, Any]], family_archives: List[Dict[str, Any]]) -> Path:
        payload = {
            "teacher": self.teacher,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "cases": [
                {
                    "article_date": item["article_date"],
                    "effective_trade_date": item["effective_trade_date"],
                    "strategy_id": item["strategy_id"],
                    "strategy_name": item["strategy_name"],
                    "primary_family": item["primary_family"],
                    "secondary_family": item.get("secondary_family"),
                    "family_confidence": item["family_confidence"],
                    "next_event": item["next_event"],
                }
                for item in cases
            ],
            "families": [
                {
                    "strategy_key": item["strategy_key"],
                    "strategy_id": item["strategy_id"],
                    "strategy_name": item["strategy_name"],
                    "example_dates": item["example_dates"],
                }
                for item in family_archives
            ],
            "date_to_family": {
                item["article_date"]: {
                    "primary_family": item["primary_family"],
                    "secondary_family": item.get("secondary_family"),
                }
                for item in cases
            },
            "family_to_dates": {
                family["strategy_key"]: [case["article_date"] for case in cases if case["primary_family"] == family["strategy_key"]]
                for family in family_archives
            },
        }
        path = self.archive_root / f"strategy_index_{self.teacher}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def find_next_event_date(self, date_str: str) -> Optional[str]:
        dates = self.list_article_dates()
        for item in dates:
            if item > date_str:
                return item
        return None

    def list_article_dates(self) -> List[str]:
        teacher_dir = self.article_root / self.teacher
        if not teacher_dir.exists():
            return []
        dates = []
        for path in teacher_dir.glob("*.md"):
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}\.md", path.name):
                dates.append(path.stem)
        return sorted(dates)

    def load_existing_cases(self) -> List[Dict[str, Any]]:
        cases = []
        for path in sorted(self.archive_root.glob(f"strategy_case_*_{self.teacher}.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if payload.get("article_date"):
                cases.append(payload)
        return sorted(cases, key=lambda item: item["article_date"])

    def rebuild_peer_dates(self, cases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        family_to_dates: Dict[str, List[str]] = {}
        for case in cases:
            family_to_dates.setdefault(case["primary_family"], []).append(case["article_date"])
        for case in cases:
            case["peer_dates_within_family"] = [
                item for item in family_to_dates.get(case["primary_family"], []) if item != case["article_date"]
            ]
        return cases

    def _build_market_context(self, report: Dict[str, Any]) -> Dict[str, Any]:
        latest = report["market_window_5d"][-1]
        rotation = report["rotation_analysis"]
        return {
            "market_stage": rotation["stage"],
            "emotion_cycle": report["emotion_cycle"]["cycle"],
            "market_window_5d": report["market_window_5d"],
            "hot_plates_today": [plate["name"] for plate in latest.get("hot_plates", [])],
            "hot_plate_source": latest.get("hot_plate_source"),
            "dominant_rotation_themes": self._unique_keep_order(rotation.get("daily_top_themes", [])),
            "rotation_reason": rotation.get("reason"),
            "top_theme_today": [item["theme"] for item in latest.get("top_themes", [])],
        }

    def _build_stock_cases(self, report: Dict[str, Any], instance: Dict[str, Any]) -> List[Dict[str, Any]]:
        overrides = instance.get("stock_overrides", {})
        cases = []
        for stock in report["sample_stocks"]:
            override = overrides.get(stock["stock_name"], {})
            cases.append(
                {
                    "stock_name": stock["stock_name"],
                    "code6": stock["code6"],
                    "primary_plate": stock["primary_plate"],
                    "related_themes": stock.get("related_themes", []),
                    "role_label": override.get("role_label", stock.get("role", "观察")),
                    "phase": stock.get("phase"),
                    "entry_style": override.get("entry_style", self._default_entry_style(stock)),
                    "exit_style": override.get("exit_style", self._default_exit_style(stock)),
                    "entry_reason": stock.get("selection_reason"),
                    "shape_tags": stock.get("shape_tags", []),
                    "chip_tags": stock.get("chip_profile", {}).get("tags", []),
                    "amount_tags": stock.get("amount_profile", {}).get("tags", []),
                    "risk_points": override.get("risk_points", self._default_risk_points(stock)),
                    "peer_comparison": stock.get("peer_comparison", {}),
                }
            )
        return cases

    def _collect_risk_flags(self, stock_cases: List[Dict[str, Any]], family: Dict[str, Any], exit_playbook: Dict[str, Any]) -> List[str]:
        flags = set(family["risk_flags"])
        for case in stock_cases:
            for item in case.get("risk_points", []):
                flags.add(item)
        for item in exit_playbook["fail_fast_signals"]:
            flags.add(item)
        return sorted(flags)

    def _build_system_mapping(self, family: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "market_tags": self._unique_keep_order(family["market_features"]["required"] + family["market_features"]["optional"][:1]),
            "theme_tags": self._unique_keep_order(family["theme_features"]["required"] + family["theme_features"]["optional"][:1]),
            "stock_tags": self._unique_keep_order(
                family["stock_features"]["required"][:2]
                + family["amount_features"]["required"][:1]
                + family["chip_features"]["required"][:1]
            ),
            "triggers": family["entry_conditions"],
            "vetoes": family["veto_conditions"],
        }

    def _default_entry_playbook(self, family: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "preconditions": family["market_features"]["required"] + family["theme_features"]["required"][:1],
            "watch_window": family["watch_window"],
            "confirm_signals": family["entry_conditions"],
            "veto_signals": family["veto_conditions"],
        }

    def _default_exit_playbook(self, family: Dict[str, Any]) -> Dict[str, Any]:
        reduce_signals = family["amount_features"]["optional"][:1] or family["stock_features"]["optional"][:1] or ["量价背离后减仓"]
        return {
            "take_profit_signals": family["exit_conditions"],
            "reduce_signals": reduce_signals,
            "fail_fast_signals": family["veto_conditions"][:2],
        }

    def _template_notes(self, family_key: str, article_text: str) -> str:
        if family_key == "midcycle_resonance_family":
            return "本次更强调跨周期共振和细分家族联动，而不是日内最热主线。"
        if family_key == "dragon_second_stage_family":
            return "本次更强调总龙、伴生股和主升二阶段接力。"
        if family_key == "high_level_emotion_family":
            return "本次更强调高位情绪边界、断板反包和退潮甄别。"
        if family_key == "framework_meta_family":
            return "本次更像老师的方法论沉淀，而非单日直接下单策略。"
        if "突破" in article_text:
            return "本次更强调趋势突破后的主升延续。"
        return "本次从市场环境切入，再映射到可执行策略族。"

    def _article_excerpt_hint(self, article_text: str) -> str:
        article_text = re.sub(r"\s+", " ", article_text).strip()
        return article_text[:180]

    def _default_entry_style(self, stock: Dict[str, Any]) -> str:
        phase = stock.get("phase", "")
        shape = stock.get("shape_tags", [])
        if "放量突破" in shape:
            return "突破确认"
        if "主升" in phase:
            return "主升承接"
        if "预热" in phase:
            return "低吸观察"
        return "分歧转强"

    def _default_exit_style(self, stock: Dict[str, Any]) -> str:
        amount_tags = stock.get("amount_tags", [])
        if "尾段量能回落" in amount_tags:
            return "冲高分批兑现"
        if "启动日爆量" in amount_tags:
            return "爆量后严格跟随承接兑现"
        return "跌破承接位离场"

    def _default_risk_points(self, stock: Dict[str, Any]) -> List[str]:
        points = []
        phase = stock.get("phase", "")
        amount_tags = stock.get("amount_tags", [])
        if "高位" in phase:
            points.append("高位分歧风险")
        if "尾段量能回落" in amount_tags:
            points.append("尾段量能回落")
        if not points:
            points.append("题材切换风险")
        return points

    def _collect_family_reference_stocks(self, cases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        rows = []
        for case in cases:
            for stock in case.get("reference_stocks", []):
                key = (stock["stock_name"], stock["code6"])
                if key in seen:
                    continue
                seen.add(key)
                rows.append(stock)
        return rows[:20]

    def _get_case_assignment(self, date_str: str) -> Dict[str, Any]:
        return CASE_ASSIGNMENTS[date_str]

    def _get_family_definition(self, family_key: str) -> Dict[str, Any]:
        return FAMILY_DEFINITIONS[family_key]

    def _load_article_text(self, date_str: str) -> str:
        path = self.article_root / self.teacher / f"{date_str}.md"
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8")

    def _unique_keep_order(self, items: List[str]) -> List[str]:
        seen = set()
        result = []
        for item in items:
            if not item or item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build strategy-first archives from teacher probes")
    parser.add_argument("--date", action="append", required=True, help="Article date, e.g. 2026-03-08")
    parser.add_argument("--teacher", default="niepan")
    parser.add_argument("--chain-next", type=int, default=0)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    builder = StrategyArchiveBuilder(teacher=args.teacher)

    dates = list(args.date)
    if args.chain_next > 0 and dates:
        all_dates = builder.list_article_dates()
        tail = dates[-1]
        if tail in all_dates:
            idx = all_dates.index(tail)
            dates.extend(all_dates[idx + 1 : idx + 1 + args.chain_next])

    new_cases = [builder.build_case_archive(date_str) for date_str in dates]
    existing = {case["article_date"]: case for case in builder.load_existing_cases()}
    for case in new_cases:
        existing[case["article_date"]] = case
    all_cases = builder.rebuild_peer_dates([existing[key] for key in sorted(existing)])

    for case in all_cases:
        json_path, md_path = builder.write_case_archive(case)
        if case["article_date"] in dates:
            print(
                json.dumps(
                    {
                        "article_date": case["article_date"],
                        "effective_trade_date": case["effective_trade_date"],
                        "primary_family": case["primary_family"],
                        "strategy_id": case["strategy_id"],
                        "json_path": str(json_path),
                        "md_path": str(md_path),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )

    family_archives = []
    for family_key in FAMILY_DEFINITIONS:
        family_archive = builder.build_family_archive(family_key, all_cases)
        builder.write_family_archive(family_archive)
        family_archives.append(family_archive)

    catalog_path = builder.write_catalog(family_archives, all_cases)
    index_path = builder.write_index(all_cases, family_archives)
    logger.info("strategy catalog written to %s", catalog_path)
    logger.info("strategy archive index written to %s", index_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
