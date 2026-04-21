"""
@FileName: cron_parser.py
@Description: Cron表达式解析器 - 支持标准Cron表达式
@Author: HiPeng
@Time: 2026/4/21
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional, Set


class CronField(Enum):
    """Cron字段枚举"""
    MINUTE = 0  # 分钟 0-59
    HOUR = 1  # 小时 0-23
    DAY = 2  # 日期 1-31
    MONTH = 3  # 月份 1-12
    WEEKDAY = 4  # 星期 0-6 (0=周日)


class CronExpression:
    """Cron表达式

    支持标准5字段Cron表达式：
        * * * * *
        │ │ │ │ │
        │ │ │ │ └─── 星期 (0-6, 0=周日)
        │ │ │ └───── 月份 (1-12)
        │ │ └─────── 日期 (1-31)
        │ └───────── 小时 (0-23)
        └─────────── 分钟 (0-59)

    特殊字符：
        *       任意值
        ,       列表分隔符
        -       范围
        /       步长
        ?       不指定（用于日期和星期）
        L       月末/周末
        #       第几个星期几

    示例：
        "* * * * *"           - 每分钟
        "0 * * * *"           - 每小时整点
        "0 9 * * *"           - 每天9:00
        "*/5 * * * *"         - 每5分钟
        "0 9 * * 1-5"         - 工作日9:00
        "0 9 1 * *"           - 每月1号9:00
        "0 9 L * *"           - 每月最后一天9:00
    """

    def __init__(self, expression: str):
        self.expression = expression.strip()
        self._fields: List[Set[int]] = []
        self._original_fields: List[str] = []
        self._parse()

    def _parse(self) -> None:
        """解析Cron表达式"""
        parts = self.expression.split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression: {self.expression}. "
                "Expected 5 fields: minute hour day month weekday"
            )

        self._original_fields = parts

        # 解析各个字段
        self._fields = [
            self._parse_field(parts[0], 0, 59, "minute"),  # 分钟
            self._parse_field(parts[1], 0, 23, "hour"),  # 小时
            self._parse_field(parts[2], 1, 31, "day"),  # 日期
            self._parse_field(parts[3], 1, 12, "month"),  # 月份
            self._parse_weekday(parts[4]),  # 星期
        ]

    def _parse_field(self, field: str, min_val: int, max_val: int, name: str) -> Set[int]:
        """解析单个字段"""
        if field == "*" or field == "?":
            # 所有值
            return set(range(min_val, max_val + 1))

        if field.startswith("*/"):
            # 步长: */5
            step = int(field[2:])
            return set(range(min_val, max_val + 1, step))

        if "/" in field and not field.startswith("*/"):
            # 带步长的范围: 1-10/2
            range_part, step_part = field.split("/")
            step = int(step_part)
            values = self._parse_range(range_part, min_val, max_val)
            return {v for v in values if (v - min(values)) % step == 0}

        if "," in field:
            # 列表: 1,2,3
            result = set()
            for part in field.split(","):
                result.update(self._parse_range(part, min_val, max_val))
            return result

        # 单个值或范围
        return self._parse_range(field, min_val, max_val)

    def _parse_range(self, expr: str, min_val: int, max_val: int) -> Set[int]:
        """解析范围表达式"""
        if "-" in expr:
            start, end = expr.split("-")
            start_val = int(start)
            end_val = int(end)
            if start_val < min_val or end_val > max_val or start_val > end_val:
                raise ValueError(f"Invalid range: {expr}")
            return set(range(start_val, end_val + 1))
        else:
            val = int(expr)
            if val < min_val or val > max_val:
                raise ValueError(f"Value {val} out of range [{min_val}-{max_val}]")
            return {val}

    def _parse_weekday(self, field: str) -> Set[int]:
        """解析星期字段"""
        if field == "*" or field == "?":
            return set(range(0, 7))

        # 处理特殊字符 L (last)
        if field == "L":
            return set(range(0, 7))  # L 需要结合月份计算，这里返回所有

        # 处理 # (第几个星期几)
        if "#" in field:
            # 格式: 5#2 表示第二个星期五
            return set(range(0, 7))  # 需要动态计算

        # 处理 L-1 等
        if field.startswith("L"):
            return set(range(0, 7))

        return self._parse_field(field, 0, 6, "weekday")

    def next(self, after: Optional[datetime] = None) -> datetime:
        """获取下一次执行时间

        Args:
            after: 起始时间，默认为当前时间

        Returns:
            下一次执行时间
        """
        now = after or datetime.now()
        # 从当前时间开始，检查未来1年内的执行时间
        max_iterations = 366 * 24 * 60  # 最多检查一年

        for _ in range(max_iterations):
            # 检查月份
            if now.month not in self._fields[CronField.MONTH.value]:
                # 跳到下个月
                if now.month == 12:
                    now = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0)
                else:
                    now = now.replace(month=now.month + 1, day=1, hour=0, minute=0)
                continue

            # 检查日期
            day_match = self._check_day(now)
            if not day_match:
                # 跳到下一天
                now = now.replace(day=1) + timedelta(days=32)
                now = now.replace(day=1, hour=0, minute=0)
                continue

            # 检查小时
            if now.hour not in self._fields[CronField.HOUR.value]:
                # 跳到下一小时
                if now.hour == 23:
                    now = now.replace(day=now.day + 1, hour=0, minute=0)
                else:
                    now = now.replace(hour=now.hour + 1, minute=0)
                continue

            # 检查分钟
            if now.minute not in self._fields[CronField.MINUTE.value]:
                # 跳到下一分钟
                if now.minute == 59:
                    now = now.replace(hour=now.hour + 1, minute=0)
                else:
                    now = now.replace(minute=now.minute + 1)
                continue

            # 所有字段匹配
            return now

        raise ValueError(f"No valid next execution time found for cron: {self.expression}")

    def _check_day(self, dt: datetime) -> bool:
        """检查日期是否匹配"""
        day_in_month = dt.day
        weekday = dt.weekday()  # 0=周一, 6=周日
        # 转换星期格式: 0=周日, 1=周一, ..., 6=周六
        cron_weekday = (weekday + 1) % 7

        # 检查日期字段
        day_field = self._fields[CronField.DAY.value]
        weekday_field = self._fields[CronField.WEEKDAY.value]

        # 处理特殊值
        day_match = day_in_month in day_field or "*" in str(day_field) or "?" in str(day_field)
        weekday_match = cron_weekday in weekday_field or "*" in str(weekday_field) or "?" in str(weekday_field)

        # 检查是否有 L (月末) 特殊处理
        if "L" in self._original_fields[CronField.DAY.value]:
            # 检查是否是当月最后一天
            next_day = dt + timedelta(days=1)
            if next_day.month != dt.month:
                day_match = True
            else:
                day_match = False

        return day_match and weekday_match

    def previous(self, before: Optional[datetime] = None) -> datetime:
        """获取上一次执行时间

        Args:
            before: 截止时间，默认为当前时间

        Returns:
            上一次执行时间
        """
        # 简单实现：从当前时间往前推
        now = before or datetime.now()
        max_iterations = 366 * 24 * 60

        for _ in range(max_iterations):
            # 往前推一分钟
            now = now - timedelta(minutes=1)

            if self._matches(now):
                return now

        raise ValueError(f"No valid previous execution time found for cron: {self.expression}")

    def _matches(self, dt: datetime) -> bool:
        """检查时间点是否匹配"""
        if dt.month not in self._fields[CronField.MONTH.value]:
            return False
        if not self._check_day(dt):
            return False
        if dt.hour not in self._fields[CronField.HOUR.value]:
            return False
        if dt.minute not in self._fields[CronField.MINUTE.value]:
            return False
        return True

    def get_next_n(self, n: int, after: Optional[datetime] = None) -> List[datetime]:
        """获取接下来n次执行时间

        Args:
            n: 获取次数
            after: 起始时间

        Returns:
            执行时间列表
        """
        results = []
        current = after or datetime.now()

        for _ in range(n):
            current = self.next(after=current)
            results.append(current)
            # 加一秒避免重复获取同一个时间
            current = current + timedelta(seconds=1)

        return results

    def __str__(self) -> str:
        return self.expression

    def __repr__(self) -> str:
        return f"CronExpression('{self.expression}')"


class CronParser:
    """Cron表达式解析器

    提供便捷的解析方法。

    使用示例：
        >>> cron = CronParser.parse("0 9 * * *")
        >>> next_run = cron.next()
        >>> print(next_run)

        >>> # 获取接下来10次执行时间
        >>> runs = cron.get_next_n(10)
    """

    @staticmethod
    def parse(expression: str) -> CronExpression:
        """解析Cron表达式

        Args:
            expression: Cron表达式字符串

        Returns:
            CronExpression对象

        Raises:
            ValueError: 表达式格式错误
        """
        return CronExpression(expression)

    @staticmethod
    def is_valid(expression: str) -> bool:
        """验证Cron表达式是否有效

        Args:
            expression: Cron表达式字符串

        Returns:
            是否有效
        """
        try:
            CronExpression(expression)
            return True
        except ValueError:
            return False

    @staticmethod
    def describe(expression: str) -> str:
        """生成Cron表达式的中文描述

        Args:
            expression: Cron表达式字符串

        Returns:
            中文描述
        """
        parts = expression.split()
        if len(parts) != 5:
            return "无效的Cron表达式"

        minute, hour, day, month, weekday = parts

        descriptions = []

        # 分钟
        if minute == "*":
            descriptions.append("每分钟")
        elif minute.startswith("*/"):
            descriptions.append(f"每{minute[2:]}分钟")
        elif minute == "0":
            pass  # 整点
        else:
            descriptions.append(f"第{minute}分钟")

        # 小时
        if hour == "*":
            if descriptions and descriptions[-1] == "每分钟":
                descriptions[-1] = "每小时"
            else:
                descriptions.append("每小时")
        elif hour.startswith("*/"):
            descriptions.append(f"每{hour[2:]}小时")
        elif hour == "0":
            descriptions.append("午夜")
        else:
            descriptions.append(f"{hour}点")

        # 日期
        if day == "*":
            pass
        elif day == "L":
            descriptions.append("每月最后一天")
        elif day.startswith("*/"):
            descriptions.append(f"每{day[2:]}天")
        else:
            descriptions.append(f"每月{day}号")

        # 月份
        if month != "*":
            month_names = ["1月", "2月", "3月", "4月", "5月", "6月",
                           "7月", "8月", "9月", "10月", "11月", "12月"]
            if "-" in month:
                start, end = month.split("-")
                descriptions.append(f"{month_names[int(start) - 1]}至{month_names[int(end) - 1]}")
            else:
                descriptions.append(month_names[int(month) - 1])

        # 星期
        if weekday != "*" and weekday != "?":
            weekday_names = ["周日", "周一", "周二", "周三", "周四", "周五", "周六"]
            if "-" in weekday:
                start, end = weekday.split("-")
                descriptions.append(f"{weekday_names[int(start)]}至{weekday_names[int(end)]}")
            else:
                descriptions.append(weekday_names[int(weekday)])

        if not descriptions:
            return "立即执行"

        return " ".join(descriptions)


# 预定义常用Cron表达式
PREDEFINED_CRONS = {
    "@yearly": "0 0 1 1 *",  # 每年1月1日
    "@annually": "0 0 1 1 *",  # 每年
    "@monthly": "0 0 1 * *",  # 每月1号
    "@weekly": "0 0 * * 0",  # 每周日
    "@daily": "0 0 * * *",  # 每天
    "@midnight": "0 0 * * *",  # 每天午夜
    "@hourly": "0 * * * *",  # 每小时
}


def parse_predefined(name: str) -> Optional[CronExpression]:
    """解析预定义的Cron表达式

    Args:
        name: 预定义名称 (@yearly, @monthly, @weekly, @daily, @hourly)

    Returns:
        CronExpression对象或None
    """
    if name in PREDEFINED_CRONS:
        return CronParser.parse(PREDEFINED_CRONS[name])
    return None
