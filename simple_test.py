#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试修复后的JSON解析逻辑
"""

import re
import ast

# 模拟东方财富返回的JavaScript响应
mock_response = '''var r = [["000001","HXCZHH","华夏成长混合","混合型-偏股","HXCZHH"],["000002","HXCZHH2","华夏成长混合2","混合型-偏股2","HXCZHH2"]];'''  

print("测试修复后的JSON解析逻辑...")

# 使用正则表达式提取数组内容
array_pattern = r"var r = (\[.*?\]);"
array_match = re.search(array_pattern, mock_response, re.DOTALL)

if array_match:
    # 提取完整数组
    array_str = array_match.group(1)
    print(f"提取的完整数组: {array_str[:100]}...")
    
    # 使用ast.literal_eval()安全解析
    fund_data = ast.literal_eval(array_str)
    print(f"解析后的基金数据: {fund_data}")
    
    # 转换为结构化数据
    result = []
    for fund in fund_data:
        if len(fund) >= 5:
            result.append(
                {
                    "fund_code": fund[0],
                    "short_name": fund[1],
                    "fund_name": fund[2],
                    "fund_type": fund[3],
                    "pinyin": fund[4],
                }
            )
    
    print(f"最终结果: {result}")
    print("测试成功!")
else:
    print("测试失败，未找到数组数据")
