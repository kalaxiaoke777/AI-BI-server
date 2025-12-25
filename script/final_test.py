#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终测试修复后的JSON解析逻辑
"""

import requests
import re
import ast

if __name__ == "__main__":
    print("最终测试修复后的JSON解析逻辑...")
    
    try:
        # 发送真实请求，测试实际修复效果
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://fund.eastmoney.com",
        }
        
        response = requests.get("https://fund.eastmoney.com/js/fundcode_search.js", headers=headers, timeout=10)
        response.raise_for_status()
        
        content = response.text
        print(f"获取到真实响应，长度: {len(content)}")
        
        # 使用我们修复后的逻辑解析
        array_pattern = r"var r = (\[.*?\]);"
        array_match = re.search(array_pattern, content, re.DOTALL)
        
        if array_match:
            array_str = array_match.group(1)
            print(f"成功提取数组，长度: {len(array_str)}")
            
            # 使用ast.literal_eval()解析
            fund_data = ast.literal_eval(array_str)
            print(f"成功解析数组，包含 {len(fund_data)} 个基金")
            
            # 显示前5个基金
            print("前5个基金数据:")
            for fund in fund_data[:5]:
                if len(fund) >= 5:
                    print(f"基金代码: {fund[0]}, 基金名称: {fund[2]}, 基金类型: {fund[3]}")
            
            print("\n修复成功！JSON解析失败问题已解决。")
        else:
            print("未找到数组数据")
            
    except Exception as e:
        print(f"测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
