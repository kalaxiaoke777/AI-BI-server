import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.scrapers.eastmoney import EastMoneyScraper

def test_fund_company_relation():
    """测试基金-公司关系同步逻辑"""
    scraper = EastMoneyScraper()
    
    print("=== 测试基金-公司关系同步 ===")
    
    # 1. 测试获取单个公司的基金列表
    print("\n1. 测试获取单个公司的基金列表:")
    company_id = "80205268"  # 东海基金公司
    company_name = "东海基金管理有限责任公司"
    funds = scraper.get_funds_by_company_id(company_id, company_name)
    print(f"公司: {company_name}，基金数量: {len(funds)}")
    
    if funds:
        print("前3只基金:")
        for fund in funds[:3]:
            print(f"  - {fund['fund_code']}: {fund['fund_name']} (类型: {fund['fund_type']}, 经理: {fund['manager']})")
    
    # 2. 测试获取基金-公司关系（指定基金代码）
    print("\n2. 测试获取基金-公司关系（指定基金代码）:")
    test_fund_codes = ["007439", "007463", "018886"]  # 东海基金旗下基金
    relations = scraper.get_fund_company_relation(test_fund_codes)
    print(f"指定基金代码的关系数量: {len(relations)}")
    
    for relation in relations:
        print(f"  - 基金: {relation['fund_code']} {relation['fund_name']}，公司: {relation['company_name']}")
    
    # 3. 测试获取所有基金-公司关系（仅前10个公司）
    print("\n3. 测试获取所有基金-公司关系（仅前10个公司）:")
    # 临时修改get_fund_company_list方法，只返回前10个公司用于测试
    original_get_fund_company_list = scraper.get_fund_company_list
    
    def limited_get_fund_company_list():
        companies = original_get_fund_company_list()
        return companies[:10]  # 只返回前10个公司
    
    scraper.get_fund_company_list = limited_get_fund_company_list
    
    all_relations = scraper.get_fund_company_relation()
    print(f"前10个公司的基金关系数量: {len(all_relations)}")
    
    if all_relations:
        print("前5条关系:")
        for relation in all_relations[:5]:
            print(f"  - 基金: {relation['fund_code']} {relation['fund_name']}，公司: {relation['company_name']}")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    test_fund_company_relation()
