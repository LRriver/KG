import os
import json
import time
from dotenv import load_dotenv
from openai import OpenAI
from pymongo import MongoClient
import concurrent.futures
from bson.objectid import ObjectId
from datetime import datetime


load_dotenv()

client = OpenAI(
    api_key=os.getenv("ds_api_key"),
    base_url="https://api.deepseek.com/v1",
)


# MongoDB 连接配置
MONGO_HOST =  os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
SOURCE_COLLECTION_NAME = os.getenv("SOURCE_COLLECTION_NAME")
TARGET_COLLECTION_NAME = os.getenv("TARGET_COLLECTION_NAME")




MAX_WORKERS = 3  # 并发任务数
BATCH_SIZE = 10    # 每批写入数据库的数量


# unit_event_prompt_template = (
#     "你是一个新闻分析助手。你会收到一段新闻。\n"
#     "你需要分析与这个文本里的事件，从新闻中提取重要事件（可能不唯一，但不要小事件，需要是文本中的主要事件）。\n"
#     "事件由时间、主体、客体、关系、事件描述组成，一个新闻可能有多个事件，但除非有必要，否则不要提取过多小事件，事件必须要在文件中有代表性，要能概括文本的主要事件。\n"
#     "input: 这里是实际的新闻输入内容: {content}\n"
#     "新闻文本里的主要事件的格式如下：\n"
#     "- time 字段可以为空字符串\n"
#     "- subject 字段必须不为空\n"
#     "- relation 字段必须不为空（尽量简洁）\n"
#     "- object 字段必须不为空\n"
#     "- desc 字段必须不为空\n"
#     "请返回 JSON 格式字符串，例如："
#     '{{"time":"2025年", "subject":"TSMC", "relation":"投资", "object":"美国亚利桑那州", "desc":"全球领先的半导体制造商台积电（TSMC）近日宣布，将在美国亚利桑那州扩大投资..."}}'
#     "请仔细判别是否为主要事件，一个文本抽取的主要事件只能为1个或者2个，最多为2个，且不要重复，请避免事件有重复、包含关系或者十分相近，如ON Semiconductor Corp在2024财年第三季度营收下降是一个主要事件，但分析师预期营收、分析师预期每股收益就不属于主要事件，而应该是与事件相关的三元组。"
# )

# triplet_prompt_template = (
#     "你是一个新闻分析助手。你会收到一段新闻，以及基于这条文本抽取的事件四元组（就是传统的三元组+时间属性），\n"
#     "你需要基于这个文本抽取与给的事件四元组相关的三元组（可能不止一个），尽量要与事件有联系，抽取的三元组的头实体和尾实体很可能会与事件四元组的头实体、尾实体一样，这是很正常的，但注意三元组需要与事件四元组有部分区别，不要抽出与事件的除时间属性外的三元组完全一样的三元组\n"
#     "新闻内容：\n{news_text}\n"
#     "事件四元组：\n{unit_events}\n"
#     "请抽取相关的三元组，返回 JSON 格式列表，例如："
#     '[{"firstEntity": "Jensen Huang", "relation": "主导", "secondEntity": "Nvidia"}]'
# )

unit_event_prompt_template = (
    "你是一个新闻分析助手。你会收到一段新闻,文本可能有繁体中文或者其他国家的语言，但你最后输出的结果的语言都需要是简体中文。\\n"
    "你需要分析与这个文本里的事件，从新闻中提取重要事件（可能不唯一，但不要小事件，需要是文本中的主要事件）。\\n"
    "事件由时间、主体、客体、关系、事件描述组成，一个新闻可能有多个事件，但除非有必要，否则不要提取过多小事件，事件必须要在文件中有代表性，要能概括文本的主要事件。\\n"
    "你要抽取的是高度概括文本的主要事件,一个文本抽取的主要事件只能为1个或者2个，最多也只能为2个，且不要重复"
    "请避免事件有重复、包含关系或者十分相近，如ON Semiconductor Corp在2024财年第三季度营收下降是一个主要事件，但分析师预期营收、分析师预期每股收益就不属于主要事件，而应该是与事件相关的三元组。"
    "新闻文本里的主要事件的格式如下：\\n"
    "- time 字段可以为空字符串\\n"
    "- subject 字段必须不为空\\n"
    "- relation 字段必须不为空（尽量简洁）\\n"
    "- object 字段必须不为空\\n"
    "- desc 字段必须不为空\\n"
    "input: 这里是实际的新闻输入内容: {content}\\n"
    "请返回 JSON 格式字符串，例如："
    '{{"time":"2025年", "subject":"TSMC", "relation":"投资", "object":"美国亚利桑那州", "desc":"全球领先的半导体制造商台积电（TSMC）近日宣布，将在美国亚利桑那州扩大投资..."}}'
    "再次提醒,你要抽取的是高度概括文本的1个或者2个主要事件,你最后输出的结果的语言都需要是简体中文。"
)

triplet_prompt_template = (
    "你是一个新闻分析助手。你会收到一段新闻，以及基于这条文本抽取的事件四元组（就是传统的三元组+时间属性）,文本可能有繁体中文或者其他国家的语言，但你最后输出的结果的语言都需要是简体中文。 \\n"
    "你需要基于这个文本抽取与给的事件四元组相关的三元组（可能不止一个），尽量要与事件有联系，抽取的三元组的头实体和尾实体很可能会与事件四元组的头实体、尾实体一样，这是很正常的，但注意三元组需要与事件四元组有部分区别，不要抽出与事件的除时间属性外的三元组完全一样的三元组\\n"
    "新闻内容：\\n{news_text}\\n"
    "事件四元组：\\n{unit_events}\\n"
    "请抽取相关的三元组，返回 JSON 格式列表，例如："
    '[{{"firstEntity": "Jensen Huang", "relation": "主导", "secondEntity": "Nvidia"}}]'
    "再次提醒,你最后输出的结果的语言都需要是简体中文。"
)



def call_llm_with_retry(prompt, max_retries=3, retry_delay=5):
    """
    调用大模型API，并带有重试机制。
    """
    for attempt in range(max_retries):
        try:
            completion = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "你是一个专业的信息抽取助手。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=1.0
            )
            return completion.choices[0].message.content.strip()
        except Exception as e:
            print(f"API 调用失败，错误: {e}。正在进行第 {attempt + 1} 次重试...")
            time.sleep(retry_delay)
    print("API 调用在多次重试后仍然失败。")
    return None

def parse_json_response(response_text):
    """
    解析LLM返回的可能包含代码块标记的JSON字符串。
    """
    if response_text is None:
        return None
    try:
        # 移除可能的markdown代码块标记
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        return json.loads(response_text.strip())
    except json.JSONDecodeError:
        print(f"JSON 解析失败: {response_text}")
        return None

# def process_item(item):
#     """
#     处理单个数据项（提取四元组和三元组）。
#     """
#     content = item.get("page_content")
#     if not content:
#         return None

#     print(f"正在处理 ID: {item['_id']} 的新闻...")

#     # 1. 抽取事件四元组
#     unit_prompt = unit_event_prompt_template.format(content=content)
#     unit_response_raw = call_llm_with_retry(unit_prompt)
#     unit_events = parse_json_response(unit_response_raw)

#     if not unit_events:
#         print(f"ID: {item['_id']} 的事件四元组提取失败或为空。")
#         return None

#     # 2. 抽取三元组
#     triplet_prompt = triplet_prompt_template.format(
#         news_text=content,
#         unit_events=json.dumps(unit_events, ensure_ascii=False)
#     )
#     triplet_response_raw = call_llm_with_retry(triplet_prompt)
#     relations = parse_json_response(triplet_response_raw)
    
#     if not relations:
#         print(f"ID: {item['_id']} 的三元组提取失败或为空。")
#         # 即使三元组提取失败，我们可能仍然希望保存事件
#         relations = []


#     # 3. 整合结果
#     result = {
#         "source_id": item["_id"],
#         "page_link": item.get("page_link"),
#         "page_title": item.get("page_title"),
#         "news_content": content,
#         "unit_events": unit_events,
#         "relations": relations,
#         # "processed_time": time.time()
#     }
#     return result

def process_item(item):
    """
    处理单个数据项（提取四元组和三元组），并填充缺失的事件时间。
    """
    content = item.get("page_content")
    if not content:
        return None

    print(f"正在处理 ID: {item['_id']} 的新闻...")

    # 抽取事件四元组
    unit_prompt = unit_event_prompt_template.format(content=content)
    unit_response_raw = call_llm_with_retry(unit_prompt)
    unit_events = parse_json_response(unit_response_raw)

    if not unit_events:
        print(f"ID: {item['_id']} 的事件四元组提取失败或为空。")
        return None
        
    try:
        # 统一处理，无论返回的是单个dict还是list，都放入一个list中迭代
        events_to_process = unit_events if isinstance(unit_events, list) else [unit_events]

        for event in events_to_process:
            # 检查 time 字段是否为空或不存在
            if not event.get("time"):
                date1 = item.get("date1")
                if date1:
                    # 将 datetime 对象转换为 ISO 格式的字符串并填充
                    event['time'] = date1.isoformat()
                    print(f"ID: {item['_id']} - 事件时间为空，已用 date1: {event['time']} 填充。")

        # 如果原始数据是单个dict，则在处理后仍保持其格式
        if not isinstance(unit_events, list):
            unit_events = events_to_process[0]
            
    except Exception as e:
        print(f"ID: {item['_id']} - 填充时间时发生错误: {e}。将使用原始抽取结果。")


    # 基于事件四元组抽取三元组
    triplet_prompt = triplet_prompt_template.format(
        news_text=content,
        unit_events=json.dumps(unit_events, ensure_ascii=False)
    )
    triplet_response_raw = call_llm_with_retry(triplet_prompt)
    relations = parse_json_response(triplet_response_raw)
    
    if not relations:
        print(f"ID: {item['_id']} 的三元组提取失败或为空。")
        # 即使三元组提取失败，我们仍然希望保存事件
        relations = []


    # 整合结果 "date1" : ISODate("2025-04-26T03:04:22.000Z"),
    result = {
        "source_id": item["_id"],
        "page_link": item.get("page_link"),
        "page_title": item.get("page_title"),
        "data":item.get("date1"),
        "news_content": content,
        "unit_events": unit_events, # 这里保存的是已经填充过时间的事件
        "relations": relations,
        # "processed_time": time.time()
    }
    return result


def json_default_converter(o):
    """自定义JSON转换器，用于处理 ObjectId 和 datetime 等特殊类型。"""
    if isinstance(o, ObjectId):
        return str(o)
    if isinstance(o, datetime):
        return o.isoformat() # <--- 新增这行来处理datetime对象
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
def main():
    # 连接 MongoDB
    try:
        client_mongo = MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            serverSelectionTimeoutMS=5000 # 设置连接超时
        )
        # 检查连接
        client_mongo.server_info() 
        db = client_mongo[MONGO_DB_NAME]
        source_collection = db[SOURCE_COLLECTION_NAME]
        target_collection = db[TARGET_COLLECTION_NAME]
        print("✅ MongoDB 连接成功。")
    except Exception as e:
        print(f"❌ MongoDB 连接失败: {e}")
        return

    # 获取所有待处理的数据
    try:
        news_list = list(source_collection.find())
        # news_list = list(source_collection.find().limit(5))
        print(f"从 '{SOURCE_COLLECTION_NAME}' 表中找到 {len(news_list)} 条数据。")
    except Exception as e:
        print(f"❌ 从MongoDB读取数据失败: {e}")
        return

    results_batch = []
    total_processed = 0

    # 程池并发处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务
        future_to_item = {executor.submit(process_item, item): item for item in news_list}

        for future in concurrent.futures.as_completed(future_to_item):
            try:
                result = future.result()
                if result:
                    results_batch.append(result)
                    # 当达到批处理大小时，写入数据库
                    if len(results_batch) >= BATCH_SIZE:
                        print(f"--- 正在将 {len(results_batch)} 条结果写入数据库... ---")
                        target_collection.insert_many(results_batch)
                        total_processed += len(results_batch)
                        results_batch = []
            except Exception as exc:
                item_id = future_to_item[future].get('_id', '未知ID')
                print(f"ID: {item_id} 的数据在处理过程中产生错误: {exc}")

    # 处理完成后，将剩余的结果写入数据库
    if results_batch:
        print(f"--- 正在将最后 {len(results_batch)} 条结果写入数据库... ---")
        target_collection.insert_many(results_batch)
        total_processed += len(results_batch)

    print(f"\n✅ 全部处理完成！总共 {total_processed} 条结果已保存至 '{TARGET_COLLECTION_NAME}' 表。")
    client_mongo.close()

# --- 主程序 (修改为本地保存) ---

# def main():
#     # 连接 MongoDB (仅用于读取源数据)
#     try:
#         client_mongo = MongoClient(
#             host=MONGO_HOST,
#             port=MONGO_PORT,
#             username=MONGO_USERNAME,
#             password=MONGO_PASSWORD,
#             serverSelectionTimeoutMS=5000
#         )
#         client_mongo.server_info()
#         db = client_mongo[MONGO_DB_NAME]
#         source_collection = db[SOURCE_COLLECTION_NAME]
#         print("✅ MongoDB 连接成功 (用于读取数据)。")
#     except Exception as e:
#         print(f"❌ MongoDB 连接失败: {e}")
#         return

    
#     try:
#         news_list = list(source_collection.find().limit(5)) 
#         print(f"测试模式：从 '{SOURCE_COLLECTION_NAME}' 表中找到并限定处理 {len(news_list)} 条数据。")
#         if not news_list:
#             print("未找到任何数据，请检查源数据表是否为空。")
#             return
            
#     except Exception as e:
#         print(f"❌ 从MongoDB读取数据失败: {e}")
#         return

#     # 将所有结果收集到这个列表中
#     all_results = []
#     total_processed_count = 0

#     # 使用线程池进行并发处理
#     with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         future_to_item = {executor.submit(process_item, item): item for item in news_list}

#         for future in concurrent.futures.as_completed(future_to_item):
#             try:
#                 result = future.result()
#                 if result:
#                     # 不再分批，直接将结果添加到总列表中
#                     all_results.append(result)
#             except Exception as exc:
#                 item_id = future_to_item[future].get('_id', '未知ID')
#                 print(f"ID: {item_id} 的数据在处理过程中产生错误: {exc}")

#     total_processed_count = len(all_results)
#     print(f"\n--- 数据处理完成，共获得 {total_processed_count} 条有效结果。---")

#     # --- 将所有结果保存到本地JSON文件 ---
#     output_filename = "processed_results_local.json"
#     print(f"--- 正在将结果保存到本地文件: {output_filename} ---")
#     try:
#         with open(output_filename, "w", encoding="utf-8") as f:
#             # 使用自定义转换器来处理 ObjectId
#             json.dump(all_results, f, ensure_ascii=False, indent=4, default=json_default_converter)
#         print(f"✅ 测试完成！结果已成功保存至 {output_filename}")
#     except Exception as e:
#         print(f"❌ 保存到文件时出错: {e}")
        
#     client_mongo.close()


if __name__ == "__main__":
    main()