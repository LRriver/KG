import uuid
from pymongo import MongoClient, ASCENDING, UpdateOne

# --- MongoDB 连接配置 ---
MONGO_HOST = "121.48.163.69"
MONGO_PORT = 27018
MONGO_USERNAME = "root"
MONGO_PASSWORD = "rootdb@"
MONGO_DB_NAME = "2024-12-2-task"
COLLECTION_NAME = "TSMC-Event"


BATCH_SIZE = 500  

def main():
    """
    连接到MongoDB，为 'TSMC-Event' 集合中 'unit_events' 里的每个事件添加UUID。
    """
    print("--- 启动为事件添加UUID的脚本 ---")
    
    # 1. 连接数据库
    try:
        client_mongo = MongoClient(
            host=MONGO_HOST,
            port=MONGO_PORT,
            username=MONGO_USERNAME,
            password=MONGO_PASSWORD,
            serverSelectionTimeoutMS=5000
        )
        client_mongo.server_info()
        db = client_mongo[MONGO_DB_NAME]
        collection = db[COLLECTION_NAME]
        print("✅ MongoDB 连接成功。")
    except Exception as e:
        print(f"❌ MongoDB 连接失败: {e}")
        return

    # 2. 使用高效的分批循环处理整个集合
    last_id = None
    total_docs_processed = 0
    total_events_updated = 0

    while True:
        print(f"\n--- 正在读取下一批数据 (从 _id > {last_id} 开始)...")
        query = {"_id": {"$gt": last_id}} if last_id else {}
        
        # 按 _id 排序以保证分页的稳定性
        documents_batch = list(
            collection.find(query).sort("_id", ASCENDING).limit(BATCH_SIZE)
        )

        if not documents_batch:
            print("--- 检测到没有更多数据，所有处理已完成。 ---")
            break

        last_id = documents_batch[-1]['_id']
        print(f"成功读取 {len(documents_batch)} 条数据，准备在本地进行处理...")

        # 3. 在Python中处理每个文档并准备更新操作
        update_operations = []
        for doc in documents_batch:
            doc_id = doc['_id']
            # 使用 .get() 并提供默认空列表，确保代码健壮性
            unit_events = doc.get('unit_events', [])
            
            # 检查 unit_events 是否为空列表
            if unit_events and isinstance(unit_events, list):
                # 列表不为空，遍历并添加UUID
                events_with_uuid_count = 0
                for event in unit_events:
                    # 确保只为没有uuid的事件添加，防止重复运行脚本时出错
                    if 'event_uuid' not in event:
                        event['event_uuid'] = str(uuid.uuid4())
                        events_with_uuid_count += 1
                
                # 准备更新操作
                update_payload = {
                    "$set": {
                        "unit_events": unit_events, # 更新包含uuid的整个列表
                        "has_events": True          # 添加标志位
                    }
                }
                update_operations.append(UpdateOne({'_id': doc_id}, update_payload))
                total_events_updated += events_with_uuid_count
            else:
                # 列表为空或字段不存在，只更新标志位
                update_payload = {"$set": {"has_events": False}}
                update_operations.append(UpdateOne({'_id': doc_id}, update_payload))

        # 4. 执行批量更新
        if update_operations:
            print(f"--- 正在将 {len(update_operations)} 条更新写入数据库...")
            try:
                result = collection.bulk_write(update_operations)
                print(f"✅ 数据库更新成功。匹配: {result.matched_count}, 修改: {result.modified_count}")
                total_docs_processed += result.modified_count
            except Exception as e:
                print(f"❌ 数据库批量更新失败: {e}")
        else:
            print("--- 当前批次无需更新。 ---")

    print(f"\n🎉 脚本执行完毕！")
    print(f"   共检查并更新了 {total_docs_processed} 个文档。")
    print(f"   共为 {total_events_updated} 个独立事件添加了UUID。")
    client_mongo.close()


if __name__ == "__main__":
    main()