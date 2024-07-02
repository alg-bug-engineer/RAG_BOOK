from py2neo import Graph, Node, Relationship
import json
from datetime import datetime
import os

# 连接到 Neo4j 数据库
graph = Graph("bolt://localhost:7687", auth=("neo4j", "12345678"))

# 清空数据库（谨慎使用）
graph.delete_all()

# 创建索引和约束
def create_constraints_and_indexes():
    graph.run("CREATE CONSTRAINT document_id IF NOT EXISTS FOR (d:Document) REQUIRE d.ID IS UNIQUE")
    graph.run("CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.ID IS UNIQUE")
    graph.run("CREATE CONSTRAINT keyword_id IF NOT EXISTS FOR (k:Keyword) REQUIRE k.ID IS UNIQUE")
    graph.run("CREATE CONSTRAINT rag_component_id IF NOT EXISTS FOR (r:RAGComponent) REQUIRE r.ID IS UNIQUE")
    graph.run("CREATE CONSTRAINT rag_flow_id IF NOT EXISTS FOR (f:RAGFlow) REQUIRE f.ID IS UNIQUE")

    graph.run("CREATE INDEX document_title IF NOT EXISTS FOR (d:Document) ON (d.title)")
    graph.run("CREATE INDEX document_publish_time IF NOT EXISTS FOR (d:Document) ON (d.publish_time)")
    graph.run("CREATE INDEX author_name IF NOT EXISTS FOR (a:Author) ON (a.name)")
    graph.run("CREATE INDEX keyword_name IF NOT EXISTS FOR (k:Keyword) ON (k.name)")

# 处理属性值，将复杂结构序列化
def process_properties(properties):
    for key, value in properties.items():
        if isinstance(value, (dict, list)):
            properties[key] = json.dumps(value)
    return properties

# 创建或更新节点的函数
def create_or_update_node(label, properties):
    properties = process_properties(properties)
    node = Node(label, **properties)
    graph.merge(node, label, "ID")
    return node

# 创建关系的函数
def create_relationship(start_node, end_node, relationship_type):
    rel = Relationship(start_node, relationship_type, end_node)
    graph.merge(rel)

# 处理文档数据的函数
def process_document(doc_data):
    # 创建文档节点
    doc_node = create_or_update_node("Document", doc_data["文档节点"])

    # 处理作者
    for author_data in doc_data["作者节点"]:
        author_node = create_or_update_node("Author", author_data)
        create_relationship(doc_node, author_node, "AUTHORED_BY")
        
        # 创建作者之间的合作关系
        for other_author_data in doc_data["作者节点"]:
            if author_data["ID"] != other_author_data["ID"]:
                other_author_node = create_or_update_node("Author", other_author_data)
                create_relationship(author_node, other_author_node, "COLLABORATES_WITH")

    # 处理关键词
    for keyword_data in doc_data["关键词节点"]:
        keyword_node = create_or_update_node("Keyword", keyword_data)
        create_relationship(doc_node, keyword_node, "HAS_KEYWORD")
        
        # 创建关键词之间的共现关系
        for other_keyword_data in doc_data["关键词节点"]:
            if keyword_data["ID"] != other_keyword_data["ID"]:
                other_keyword_node = create_or_update_node("Keyword", other_keyword_data)
                create_relationship(keyword_node, other_keyword_node, "CO_OCCURS_WITH")

    # 处理RAG组件
    for component_data in doc_data["RAG组件节点"]:
        component_node = create_or_update_node("RAGComponent", component_data)
        component_type = component_data["类型"].upper()
        create_relationship(doc_node, component_node, f"USES_{component_type}")

    # 处理RAG流程
    flow_data = doc_data["RAG流程节点"]
    flow_node = create_or_update_node("RAGFlow", flow_data)
    create_relationship(doc_node, flow_node, "IMPLEMENTS")

    # 连接RAG流程和RAG组件
    for component_type in ["索引构建", "检索", "重排", "生成"]:
        component_ids = flow_data.get(f"{component_type}组件ID", [])
        if isinstance(component_ids, str):
            component_ids = [component_ids]  # 转换单个ID为列表

        if not component_ids:  # 跳过None
            continue
        for component_id in component_ids:
            component_node = graph.nodes.match("RAGComponent", ID=component_id).first()
            if component_node:
                create_relationship(flow_node, component_node, f"USES_{component_type.upper()}")


def main():
    create_constraints_and_indexes()

    # nun_files = min(len(os.listdir(directory)) + 1, 101)
    directory = "./kg_data"
    for f in os.listdir(directory):
        print(f"::::current file: {f}")
        file_path = os.path.join(directory, f)
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        process_document(data)

    # # 读取JSON数据
    # with open('paste.txt', 'r', encoding='utf-8') as f:
    #     data = json.load(f)

    # # 处理文档数据
    # process_document(data)

    print("Knowledge graph has been successfully built.")

if __name__ == "__main__":
    main()