import json
import os
from typing import List, Dict, Any
from neo4j import GraphDatabase

class DataLoader:
    @staticmethod
    def load_json_files(directory: str) -> List[Dict[str, Any]]:
        data = []
        # nun_files = min(len(os.listdir(directory)) + 1, 101)
        for f in os.listdir(directory):
            print(f"::::current file: {f}")
            file_path = os.path.join(directory, f)
            with open(file_path, 'r', encoding='utf-8') as file:
                data.append(json.load(file))
        return data

class DataProcessor:
    @staticmethod
    def process_data(data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        processed_data = {
            "documents": [],
            "authors": [],
            "keywords": [],
            "rag_components": [],
            "rag_flows": []
        }
        
        for item in data:
            processed_data["documents"].append(item["文档节点"])
            processed_data["authors"].extend(item["作者节点"])
            processed_data["keywords"].extend(item["关键词节点"])
            processed_data["rag_components"].extend(DataProcessor.preprocess_rag_components(item["RAG组件节点"]))
            processed_data["rag_flows"].append(DataProcessor.preprocess_rag_flow(item["RAG流程节点"]))
        return processed_data

    @staticmethod
    def preprocess_rag_flow(rag_flow: Dict[str, Any]) -> Dict[str, Any]:
        if "整体性能指标" in rag_flow and isinstance(rag_flow["整体性能指标"], dict):
            rag_flow["整体性能指标"] = json.dumps(rag_flow["整体性能指标"])
        return rag_flow
    
    def preprocess_rag_components(rag_components: List[Dict]) -> List[Dict]:
        for ele in rag_components:
            if "性能指标" in ele and isinstance(ele["性能指标"], dict):
                ele["性能指标"] = json.dumps(ele["性能指标"])
        return rag_components

class Neo4jConnection:
    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def execute_query(self, query: str, parameters: Dict[str, Any] = None):
        try:
            with self._driver.session() as session:
                print(f"::::::query: {query}")
                result = session.run(query, parameters)
                return list(result)
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

class KnowledgeGraphBuilder:
    def __init__(self, neo4j_connection: Neo4jConnection):
        self.neo4j = neo4j_connection

    def create_constraints(self):
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.ID IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.ID IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (k:Keyword) REQUIRE k.ID IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:RAGComponent) REQUIRE c.ID IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (f:RAGFlow) REQUIRE f.ID IS UNIQUE"
        ]
        for constraint in constraints:
            self.neo4j.execute_query(constraint)

    def create_document_nodes(self, documents: List[Dict[str, Any]]):
        query = """
        UNWIND $documents AS doc
        MERGE (d:Document {ID: doc.ID})
        SET d += doc
        """
        self.neo4j.execute_query(query, {"documents": documents})

    def create_rag_flow_nodes(self, flows: List[Dict[str, Any]]):
        query = """
        UNWIND $flows AS flow
        MERGE (f:RAGFlow {ID: flow.ID})
        SET f += flow
        """
        self.neo4j.execute_query(query, {"flows": flows})

    def create_author_nodes(self, authors: List[Dict[str, Any]]):
        query = """
        UNWIND $authors AS author
        MERGE (a:Author {ID: author.ID})
        SET a += author
        """
        self.neo4j.execute_query(query, {"authors": authors})

    def create_keyword_nodes(self, keywords: List[Dict[str, Any]]):
        query = """
        UNWIND $keywords AS keyword
        MERGE (k:Keyword {ID: keyword.ID})
        SET k += keyword
        """
        self.neo4j.execute_query(query, {"keywords": keywords})

    def create_rag_component_nodes(self, components: List[Dict[str, Any]]):
        query = """
        UNWIND $components AS component
        MERGE (c:RAGComponent {ID: component.ID})
        SET c += component
        """
        self.neo4j.execute_query(query, {"components": components})

    def create_rag_flow_nodes(self, flows: List[Dict[str, Any]]):
        query = """
        UNWIND $flows AS flow
        MERGE (f:RAGFlow {ID: flow.ID})
        SET f += flow
        """
        self.neo4j.execute_query(query, {"flows": flows})

    def create_relationships(self):
        relationships = [
            """
            MATCH (d:Document), (a:Author)
            WHERE a.ID IN d.作者
            CREATE (a)-[:AUTHORED]->(d)
            """,
            """
            MATCH (d:Document), (k:Keyword)
            WHERE k.ID IN d.关键词
            CREATE (d)-[:HAS_KEYWORD]->(k)
            """,
            """
            MATCH (d:Document), (c:RAGComponent)
            WHERE d.ID IN c.相关文档列表
            CREATE (d)-[:USES_COMPONENT]->(c)
            """,
            """
            MATCH (d:Document), (f:RAGFlow)
            WHERE d.ID IN f.相关文档列表
            CREATE (d)-[:IMPLEMENTS_FLOW]->(f)
            """,
            """
            MATCH (f:RAGFlow), (c:RAGComponent)
            WHERE c.ID IN [f.索引构建组件ID, f.检索组件ID, f.重排组件ID, f.生成组件ID]
            CREATE (f)-[:INCLUDES_COMPONENT]->(c)
            """
        ]
        for relationship in relationships:
            self.neo4j.execute_query(relationship)

    def build_graph(self, data: Dict[str, List[Dict[str, Any]]]):
        self.create_constraints()
        self.create_document_nodes(data["documents"])
        self.create_author_nodes(data["authors"])
        self.create_keyword_nodes(data["keywords"])
        self.create_rag_component_nodes(data["rag_components"])
        self.create_rag_flow_nodes(data["rag_flows"])
        self.create_relationships()

def main():
    try:
        # 配置Neo4j连接信息
        NEO4J_URI = "bolt://localhost:7687"
        NEO4J_USER = "neo4j"
        NEO4J_PASSWORD = "12345678"

        # 加载数据
        data_loader = DataLoader()
        raw_data = data_loader.load_json_files("kg_data")

        # 处理数据
        data_processor = DataProcessor()
        processed_data = data_processor.process_data(raw_data)

        # 创建Neo4j连接
        neo4j_connection = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

        # 构建知识图谱
        graph_builder = KnowledgeGraphBuilder(neo4j_connection)
        graph_builder.build_graph(processed_data)

        # 关闭Neo4j连接
        neo4j_connection.close()

        print("Knowledge graph construction completed successfully.")
    except Exception as e:
        print(f"An error occurred during knowledge graph construction: {e}")
    finally:
        if 'neo4j_connection' in locals():
            neo4j_connection.close()

if __name__ == "__main__":
    main()