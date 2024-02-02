from flask import Flask, request
from flask_cors import CORS
import json
from questionsHandler import GraphHandler
from decouple import config


URI = config("URI")
PASSWORD = "passw0rd"
USER = "neo4j"

neo4j = Flask(__name__)
graphBuilder = GraphHandler(uri=URI, user=USER, password=PASSWORD)
CORS(app=neo4j)


@neo4j.route("/insert-question", methods=["POST"])
def add_question():
    request_data = request.get_json()
    question_id = request_data.get("id")
    response = graphBuilder.create_node(id=question_id)
    return json.dumps({"message": response})


@neo4j.route("/delete-question/<id>", methods=["DELETE"])
def delete_question(id: str):
    response = graphBuilder.delete_node(id=id)
    return json.dumps({"message": response})


@neo4j.route("/find-next-question/<id>/<choice>", methods=["GET"])
def find_next_question(id: str, choice):
    response = graphBuilder.search_next_node(id=id, choice=choice)
    return json.dumps({"next_node": response})


@neo4j.route("/create-edge", methods=["POST"])
def create_edge_question():
    request_data = request.get_json()
    source_id = request_data.get("source")
    target_id = request_data.get("target")
    choice = request_data.get("choice")
    edgeId = request_data.get("edgeId")
    response = graphBuilder.create_edge_nodes(
        source=source_id, target=target_id, choice=choice, edgeId=edgeId
    )
    return json.dumps({"message": response})


@neo4j.route("/update-edge", methods=["POST"])
def update_edge_question():
    request_data = request.get_json()
    choice = request_data.get("choice")
    edgeId = request_data.get("edgeId")
    response = graphBuilder.update_edge(
        edgeId=edgeId,
        choice=choice,
    )

    return f"Updated Edge {edgeId}"


@neo4j.route("/delete-edge/<edgeId>", methods=["DELETE"])
def delete_node_edge(edgeId: str):
    response = graphBuilder.delete_edge_node(edgeId=edgeId)
    return json.dumps({"message": response})


if __name__ == "__main__":
    neo4j.run(debug=True)
