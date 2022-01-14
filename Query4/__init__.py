import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func

"""
Neo4j :
Requêtez la base pour aider à déterminer si il y a une correlation entre le rating d'un film et son genre
"""

def main(req: func.HttpRequest) -> func.HttpResponse:

    neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
    neo4j_user = os.environ["TPBDD_NEO4J_USER"]
    neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

    if len(neo4j_server)==0 or len(neo4j_user)==0 or len(neo4j_password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)

    errorMessage = ""
    dataString = ""

    try:
        logging.info("Test de connexion avec py2neo...")
        graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))
        producers = graph.run("MATCH (movie:Title)-[r:IS_OF_GENRE]->(genre_movie:Genre) WITH genre_movie, avg(movie.averageRating) as avg_rating RETURN genre_movie.genre, avg_rating")
        for producer in producers:
            dataString += f"CYPHER: Genre={producer['genre_movie.genre']}, average_rating ={producer['avg_rating']\n"
    except:
        errorMessage = "Erreur de connexion a la base Neo4j"

    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString)
