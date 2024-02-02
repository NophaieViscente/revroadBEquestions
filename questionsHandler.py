from neo4j import GraphDatabase


class GraphHandler:
    """
    Handles interactions with a Neo4j graph database.

    Parameters
    ----------
    uri : str
        URI of the Neo4j database.
    user : str
        Username to authenticate in the Neo4j database.
    password : str
        Password to authenticate in the Neo4j database.
    """

    def __init__(self, uri: str, user: str, password: str) -> None:
        """
        Initializes the class with the uri, user and password to connect to the Neo4j database.

        Parameters
        ----------
        uri : str
            URI of the Neo4j database.
        user : str
            Username to authenticate in the Neo4j database.
        password : str
            Password to authenticate in the Neo4j database.
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """
        Closes the connection to the Neo4j database.
        """
        self.driver.close()

    @staticmethod
    def _run_query(tx, query: str):
        """
        Runs a query on the Neo4j database.

        Parameters
        ----------
        tx :
            Session object for the Neo4j database.
        query : str
            Cypher query to be executed in the Neo4j database.

        Returns
        -------
        List of records from the executed query.
        """
        result = tx.run(query)
        return [record for record in result]

    @classmethod
    def __format_query_create_node__(cls, **kwargs) -> str:
        """
        Formats a Cypher query to create a node in the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Cypher query for creating a node.
        """
        id_node = kwargs["id"]

        query = f"MERGE (n:QUESTION {{id_question : '{id_node}'}})"

        return query

    @classmethod
    def __format_query_edge_nodes__(cls, **kwargs) -> str:
        """
        Formats a Cypher query to create an edge between nodes in the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Cypher query for creating an edge between nodes.
        """
        source = kwargs["source"]
        target = kwargs["target"]
        choice = kwargs["choice"]
        edgeId = kwargs["edgeId"]

        print(edgeId)

        if choice != "null" and type(choice) not in [bool, int, float]:
            query = f"""MATCH (n), (m)
            WHERE n.id_question = '{source}' AND m.id_question = '{target}'
            MERGE (n)-[r:NEXT {{edgeId: '{edgeId}', choice : '{choice}'}}]->(m)
            """
        elif choice != "null" and type(choice) in [bool, int, float]:
            query = f"""MATCH (n), (m)
            WHERE n.id_question = '{source}' AND m.id_question = '{target}'
            MERGE (n)-[r:NEXT {{edgeId: '{edgeId}', choice : {choice}}}]->(m)
            """
        else:
            query = f"""MATCH (n), (m)
            WHERE n.id_question = '{source}' AND m.id_question = '{target}'
            MERGE (n)-[r:NEXT {{edgeId: '{edgeId}'}}]->(m)
            """
        return query

    @classmethod
    def __format_query_delete_node__(cls, **kwargs) -> str:
        """
        Formats a Cypher query to delete a node from the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Cypher query for deleting a node.
        """
        id_node = kwargs["id"]

        query = f"""MATCH (n)
        WHERE n.id_question = '{id_node}'
        DETACH DELETE n
        """
        return query

    @classmethod
    def __format_query_delete_all_nodes__(cls) -> str:
        """
        Formats a Cypher query to delete all nodes from the Neo4j database.

        Returns
        -------
        str
            Cypher query for deleting all nodes.
        """
        query = f"""MATCH (n)
        DETACH DELETE n
        """
        return query

    @classmethod
    def __format_query_delete_relationship__(cls, **kwargs) -> str:
        """
        Formats a Cypher query to delete a relationship between nodes in the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Cypher query for deleting a relationship.
        """
        source = kwargs.get("source")
        target = kwargs.get("target")
        edgeId = kwargs.get("edgeId")

        # query = f"""MATCH (n:QUESTION)-[r:NEXT]-(m:QUESTION)
        # WHERE n.id_question = '{source}' AND m.id_question = '{target}'
        # DELETE r
        # """
        query = f"""MATCH (n:QUESTION)-[r:NEXT]-(m:QUESTION)
        WHERE r.edgeId = '{edgeId}'
        DELETE r
        """
        return query

    @classmethod
    def __format_query_search_next_node__(cls, **kwargs) -> str:
        """
        Formats a Cypher query to search for the next node based on given criteria in the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Cypher query for searching the next node.
        """
        id_node = kwargs["id"]
        choice = kwargs["choice"]

        if choice != "null" and type(choice) not in [int, float, bool]:
            query = f"""MATCH (n)-[r:NEXT]->(m)
            WHERE n.id_question = '{id_node}' AND r.choice = '{choice}'
            RETURN m.id_question
            """
        elif choice != "null" and type(choice) in [int, float, bool]:
            query = f"""MATCH (n)-[r:NEXT]->(m)
            WHERE n.id_question = '{id_node}' AND r.choice = {choice}
            RETURN m.id_question
            """
        else:
            query = f"""MATCH (n)-[r:NEXT]->(m)
            WHERE n.id_question = '{id_node}'
            RETURN m.id_question
            """
        return query

    @classmethod
    def __format_query_update_edge__(cls, **kwargs) -> str:
        choice = kwargs["choice"]
        edgeId = kwargs["edgeId"]

        if choice != "null" and type(choice) not in [int, bool, float]:
            query = f"""MATCH (n)-[r:NEXT]->(m)
            WHERE r.edgeId = '{edgeId}'
            SET r.choice = '{choice}'"""
        elif choice == "null":
            query = f"""MATCH (n)-[r:NEXT]->(m)
            WHERE r.edgeId = '{edgeId}'
            REMOVE r.choice"""
        else:
            query = f"""MATCH (n)-[r:NEXT]->(m)
            WHERE r.edgeId = '{edgeId}'
            SET r.choice = {choice}"""

        return query

    def create_node(self, **kwargs):
        """
        Creates a node in the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Confirmation message for the created node.
        """
        with self.driver.session() as session:
            response = session.execute_write(
                self._run_query, self.__format_query_create_node__(**kwargs)
            )
            return f">>> Save Node: {kwargs['id']}"

    def create_edge_nodes(self, **kwargs) -> str:
        """
        Creates an edge between nodes in the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Confirmation message for the created edge.
        """
        with self.driver.session() as session:
            response = session.execute_write(
                self._run_query, self.__format_query_edge_nodes__(**kwargs)
            )

            return f">>> Create Edge Between {kwargs['source']} and {kwargs['target']}"

    def update_edge(self, **kwargs):
        with self.driver.session() as session:
            response = session.execute_write(
                self._run_query, self.__format_query_update_edge__(**kwargs)
            )
            return f">>> Updated Edge: {kwargs['edgeId']}"

    def search_next_node(self, **kwargs) -> str:
        """
        Searches for the next node based on given criteria in the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            ID of the next node.
        """
        with self.driver.session() as session:
            response = session.execute_write(
                self._run_query, self.__format_query_search_next_node__(**kwargs)
            )

            return response[0]["m.id_question"]

    def delete_node(self, **kwargs) -> str:
        """
        Deletes a node from the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Confirmation message for the deleted node.
        """
        with self.driver.session() as session:
            response = session.execute_write(
                self._run_query, self.__format_query_delete_node__(**kwargs)
            )
            return f">>> Deleted node : {kwargs['id']}"

    def delete_edge_node(self, **kwargs) -> str:
        """
        Deletes a relationship between nodes in the Neo4j database.

        Parameters
        ----------
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        str
            Confirmation message for the deleted relationship.
        """
        with self.driver.session() as session:
            response = session.execute_write(
                self._run_query, self.__format_query_delete_relationship__(**kwargs)
            )
            return f">>> Deleted relationship {kwargs['edgeId']}"

    def delete_all_nodes(self) -> str:
        """
        Deletes all nodes from the Neo4j database.

        Returns
        -------
        str
            Confirmation message for deleting all nodes.
        """
        with self.driver.session() as session:
            response = session.execute_write(
                self._run_query, self.__format_query_delete_all_nodes__()
            )
            return f">>> All Nodes Deleted"
