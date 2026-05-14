"""
Cypher executor — currently unused with Cassandra backend.
Cassandra triples use SPARQL, not Cypher. This module is retained
for compatibility where imports reference it, but no graph DB
executors are active.
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from .cypher_generator import CypherQuery

logger = logging.getLogger(__name__)


@dataclass
class CypherResult:
    """Result from Cypher query execution."""

    records: List[Dict[str, Any]]
    summary: Dict[str, Any]
    execution_time: float
    database_type: str
    query_plan: Optional[Dict[str, Any]] = None


class CypherExecutor:
    """No-op Cypher executor — all Cypher graph DBs have been removed.

    Only Cassandra is used for triples storage, which uses SPARQL.
    This class exists solely for import compatibility.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        logger.warning(
            "CypherExecutor initialized but no graph DB executors are available. "
            "Cassandra uses SPARQL for triples queries."
        )

    async def initialize(self):
        pass

    async def execute_cypher(
        self, cypher_query: CypherQuery, database_type: str
    ) -> CypherResult:
        raise RuntimeError(
            f"Cypher queries are not supported for '{database_type}'. "
            "Cassandra triples use SPARQL. This backend has been removed."
        )

    async def execute_query(self, query: str, database_type: str) -> List[CypherResult]:
        raise RuntimeError(
            f"Cypher queries are not supported for '{database_type}'. "
            "Cassandra triples use SPARQL. This backend has been removed."
        )

    async def close(self):
        pass
