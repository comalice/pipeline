from typing import Dict, List

import pygraphviz as pgv

from node import Node


class Graph:
    """A directed graph of nodes.

    There must be one or more input nodes and one or more output nodes. These
    can be any node, but they must be marked as input/output in the graph.
    """
    def __init__(self, cyclic=False):
        self.nodes: Dict[Node, List[Node]] = {}
        self.cyclic = cyclic
        self.heads = []

    def find_heads(self) -> List[Node]:
        """Find the head of the graph. A head node has no inbound edges."""
        heads = list(self.nodes)
        for node, edges in self.nodes.items():
            for edge in edges:
                if edge in heads:
                    heads.remove(edge)
        return heads

    def add_node(self, node: Node):
        self.nodes[node] = []

        self.heads = self.find_heads()

    def connect(self, node1: Node, node2: Node):
        # Check node IO types
        if node1.output_type != node2.input_type:
            raise ValueError(f"Node types do not match: {node1.output_type} != {node2.input_type}")

        # Check if connection already exists
        if node2 in self.nodes[node1]:
            raise ValueError(f"Connection already exists: {node1} -> {node2}")

        # Check for cycles
        if not self.cyclic and self._has_cycle(node1, node2):
            raise ValueError(f"Cycle detected: {node1} -> {node2}")

        # Add nodes if they don't exist
        if node1 not in self.nodes:
            self.add_node(node1)

        if node2 not in self.nodes:
            self.add_node(node2)

        # Add connection
        self.nodes[node1].append(node2)

        # Update heads
        self.heads = self.find_heads()

    def _has_cycle(self, node1: Node, node2: Node) -> bool:
        """Check if adding a connection would create a cycle."""
        visited = set()
        return self._has_cycle_helper(node1, node2, visited)

    def _has_cycle_helper(self, node1: Node, node2: Node, visited: set) -> bool:
        """Recursive helper function for _has_cycle."""
        if node1 == node2:
            return True

        if node1 in visited:
            return False

        visited.add(node1)

        for node in self.nodes[node1]:
            if self._has_cycle_helper(node, node2, visited):
                return True

        return False

    def print_graph(self):
        g = pgv.AGraph(directed=True, cyclic=False)

        for node, edges in self.nodes.items():
            g.add_node(node.id)
            n = g.get_node(node.id)

            if node in self.heads:
                n.attr['color'] = 'green'

            # elif node.is_output:
            #     n.attr['color'] = 'red'

            for edge in edges:
                g.add_edge(node.id, edge.id)

        g.layout('dot')
        g.draw('graph.png')

        import subprocess

        subprocess.run(["xdg-open", "graph.png"])
