from typing import List

from multimethod import multimethod

from graph import Graph
from node import Node


class PipelineRunner:
    """Handles the data processing pipeline2.

    We add nodes and connection information to the app before we exec app.run().
    """
    def __init__(self):
        self.graph = Graph()
        self.nodes = {}
        self.outputs = {}

    def register_node(self, node: Node):
        self.nodes[node.id] = node
        self.graph.add_node(node)

    def register_nodes(self, nodes: List):
        for node in nodes:
            self.register_node(node)

    def connect_source(self, source: Node, destination: Node):
        # print(source, destination)
        if source.output_type != destination.input_type:
            raise ValueError(f"For {source} and {destination}\n\tOutput type {source.output_type} does not match input"
                             f" type {destination.input_type}.")
        self.graph.connect(source, destination)

    @multimethod
    def connect_sources(self, sources: List, destination: Node):
        for source in sources:
            self.connect_source(source, destination)

    def run(self):
        """Run the data processing pipeline2.

        We return here on the off chance someone is using a 'pure' or mixed
        pipeline2.
        """
        for head in self.graph.heads:
            self._run_helper(head)

        return self.outputs

    def _run_helper(self, node: Node, _input=None):
        """Recursive helper function for run."""
        if node is None:
            return

        # Exec this node.
        current_output = node.process(_input)

        # Check if this node is an output.
        if node.is_output:
            if node.id not in self.outputs:
                self.outputs[node.id] = []
            self.outputs[node.id].append(current_output)
            # Output is not necessarily terminal, so we don't return here.

        # Now pass its output to its children.
        for next_node in self.graph.nodes[node]:
            # ...and pass it to the next node
            self._run_helper(next_node, current_output)
