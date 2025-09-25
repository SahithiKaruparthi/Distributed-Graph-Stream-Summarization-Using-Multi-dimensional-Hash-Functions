# Distributed Graph Stream Summarization Using Multi-dimensional Hash Functions

This repository contains an implementation for distributed summarization of graph streams using multi-dimensional hash functions. The project addresses the challenge of processing and summarizing large-scale, high-speed graph data streams in a distributed environment, where it is not feasible to store or analyze the entire stream in memory.

## Project Overview

- **Distributed Processing:** The system is designed to work in a distributed setting, allowing multiple nodes to process partitions of the graph stream in parallel. This enables scalability for very large datasets and high-throughput streams.
- **Graph Stream Summarization:** The core objective is to generate compact summaries (sketches) of the input graph stream. These summaries enable approximate answers to queries such as edge frequency, heavy hitters, or neighborhood sizes.
- **Multi-dimensional Hash Functions:** The project leverages multi-dimensional (or multiple) hash functions to map graph elements (such as edges or node pairs) into a lower-dimensional space. This reduces memory requirements while maintaining theoretical guarantees on estimation accuracy.

## Key Concepts

### Graph Streams

A graph stream is a sequence of edges (or updates to edges) arriving over time. Due to the size and speed of such streams, it is impractical to store the entire graph.

### Sketching and Hashing

The project uses sketching techniques such as the Count-Min Sketch or related data structures, which use multiple hash functions to probabilistically estimate the frequency of elements in the stream. For a graph, each edge (or node pair) is mapped via hash functions to indices in a sketch matrix.


### Distributed Aggregation

Each worker node maintains its own local sketch. Periodically, sketches can be merged or aggregated to form a global summary, enabling distributed analytics and fault tolerance.

## Features

- Efficient, memory-bounded summarization of graph streams.
- Support for parallel and distributed data processing.
- Approximate query answering with provable accuracy bounds.
- Modular design for experimenting with different sketching or hashing strategies.

## Getting Started

### Prerequisites

- Python 3.x
- Install required dependencies with `pip install -r requirements.txt`

### Usage

1. Clone the repository:
    ```bash
    git clone https://github.com/SahithiKaruparthi/Distributed-Graph-Stream-Summarization-Using-Multi-dimensional-Hash-Functions.git
    cd Distributed-Graph-Stream-Summarization-Using-Multi-dimensional-Hash-Functions
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Run the main script (example; adapt as needed):
    ```bash
    python main.py --input data/graph_stream.txt --workers 4
    ```

## Applications

- Real-time network monitoring
- Social network analysis
- Fraud detection in transactional graphs
- Large-scale log analytics


---

Feel free to open issues or pull requests for improvements and contributions!
