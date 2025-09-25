import socket
import threading
import pickle
import time
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, split, trim
from pyspark.sql.types import StructType, StructField, StringType
from prb_sketch import PRBSketch 

class SparkSketchServer:
    def __init__(self, host='localhost', port=9992):
        self.host = host
        self.port = port
        self.clients = []
        self.spark = SparkSession.builder \
            .appName("PRBSketchServer") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        self.sketch = None
        self.running = False
        self.query = None

    def handle_client(self, conn, addr):
        print(f"[SERVER] Connected to {addr}")
        temp_dir = None # <-- CORRECTED: Initialize temp_dir to None
        
        try:
            config = pickle.loads(conn.recv(4096))
            width = config['width']
            depth = config['depth']
            conflict_limit = config['conflict_limit']
            file_path = config['file_path']
            queries = config['queries']
            batch_size = config.get('batch_size', 1000)

            print(f"[SERVER] Config received:")
            print(f"  → Width: {width}")
            print(f"  → Depth: {depth}")
            print(f"  → Conflict Limit: {conflict_limit}")
            print(f"  → File Path: {file_path}")
            print(f"  → Batch Size: {batch_size}")
            print(f"  → Queries: {queries}")

            self.sketch = PRBSketch(
                width=width,
                depth=depth,
                conflict_limit=conflict_limit
            )

            temp_dir = "/tmp/sketch_stream"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir)

            temp_file = os.path.join(temp_dir, "data.txt")
            shutil.copy2(file_path, temp_file)

            schema = StructType([
                StructField("value", StringType(), True)
            ])

            def process_batch(batch_df, batch_id):
                if batch_df.count() == 0:
                    return
                
                cleaned_df = batch_df.filter(~batch_df.value.startswith("#"))
                edges_df = cleaned_df.select(
                    split(trim(cleaned_df.value), "\t").getItem(0).alias("source"),
                    split(trim(cleaned_df.value), "\t").getItem(1).alias("dest")
                ).withColumn("weight", lit(1.0))

                edges_collected = edges_df.collect()
                self.sketch.update(edges_collected)

                stats = self.sketch.get_stats()
                print(f"Processed batch {batch_id} with {len(edges_collected)} edges")
                print(f"  → Hash Functions: {stats['hash_functions']}")
                print(f"  → Total Edges: {stats['total_edges']}")
                print(f"  → Occupancy Rate: {stats['occupancy_rate']:.2%}")

            stream_df = self.spark.readStream \
                .schema(schema) \
                .option("maxFilesPerTrigger", 1) \
                .text(temp_dir)

            self.query = stream_df.writeStream \
                .foreachBatch(process_batch) \
                .start()

            def run_query():
                self.running = True
                while self.running:
                    try:
                        results = []
                        if self.sketch:
                            for (src, dest) in queries:
                                edge_weight = self.sketch.edge_query(src, dest)
                                reachability = self.sketch.reachability_query(src, dest)
                                results.append({
                                    'query': (src, dest),
                                    'edge_weight': edge_weight,
                                    'reachability': reachability
                                })

                            stats = self.sketch.get_stats()
                            results.append({
                                'type': 'stats',
                                'stats': stats
                            })

                            conn.sendall(pickle.dumps(results))
                        time.sleep(2)
                    except (ConnectionResetError, BrokenPipeError):
                        print(f"Client {addr} disconnected.")
                        break
                    except Exception as e:
                        print(f"Query error: {e}")
                        break
                self.running = False


            query_thread = threading.Thread(target=run_query, daemon=True)
            query_thread.start()

            self.query.awaitTermination()

        except Exception as e:
            print(f"Client handling error: {e}")
        finally:
            self.running = False
            if self.query and self.query.isActive:
                self.query.stop()
            # CORRECTED: Check if temp_dir was assigned before trying to use it
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            conn.close()
            print(f"[SERVER] Connection to {addr} closed.")

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"[SERVER] PRB Sketch Server listening on {self.host}:{self.port}")

            while True:
                conn, addr = s.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(conn, addr),
                    daemon=True
                )
                client_thread.start()
                self.clients.append(conn)

    def stop(self):
        print("\n[SERVER] Shutting down PRB Sketch Server...")
        self.running = False
        if self.query and self.query.isActive:
            self.query.stop()
        self.spark.stop()

if __name__ == "__main__":
    server = SparkSketchServer()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()