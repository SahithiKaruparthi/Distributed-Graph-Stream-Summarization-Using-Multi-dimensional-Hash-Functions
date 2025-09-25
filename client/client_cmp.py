import socket
import pickle
import threading
import time

# Colors
RESET = "\033[0m"
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
BOLD = "\033[1m"

def receive_results(sock):
    while True:
        try:
            results = pickle.loads(sock.recv(4096))
            print(f"\n{BOLD}{YELLOW}--- PRB-Sketch Results ---{RESET}")
            
            for res in results:
                if res.get('type') == 'stats':
                    # Display sketch statistics
                    stats = res['stats']
                    print(f"{BOLD}{CYAN}=== Sketch Statistics ==={RESET}")
                    print(f"  {MAGENTA}Hash Functions (Depth): {stats['hash_functions']}{RESET}")
                    print(f"  {MAGENTA}Total Edges Stored: {stats['total_edges']}{RESET}")
                    print(f"  {MAGENTA}Total Weight Stored: {stats['total_weight']:.2f}{RESET}")
                    print(f"  {MAGENTA}Occupied Cells: {stats['occupied_cells']}/{stats['total_cells']}{RESET}")
                    print(f"  {MAGENTA}Occupancy Rate: {stats['occupancy_rate']:.2%}{RESET}")
                else:
                    # Display query results
                    src, dest = res['query']
                    edge_weight = res['edge_weight']
                    reachability = res['reachability']
                    print(f"{BOLD}{BLUE}Query: {src} -> {dest}{RESET}")
                    print(f"  {GREEN}Estimated Edge Weight: {edge_weight:.2f}{RESET}")
                    print(f"  {YELLOW}Is Reachable: {reachability}{RESET}")
            
            print(f"{CYAN}Waiting for next update...{RESET}")
        except EOFError:
            print(f"\n{RED}Server closed the connection.{RESET}")
            break
        except Exception as e:
            print(f"{RED}Connection error: {e}{RESET}")
            break

if __name__ == "__main__":
    # --- CONFIGURATION FOR THE SMALL TEST CASE ---
    config = {
        'width': 100,         # Smaller width for the small dataset
        'depth': 3,           # Smaller depth
        'conflict_limit': 2,
        # IMPORTANT: Make sure this path points to where you saved small_test_stream.txt
        'file_path': "/Users/sahithikaruparthi/Desktop/spark/dataset/test.txt",
        'queries': [
            (10, 20),         # Should have a weight of ~3.0
            (30, 40),         # Should have a weight of ~1.0
            (1, 2)            # A non-existent edge, should have weight 0.0
        ],
        'batch_size': 100
    }

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect(('localhost', 9992))
            s.sendall(pickle.dumps(config))

            print(f"{BOLD}{GREEN}[CLIENT] Connected to PRB Sketch Server{RESET}")
            print(f"{YELLOW}Configuration sent successfully{RESET}")
            print(f"{CYAN}Receiving streaming results...{RESET}")

            thread = threading.Thread(target=receive_results, args=(s,), daemon=True)
            thread.start()

            while thread.is_alive():
                time.sleep(1)
        except ConnectionRefusedError:
            print(f"{RED}Connection refused. Is the server running?{RESET}")
        except KeyboardInterrupt:
            print(f"\n{RED}Client shutting down...{RESET}")
        finally:
            s.close()