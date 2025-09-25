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
                    stats = res['stats']
                    print(f"{BOLD}{CYAN}=== Sketch Statistics ==={RESET}")
                    print(f"  {MAGENTA}Hash Functions (Depth): {stats['hash_functions']}{RESET}")
                    print(f"  {MAGENTA}Total Edges Stored: {stats['total_edges']}{RESET}")
                    print(f"  {MAGENTA}Occupied Cells: {stats['occupied_cells']}/{stats['total_cells']}{RESET}")
                    print(f"  {MAGENTA}Occupancy Rate: {stats['occupancy_rate']:.2%}{RESET}")
                else:
                    src, dest = res['query']
                    edge_weight = res['edge_weight']
                    print(f"{BOLD}{BLUE}Query: {src} -> {dest}{RESET}")
                    print(f"  {GREEN}Estimated Edge Weight: {edge_weight:.2f}{RESET}")
            
            print(f"{CYAN}Waiting for next update...{RESET}")
        except EOFError:
            print(f"\n{RED}Server closed the connection.{RESET}")
            break
        except Exception as e:
            print(f"{RED}Connection error: {e}{RESET}")
            break

if __name__ == "__main__":
    # --- CONFIGURATION FOR THE CONSTRAINED TEST CASE ---
    config = {
        'width': 5,           # EXTREMELY small width to force collisions
        'depth': 2,
        'conflict_limit': 1,  # No overflow capacity in cells
        'file_path': "/Users/sahithikaruparthi/Desktop/spark/dataset/test_essential.txt",
        'queries': [
            (100, 101),       # The "heavy hitter" edge
            (0, 1)            # One of the initial "chaff" edges
        ],
        'batch_size': 100
    }

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect(('localhost', 9992))
            s.sendall(pickle.dumps(config))
            print(f"{BOLD}{GREEN}[CLIENT] Connected to PRB Sketch Server with constrained config.{RESET}")
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