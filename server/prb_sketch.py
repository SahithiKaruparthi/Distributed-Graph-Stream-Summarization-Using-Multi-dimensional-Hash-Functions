import numpy as np
import hashlib
import sys

class PRBSketch:
    """
    An implementation of a graph sketch inspired by the PRB-Sketch paper,
    using a Rank-based mechanism for edge weight estimation and an
    integrated Disjoint Set Union (DSU) for connectivity queries.
    """
    def __init__(self, width, depth, conflict_limit=3):
        # --- Parameters for the Sketch ---
        self.width = width
        self.depth = depth
        self.conflict_limit = conflict_limit
        self.max_rank = np.iinfo(np.uint32).max
        self.gM = np.zeros((depth, width, width), dtype=[
            ('rank', 'u4'), 
            ('weight', 'f4'), 
            ('list', 'O')
        ])
        self.gM['rank'] = self.max_rank
        for i in range(depth):
            for j in range(width):
                for k in range(width):
                    self.gM[i, j, k]['list'] = []
        
        # --- NEW: Initialize DSU Data Structures ---
        self.dsu_parent = {}
        self.dsu_rank = {}

    # --- NEW: DSU 'find' operation with path compression ---
    def _dsu_find(self, i):
        # If node is new, it becomes its own parent
        if i not in self.dsu_parent:
            self.dsu_parent[i] = i
            self.dsu_rank[i] = 0
            return i
        
        # Path compression
        if self.dsu_parent[i] == i:
            return i
        self.dsu_parent[i] = self._dsu_find(self.dsu_parent[i])
        return self.dsu_parent[i]

    # --- NEW: DSU 'union' operation by rank ---
    def _dsu_union(self, i, j):
        root_i = self._dsu_find(i)
        root_j = self._dsu_find(j)
        
        if root_i != root_j:
            if self.dsu_rank[root_i] < self.dsu_rank[root_j]:
                self.dsu_parent[root_i] = root_j
            elif self.dsu_rank[root_i] > self.dsu_rank[root_j]:
                self.dsu_parent[root_j] = root_i
            else:
                self.dsu_parent[root_j] = root_i
                self.dsu_rank[root_i] += 1

    def _get_hashes_and_rank(self, source, dest):
        edge_str = f"{source}-{dest}"
        rank_hash = hashlib.md5(edge_str.encode()).hexdigest()
        rank = int(rank_hash, 16) % self.max_rank

        coords = []
        for i in range(self.depth):
            seed_s = f"{source}{i}"
            seed_d = f"{dest}{i}"
            hash_s = int(hashlib.md5(seed_s.encode()).hexdigest(), 16) % self.width
            hash_d = int(hashlib.md5(seed_d.encode()).hexdigest(), 16) % self.width
            coords.append((hash_s, hash_d))
            
        return coords, rank

    def update(self, edges):
        for row in edges:
            source = str(row.source)
            dest = str(row.dest)
            weight = float(row.weight)
            
            # --- MODIFIED: Update DSU with every edge ---
            self._dsu_union(source, dest)
            
            # --- Unchanged sketch update logic ---
            coords, new_rank = self._get_hashes_and_rank(source, dest)
            edge_tuple = (source, dest)

            for i in range(self.depth):
                x, y = coords[i]
                cell = self.gM[i, x, y]
                cell_rank = cell['rank']

                if new_rank < cell_rank:
                    cell['rank'] = new_rank
                    cell['weight'] = weight
                    cell['list'] = [edge_tuple]
                elif new_rank == cell_rank:
                    if edge_tuple not in cell['list']:
                         if len(cell['list']) < self.conflict_limit:
                            cell['weight'] += weight
                            cell['list'].append(edge_tuple)
                    else:
                        cell['weight'] += weight

    def edge_query(self, source, dest):
        source, dest = str(source), str(dest)
        coords, query_rank = self._get_hashes_and_rank(source, dest)
        min_weight = float('inf')
        found = False
        edge_tuple = (source, dest)

        for i in range(self.depth):
            x, y = coords[i]
            cell = self.gM[i, x, y]

            if cell['rank'] == query_rank and edge_tuple in cell['list']:
                estimated_weight = cell['weight'] / len(cell['list']) if len(cell['list']) > 0 else 0
                min_weight = min(min_weight, estimated_weight)
                found = True
        
        return min_weight if found else 0.0

    def reachability_query(self, source, dest):
        # --- REPLACED: Query path is now the DSU ---
        # This now checks for ANY path, not just a direct edge.
        source, dest = str(source), str(dest)
        
        # If nodes have never been seen, they can't be connected.
        if source not in self.dsu_parent or dest not in self.dsu_parent:
            return False
            
        return self._dsu_find(source) == self._dsu_find(dest)

    def get_stats(self):
        occupied_cells = np.sum(self.gM['rank'] != self.max_rank)
        total_cells = self.depth * self.width * self.width
        total_edges = sum(len(cell['list']) for cell in self.gM.flatten() if cell['list'])
        
        return {
            'hash_functions': self.depth,
            'total_edges': total_edges,
            'total_weight': np.sum(self.gM['weight']),
            'occupied_cells': occupied_cells,
            'total_cells': total_cells,
            'occupancy_rate': occupied_cells / total_cells if total_cells > 0 else 0
        }