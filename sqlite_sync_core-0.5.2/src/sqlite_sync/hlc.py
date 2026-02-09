"""
hlc.py - Hybrid Logical Clock Implementation.

HLC provides causal ordering (like Vector Clocks) but correlates with 
physical time and has constant O(1) space complexity regardless 
of the number of nodes.

Reference: "Logical Physical Clocks and Consistent Snapshots in 
            Distributed Databases" (Kulkarni et al., 2014)
"""

import time
import threading
from dataclasses import dataclass
from typing import Self

@dataclass(order=True, frozen=True)
class HLC:
    """
    Hybrid Logical Clock point.
    
    Fields:
    - wall_time: Physical timestamp (ms)
    - counter: Logical counter for events within the same ms
    - node_id: Unique identifier to break ties
    """
    wall_time: int
    counter: int
    node_id: str

    def __repr__(self) -> str:
        return f"{self.wall_time}:{self.counter:04x}:{self.node_id}"

    def pack(self) -> str:
        """Serialize HLC to a string."""
        return f"{self.wall_time}:{self.counter}:{self.node_id}"

    @classmethod
    def unpack(cls, s: str) -> 'HLC':
        """Deserialize HLC from a string."""
        parts = s.split(':')
        return cls(int(parts[0]), int(parts[1]), parts[2])

class HLClock:
    """
    Clock manager for generating and tracking HLC points.
    """
    
    def __init__(self, node_id: str):
        self._node_id = node_id
        self._last_hlc = HLC(0, 0, node_id)
        self._lock = threading.Lock()

    def now(self) -> HLC:
        """
        Generate a new HLC event local to this node.
        """
        with self._lock:
            physical_now = int(time.time() * 1000)
            
            if physical_now > self._last_hlc.wall_time:
                # Wall time has advanced, reset counter
                new_hlc = HLC(physical_now, 0, self._node_id)
            else:
                # Wall time is static or drifted, increment counter
                new_hlc = HLC(self._last_hlc.wall_time, self._last_hlc.counter + 1, self._node_id)
            
            self._last_hlc = new_hlc
            return new_hlc

    def update(self, remote_hlc: HLC) -> HLC:
        """
        Update the local clock state using a remote HLC point (happens-before merge).
        """
        with self._lock:
            physical_now = int(time.time() * 1000)
            
            # Max of (PhysicalNow, LocalWall, RemoteWall)
            new_wall = max(physical_now, self._last_hlc.wall_time, remote_hlc.wall_time)
            
            if new_wall == self._last_hlc.wall_time == remote_hlc.wall_time:
                # All equal, increment counters
                new_counter = max(self._last_hlc.counter, remote_hlc.counter) + 1
            elif new_wall == self._last_hlc.wall_time:
                # Local wall is ahead/equal
                new_counter = self._last_hlc.counter + 1
            elif new_wall == remote_hlc.wall_time:
                # Remote wall is ahead
                new_counter = remote_hlc.counter + 1
            else:
                # Physical now is ahead
                new_counter = 0
                
            self._last_hlc = HLC(new_wall, new_counter, self._node_id)
            return self._last_hlc
