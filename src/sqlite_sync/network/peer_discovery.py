"""
peer_discovery.py - Peer-to-Peer Discovery

Provides:
- mDNS/DNS-SD local network discovery
- Manual peer configuration
- Peer health monitoring
- Automatic failover
"""

import socket
import struct
import threading
import time
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Callable, Optional
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# Data Types
# =============================================================================

class PeerStatus(Enum):
    """Peer connection status."""
    UNKNOWN = "unknown"
    AVAILABLE = "available"
    BUSY = "busy"
    OFFLINE = "offline"
    UNREACHABLE = "unreachable"


@dataclass
class Peer:
    """Discovered peer information."""
    device_id: str
    device_name: str
    host: str
    port: int
    transport: str = "http"
    status: PeerStatus = PeerStatus.UNKNOWN
    last_seen: float = field(default_factory=time.time)
    latency_ms: Optional[float] = None
    metadata: Dict = field(default_factory=dict)
    
    @property
    def url(self) -> str:
        """Get peer URL."""
        return f"{self.transport}://{self.host}:{self.port}"
    
    def is_stale(self, timeout_seconds: float = 60) -> bool:
        """Check if peer hasn't been seen recently."""
        return time.time() - self.last_seen > timeout_seconds


@dataclass
class DiscoveryConfig:
    """Discovery configuration."""
    service_name: str = "_sqlite-sync._tcp.local."
    discovery_port: int = 5353
    announce_interval: float = 30.0
    peer_timeout: float = 90.0
    health_check_interval: float = 15.0
    max_peers: int = 100


# =============================================================================
# Simple UDP Discovery (Cross-Platform)
# =============================================================================

class UDPDiscovery:
    """
    Simple UDP broadcast-based discovery.
    
    Works on local networks without requiring mDNS libraries.
    Uses UDP broadcast for peer announcement and discovery.
    """
    
    MAGIC = b"SQLITE_SYNC_V1"
    BROADCAST_PORT = 8765
    
    def __init__(
        self,
        device_id: str,
        device_name: str,
        sync_port: int,
        transport: str = "http",
        config: DiscoveryConfig = None
    ):
        self._device_id = device_id
        self._device_name = device_name
        self._sync_port = sync_port
        self._transport = transport
        self._config = config or DiscoveryConfig()
        
        self._peers: Dict[str, Peer] = {}
        self._lock = threading.Lock()
        self._running = False
        
        self._listener_thread: Optional[threading.Thread] = None
        self._announcer_thread: Optional[threading.Thread] = None
        self._health_thread: Optional[threading.Thread] = None
        
        self._on_peer_discovered: Optional[Callable[[Peer], None]] = None
        self._on_peer_lost: Optional[Callable[[Peer], None]] = None
    
    def start(self) -> None:
        """Start discovery service."""
        if self._running:
            return
        
        self._running = True
        
        # Start listener
        self._listener_thread = threading.Thread(
            target=self._listen_loop,
            daemon=True
        )
        self._listener_thread.start()
        
        # Start announcer
        self._announcer_thread = threading.Thread(
            target=self._announce_loop,
            daemon=True
        )
        self._announcer_thread.start()
        
        # Start health checker
        self._health_thread = threading.Thread(
            target=self._health_check_loop,
            daemon=True
        )
        self._health_thread.start()
        
        logger.info(f"Discovery started for {self._device_name}")
    
    def stop(self) -> None:
        """Stop discovery service."""
        self._running = False
        
        if self._listener_thread:
            self._listener_thread.join(timeout=2)
        if self._announcer_thread:
            self._announcer_thread.join(timeout=2)
        if self._health_thread:
            self._health_thread.join(timeout=2)
        
        logger.info("Discovery stopped")
    
    def get_peers(self) -> List[Peer]:
        """Get list of discovered peers."""
        with self._lock:
            return list(self._peers.values())
    
    def get_available_peers(self) -> List[Peer]:
        """Get peers that are available for sync."""
        return [
            p for p in self.get_peers()
            if p.status == PeerStatus.AVAILABLE and not p.is_stale(self._config.peer_timeout)
        ]
    
    def add_manual_peer(
        self,
        device_id: str,
        host: str,
        port: int,
        device_name: str = "Manual Peer",
        transport: str = "http"
    ) -> Peer:
        """Manually add a peer."""
        peer = Peer(
            device_id=device_id,
            device_name=device_name,
            host=host,
            port=port,
            transport=transport,
            metadata={"manual": True}
        )
        
        with self._lock:
            self._peers[device_id] = peer
        
        logger.info(f"Added manual peer: {device_name} at {peer.url}")
        return peer
    
    def remove_peer(self, device_id: str) -> bool:
        """Remove a peer."""
        with self._lock:
            if device_id in self._peers:
                peer = self._peers.pop(device_id)
                if self._on_peer_lost:
                    self._on_peer_lost(peer)
                return True
        return False
    
    def on_peer_discovered(self, callback: Callable[[Peer], None]) -> None:
        """Set callback for peer discovery."""
        self._on_peer_discovered = callback
    
    def on_peer_lost(self, callback: Callable[[Peer], None]) -> None:
        """Set callback for peer loss."""
        self._on_peer_lost = callback
    
    def _listen_loop(self) -> None:
        """Listen for peer announcements."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Allow broadcast
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            
            sock.bind(('', self.BROADCAST_PORT))
            sock.settimeout(1.0)
            
            while self._running:
                try:
                    data, addr = sock.recvfrom(1024)
                    self._handle_announcement(data, addr)
                except socket.timeout:
                    continue
                except Exception as e:
                    if self._running:
                        logger.error(f"Listen error: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to start listener: {e}")
        finally:
            sock.close()
    
    def _announce_loop(self) -> None:
        """Periodically announce presence."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            
            while self._running:
                try:
                    self._send_announcement(sock)
                except Exception as e:
                    logger.error(f"Announce error: {e}")
                
                time.sleep(self._config.announce_interval)
                
        except Exception as e:
            logger.error(f"Announcer failed: {e}")
        finally:
            sock.close()
    
    def _health_check_loop(self) -> None:
        """Periodically check peer health."""
        while self._running:
            try:
                self._check_peer_health()
            except Exception as e:
                logger.error(f"Health check error: {e}")
            
            time.sleep(self._config.health_check_interval)
    
    def _send_announcement(self, sock: socket.socket) -> None:
        """Send discovery announcement."""
        message = {
            "device_id": self._device_id,
            "device_name": self._device_name,
            "port": self._sync_port,
            "transport": self._transport,
            "timestamp": time.time()
        }
        
        data = self.MAGIC + json.dumps(message).encode()
        
        # Broadcast to local network
        try:
            sock.sendto(data, ('<broadcast>', self.BROADCAST_PORT))
        except Exception:
            # Try specific broadcast addresses if '<broadcast>' fails
            for addr in ['255.255.255.255', '192.168.1.255', '192.168.0.255', '10.0.0.255']:
                try:
                    sock.sendto(data, (addr, self.BROADCAST_PORT))
                    break
                except Exception:
                    continue
    
    def _handle_announcement(self, data: bytes, addr: tuple) -> None:
        """Handle received announcement."""
        if not data.startswith(self.MAGIC):
            return
        
        try:
            message_data = data[len(self.MAGIC):]
            message = json.loads(message_data.decode())
            
            device_id = message.get("device_id")
            
            # Ignore our own announcements
            if device_id == self._device_id:
                return
            
            host = addr[0]
            
            peer = Peer(
                device_id=device_id,
                device_name=message.get("device_name", "Unknown"),
                host=host,
                port=message.get("port", 8080),
                transport=message.get("transport", "http"),
                status=PeerStatus.AVAILABLE,
                last_seen=time.time()
            )
            
            is_new = False
            
            with self._lock:
                if device_id not in self._peers:
                    is_new = True
                self._peers[device_id] = peer
            
            if is_new:
                logger.info(f"Discovered peer: {peer.device_name} at {peer.url}")
                if self._on_peer_discovered:
                    self._on_peer_discovered(peer)
                    
        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.debug(f"Failed to parse announcement: {e}")
    
    def _check_peer_health(self) -> None:
        """Check health of known peers."""
        stale_peers = []
        
        with self._lock:
            peers_copy = list(self._peers.items())
        
        for device_id, peer in peers_copy:
            # Skip manual peers
            if peer.metadata.get("manual"):
                continue
            
            if peer.is_stale(self._config.peer_timeout):
                stale_peers.append(device_id)
        
        # Remove stale peers
        for device_id in stale_peers:
            with self._lock:
                if device_id in self._peers:
                    peer = self._peers.pop(device_id)
                    peer.status = PeerStatus.OFFLINE
                    logger.info(f"Peer lost: {peer.device_name}")
                    if self._on_peer_lost:
                        self._on_peer_lost(peer)


# =============================================================================
# Peer Manager
# =============================================================================

class PeerManager:
    """
    Manages peer connections with automatic failover.
    
    Provides:
    - Peer selection based on latency/availability
    - Automatic failover on connection failure
    - Load balancing across multiple peers
    """
    
    def __init__(
        self,
        discovery: UDPDiscovery = None,
        prefer_lowest_latency: bool = True
    ):
        self._discovery = discovery
        self._prefer_lowest_latency = prefer_lowest_latency
        self._failed_peers: Dict[str, float] = {}  # device_id -> failure_time
        self._failure_cooldown = 60.0  # seconds before retrying failed peer
    
    def get_best_peer(self) -> Optional[Peer]:
        """Get the best available peer for sync."""
        available = self.get_available_peers()
        
        if not available:
            return None
        
        # Filter out recently failed peers
        now = time.time()
        available = [
            p for p in available
            if now - self._failed_peers.get(p.device_id, 0) > self._failure_cooldown
        ]
        
        if not available:
            # All peers failed recently, try oldest failure
            return min(available, key=lambda p: self._failed_peers.get(p.device_id, 0))
        
        # Sort by criteria
        if self._prefer_lowest_latency:
            # Prefer peers with known low latency
            with_latency = [p for p in available if p.latency_ms is not None]
            if with_latency:
                return min(with_latency, key=lambda p: p.latency_ms)
        
        # Default to most recently seen
        return max(available, key=lambda p: p.last_seen)
    
    def get_available_peers(self) -> List[Peer]:
        """Get all available peers."""
        if self._discovery:
            return self._discovery.get_available_peers()
        return []
    
    def mark_peer_failed(self, peer: Peer) -> None:
        """Mark a peer as failed."""
        self._failed_peers[peer.device_id] = time.time()
        peer.status = PeerStatus.UNREACHABLE
        logger.warning(f"Peer marked as failed: {peer.device_name}")
    
    def mark_peer_success(self, peer: Peer, latency_ms: float = None) -> None:
        """Mark a peer as successfully connected."""
        if peer.device_id in self._failed_peers:
            del self._failed_peers[peer.device_id]
        
        peer.status = PeerStatus.AVAILABLE
        peer.last_seen = time.time()
        
        if latency_ms is not None:
            peer.latency_ms = latency_ms
    
    def ping_peer(self, peer: Peer, timeout: float = 5.0) -> Optional[float]:
        """
        Ping a peer to check availability and measure latency.
        
        Returns latency in ms or None if unreachable.
        """
        import urllib.request
        import urllib.error
        
        url = f"{peer.url}/sync/health"
        start = time.perf_counter()
        
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as response:
                if response.status == 200:
                    latency_ms = (time.perf_counter() - start) * 1000
                    self.mark_peer_success(peer, latency_ms)
                    return latency_ms
        except Exception as e:
            logger.debug(f"Ping failed for {peer.device_name}: {e}")
            self.mark_peer_failed(peer)
        
        return None
    
    def ping_all_peers(self) -> Dict[str, Optional[float]]:
        """Ping all peers and return latencies."""
        results = {}
        
        for peer in self.get_available_peers():
            results[peer.device_id] = self.ping_peer(peer)
        
        return results


# =============================================================================
# Discovery Factory
# =============================================================================

def create_discovery(
    device_id: str,
    device_name: str,
    sync_port: int,
    transport: str = "http",
    config: DiscoveryConfig = None
) -> UDPDiscovery:
    """
    Create and configure a discovery service.
    
    Args:
        device_id: Unique device identifier
        device_name: Human-readable device name
        sync_port: Port the sync server is running on
        transport: Transport protocol (http, https, ws, wss)
        config: Discovery configuration
        
    Returns:
        Configured UDPDiscovery instance
    """
    return UDPDiscovery(
        device_id=device_id,
        device_name=device_name,
        sync_port=sync_port,
        transport=transport,
        config=config
    )
