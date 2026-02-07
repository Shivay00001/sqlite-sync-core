"""
http_server.py - Reference HTTP sync server.

A simple Flask-based sync server for testing and development.
Production deployments should use proper WSGI servers.
"""

import json
import time
import logging
from dataclasses import dataclass, asdict
from typing import Any

try:
    from flask import Flask, request, jsonify
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False

logger = logging.getLogger(__name__)


# In-memory storage for demo purposes
# Production should use proper database
_device_clocks: dict[str, dict[str, int]] = {}
_pending_operations: dict[str, list[dict]] = {}  # device_id -> ops for that device


def create_sync_server(host: str = "0.0.0.0", port: int = 8080):
    """
    Create a Flask sync server.
    
    Returns:
        Flask app instance
    """
    if not HAS_FLASK:
        raise ImportError("Flask required: pip install flask")
    
    app = Flask(__name__)
    
    @app.route("/sync/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok", "timestamp": int(time.time())})
    
    @app.route("/sync/handshake", methods=["POST"])
    def handshake():
        """Exchange vector clocks between devices."""
        data = request.json
        device_id = data.get("device_id")
        local_vc = data.get("vector_clock", {})
        
        # Store device's clock
        _device_clocks[device_id] = local_vc
        
        # Return merged clock of all devices
        merged_vc = {}
        for vc in _device_clocks.values():
            for dev, counter in vc.items():
                merged_vc[dev] = max(merged_vc.get(dev, 0), counter)
        
        return jsonify({
            "status": "ok",
            "vector_clock": merged_vc,
            "known_devices": list(_device_clocks.keys())
        })
    
    @app.route("/sync/push", methods=["POST"])
    def push_operations():
        """Receive operations from a device."""
        data = request.json
        source_device = data.get("device_id")
        operations = data.get("operations", [])
        
        # Distribute to other devices
        for device_id in _device_clocks:
            if device_id != source_device:
                if device_id not in _pending_operations:
                    _pending_operations[device_id] = []
                _pending_operations[device_id].extend(operations)
        
        logger.info(f"Received {len(operations)} ops from {source_device}")
        
        return jsonify({
            "status": "ok",
            "accepted_count": len(operations)
        })
    
    @app.route("/sync/pull", methods=["POST"])
    def pull_operations():
        """Send pending operations to a device."""
        data = request.json
        device_id = data.get("device_id")
        
        # Get pending ops for this device
        ops = _pending_operations.pop(device_id, [])
        
        return jsonify({
            "status": "ok",
            "operations": ops,
            "count": len(ops)
        })
    
    @app.route("/sync/register", methods=["POST"])
    def register_device():
        """Register a new device."""
        data = request.json
        device_id = data.get("device_id")
        device_name = data.get("device_name", "Unknown")
        
        _device_clocks[device_id] = {}
        _pending_operations[device_id] = []
        
        return jsonify({
            "status": "ok",
            "device_id": device_id,
            "registered_at": int(time.time())
        })
    
    return app


def run_server(host: str = "0.0.0.0", port: int = 8080, debug: bool = False):
    """Run the sync server."""
    app = create_sync_server(host, port)
    print(f"Starting sync server on http://{host}:{port}")
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_server(debug=True)
