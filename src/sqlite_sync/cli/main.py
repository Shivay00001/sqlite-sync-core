import os
import typer
import uvicorn
import logging
import signal
import json
import time
from typing import Optional
from pathlib import Path
from rich.console import Console
from rich.table import Table

from sqlite_sync.engine import SyncEngine
from sqlite_sync.transport.http_transport import HTTPTransport
from sqlite_sync.scheduler import SyncScheduler
from sqlite_sync.log.operations import SyncOperation
from sqlite_sync.network.peer_discovery import UDPDiscovery, Peer, create_discovery

app = typer.Typer(help="SQLite Sync Core CLI")
console = Console()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cli")

def get_engine(db_path: str) -> SyncEngine:
    if not os.path.exists(db_path) and "init" not in os.sys.argv:
        # Check if init is being called, if not, warn?
        # Actually init command handles creation.
        pass
    return SyncEngine(db_path)

@app.command()
def init(
    db_path: str = typer.Argument(..., help="Path to SQLite database"),
    enable_tables: Optional[list[str]] = typer.Option(None, "--table", "-t", help="Tables to enable sync for")
):
    """Initialize a database for synchronization."""
    engine = SyncEngine(db_path)
    device_id = engine.initialize()
    console.print(f"[green]Initialized sync for {db_path}[/green]")
    console.print(f"Device ID: {device_id.hex()}")
    
    if enable_tables:
        with engine:
            for table in enable_tables:
                engine.enable_sync_for_table(table)
                console.print(f"Enabled sync for table: {table}")

@app.command()
def serve(
    db_path: str = typer.Argument(..., help="Path to SQLite database"),
    host: str = typer.Option("0.0.0.0", help="Host to bind to"),
    port: int = typer.Option(8000, help="Port to bind to"),
    reload: bool = typer.Option(False, help="Enable auto-reload")
):
    """Start the HTTP sync server."""
    os.environ["SQLITE_SYNC_DB_PATH"] = db_path
    console.print(f"[bold green]Starting sync server on http://{host}:{port}[/bold green]")
    uvicorn.run("sqlite_sync.transport.server:app", host=host, port=port, reload=reload)

@app.command()
def sync(
    db_path: str = typer.Argument(..., help="Path to SQLite database"),
    peer_url: str = typer.Argument(..., help="URL of peer to sync with"),
    interval: Optional[float] = typer.Option(None, "--interval", "-i", help="Run in background with interval (seconds)"),
    daemon: bool = typer.Option(False, "--daemon", "-d", help="Run as daemon with graceful shutdown"),
    auth_token: Optional[str] = typer.Option(None, help="Auth token")
):
    """Run synchronization (one-off, interval, or daemon mode)."""
    engine = SyncEngine(db_path)
    device_id = engine.device_id  # Ensure initialized
    
    transport = HTTPTransport(peer_url, device_id, auth_token=auth_token)
    scheduler = SyncScheduler(engine, transport, interval_seconds=interval or 60.0)
    
    if daemon or interval:
        effective_interval = interval or 60.0
        console.print(f"[bold green]Starting sync daemon with {peer_url} (interval={effective_interval}s)[/bold green]")
        
        if daemon:
            # Daemon mode: run in background thread with signal handling
            stop_requested = [False]
            
            def handle_signal(signum, frame):
                console.print("\n[yellow]Shutdown signal received. Stopping gracefully...[/yellow]")
                stop_requested[0] = True
                scheduler.stop()
            
            # Register signal handlers
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)
            
            # Start scheduler in background thread
            scheduler.start(in_background=True)
            console.print("[green]Daemon started. Press Ctrl+C to stop.[/green]")
            
            # Keep main thread alive, waiting for signals
            try:
                while not stop_requested[0]:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
            finally:
                scheduler.stop()
                engine.close()
                console.print("[green]Daemon stopped.[/green]")
        else:
            # Run blocking with interval (original behavior)
            scheduler.start(in_background=False)
    else:
        console.print(f"Syncing with {peer_url}...")
        import asyncio
        try:
            asyncio.run(scheduler.sync_now())
            console.print("[green]Sync completed successfully[/green]")
        except Exception as e:
            console.print(f"[red]Sync failed: {e}[/red]")
            raise typer.Exit(code=1)


@app.command()
def status(db_path: str = typer.Argument(..., help="Path to SQLite database")):
    """Show synchronization status."""
    engine = SyncEngine(db_path)
    
    with engine:
        # Basic Info
        table = Table(title="Sync Status")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="magenta")
        
        table.add_row("Device ID", engine.device_id.hex())
        
        # Vector Clock
        vc = engine.get_vector_clock()
        vc_str = "\n".join([f"{k[:8]}...: {v}" for k, v in vc.items()])
        table.add_row("Vector Clock", vc_str or "Empty")
        
        # Unresolved Conflicts
        conflicts = engine.get_unresolved_conflicts()
        table.add_row("Unresolved Conflicts", str(len(conflicts)))
        
        console.print(table)
        
        if conflicts:
            conflict_table = Table(title="Conflicts")
            conflict_table.add_column("ID")
            conflict_table.add_column("Table")
            conflict_table.add_column("Detected At")
            
            for c in conflicts:
                conflict_table.add_row(
                    c.conflict_id.hex()[:8],
                    c.table_name,
                    str(c.detected_at)
                )
            console.print(conflict_table)

@app.command()
def resolve(
    db_path: str = typer.Argument(..., help="Path to SQLite database"),
    conflict_id: Optional[str] = typer.Option(None, "--id", help="Conflict ID to resolve"),
    resolution: Optional[str] = typer.Option(None, "--strategy", help="Resolution strategy (local/remote)")
):
    """Resolve conflicts manually."""
    engine = SyncEngine(db_path)
    
    with engine:
        conflicts = engine.get_unresolved_conflicts()
        
        if not conflicts:
            console.print("[green]No unresolved conflicts found.[/green]")
            return

        if not conflict_id:
            # Show list if no ID provided
            status(db_path)
            conflict_id = typer.prompt("Enter conflict ID (or short hex) to resolve")
        
        # Find the conflict
        target = None
        for c in conflicts:
            if c.conflict_id.hex().startswith(conflict_id):
                target = c
                break
        
        if not target:
            console.print(f"[red]Conflict {conflict_id} not found.[/red]")
            raise typer.Exit(1)
            
        cid_hex = target.conflict_id.hex()
        
        if not resolution:
            console.print(f"\nResolving conflict in table [bold]{target.table_name}[/bold]")
            console.print(f"Conflict ID: {cid_hex}")
            console.print("1) Keep Local version")
            console.print("2) Accept Remote version")
            choice = typer.prompt("Choose resolution", type=int)
            resolution = "local" if choice == 1 else "remote"
            
        try:
            engine.resolve_conflict(cid_hex, resolution)
            console.print(f"[green]Conflict resolved using {resolution} strategy.[/green]")
        except Exception as e:
            console.print(f"[red]Resolution failed: {e}[/red]")
            raise typer.Exit(1)

@app.command()
def snapshot(
    db_path: str = typer.Argument(..., help="Path to SQLite database"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file")
):
    """Create a snapshot of the database."""
    engine = SyncEngine(db_path)
    with engine:
        snap = engine.create_snapshot()
        console.print(f"[green]Snapshot created: {snap.snapshot_id.hex()}[/green]")
        console.print(f"Rows: {snap.row_count}, Size: {snap.size_bytes} bytes")


# =============================================================================
# Peer Discovery Commands
# =============================================================================

PEERS_CONFIG_FILE = ".sqlite_sync_peers.json"


def load_peers_config() -> dict:
    """Load peers configuration from file."""
    if os.path.exists(PEERS_CONFIG_FILE):
        with open(PEERS_CONFIG_FILE, "r") as f:
            return json.load(f)
    return {"peers": []}


def save_peers_config(config: dict) -> None:
    """Save peers configuration to file."""
    with open(PEERS_CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


@app.command()
def peers(
    discover: bool = typer.Option(False, "--discover", help="Start discovery and show live peers"),
    auto_add: bool = typer.Option(False, "--auto-add", help="Discover peers and add to local config"),
    timeout: float = typer.Option(10.0, "--timeout", "-t", help="Discovery timeout in seconds"),
    port: int = typer.Option(8000, "--port", "-p", help="Local sync server port")
):
    """Discover and manage sync peers on the local network."""
    
    if discover or auto_add:
        # Start peer discovery
        console.print("[bold blue]Starting peer discovery...[/bold blue]")
        
        # Generate a temporary device ID for discovery
        import uuid
        device_id = uuid.uuid4().hex[:16]
        device_name = f"Discovery-{device_id[:8]}"
        
        discovery = create_discovery(
            device_id=device_id,
            device_name=device_name,
            sync_port=port
        )
        
        discovered_peers: list[Peer] = []
        
        def on_peer_discovered(peer: Peer):
            discovered_peers.append(peer)
            console.print(f"  [green]Found:[/green] {peer.device_name} at {peer.url}")
        
        discovery.on_peer_discovered(on_peer_discovered)
        discovery.start()
        
        console.print(f"[dim]Listening for {timeout} seconds...[/dim]")
        time.sleep(timeout)
        discovery.stop()
        
        if not discovered_peers:
            console.print("[yellow]No peers discovered on the local network.[/yellow]")
            return
        
        # Display discovered peers
        table = Table(title="Discovered Peers")
        table.add_column("Name", style="cyan")
        table.add_column("URL", style="magenta")
        table.add_column("Device ID", style="dim")
        
        for peer in discovered_peers:
            table.add_row(peer.device_name, peer.url, peer.device_id[:16] + "...")
        
        console.print(table)
        
        # Auto-add to config if requested
        if auto_add:
            config = load_peers_config()
            existing_ids = {p.get("device_id") for p in config["peers"]}
            
            added = 0
            for peer in discovered_peers:
                if peer.device_id not in existing_ids:
                    config["peers"].append({
                        "device_id": peer.device_id,
                        "device_name": peer.device_name,
                        "url": peer.url,
                        "host": peer.host,
                        "port": peer.port,
                        "transport": peer.transport,
                        "added_at": int(time.time())
                    })
                    added += 1
            
            save_peers_config(config)
            console.print(f"[green]Added {added} new peer(s) to config ({PEERS_CONFIG_FILE})[/green]")
    else:
        # Just show saved peers
        config = load_peers_config()
        
        if not config["peers"]:
            console.print("[yellow]No saved peers. Use --discover or --auto-add to find peers.[/yellow]")
            return
        
        table = Table(title="Saved Peers")
        table.add_column("Name", style="cyan")
        table.add_column("URL", style="magenta")
        table.add_column("Device ID", style="dim")
        
        for peer in config["peers"]:
            table.add_row(
                peer.get("device_name", "Unknown"),
                peer.get("url", ""),
                peer.get("device_id", "")[:16] + "..."
            )
        
        console.print(table)


# =============================================================================
# CLI Consistency - Aliases
# =============================================================================

@app.command()
def start(
    db_path: str = typer.Argument(..., help="Path to SQLite database"),
    host: str = typer.Option("0.0.0.0", help="Host to bind to"),
    port: int = typer.Option(8000, help="Port to bind to"),
    reload: bool = typer.Option(False, help="Enable auto-reload")
):
    """Start the sync server (alias for 'serve')."""
    serve(db_path=db_path, host=host, port=port, reload=reload)


def main():
    app()

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
