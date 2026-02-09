import os
import typer
import uvicorn
import logging
from typing import Optional
from pathlib import Path
from rich.console import Console
from rich.table import Table

from sqlite_sync.engine import SyncEngine
from sqlite_sync.transport.http_transport import HTTPTransport
from sqlite_sync.scheduler import SyncScheduler
from sqlite_sync.log.operations import SyncOperation

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
    auth_token: Optional[str] = typer.Option(None, help="Auth token")
):
    """Run synchronization (one-off or background)."""
    engine = SyncEngine(db_path)
    device_id = engine.device_id # Ensure initialized
    
    transport = HTTPTransport(peer_url, device_id, auth_token=auth_token)
    scheduler = SyncScheduler(engine, transport, interval_seconds=interval or 60.0)
    
    if interval:
        console.print(f"Starting background sync with {peer_url} (interval={interval}s)")
        # Run blocking
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
    conflict_id: str = typer.Option(None, "--id", help="Conflict ID to resolve"),
    resolution: str = typer.Option("local", help="Resolution strategy (local/remote)")
):
    """Resolve conflicts."""
    console.print("[yellow]Manual resolution not yet fully implemented via CLI[/yellow]")
    # TODO: Implement manual resolution logic

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

def main():
    app()

if __name__ == "__main__":
    main()
