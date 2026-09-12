"""
End-to-End Live Demonstration Runner for Kafka Avro Order Processing System.
Automates topic setup, producer generation with fault injection, and aggregation display.
"""
import subprocess
import sys
import time
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

console = Console()
ROOT_DIR = Path(__file__).resolve().parent.parent
VENV_PYTHON = ROOT_DIR / ".venv" / "Scripts" / "python.exe"

if not VENV_PYTHON.exists():
    VENV_PYTHON = Path(sys.executable)


def main():
    console.print(
        Panel.fit(
            "[bold cyan]Big Data & Analytics - Take Home Assignment Demo[/bold cyan]\n"
            "[green]Kafka Avro Order Processing System[/green]\n\n"
            "Features Demonstrated:\n"
            "  1. Avro Serialization & Schema Registry\n"
            "  2. Real-Time Price Aggregation (Running Average)\n"
            "  3. Retry Logic with Exponential Backoff for Temporary Failures\n"
            "  4. Dead Letter Queue (DLQ) for Permanently Failed Messages\n"
            "  5. Web Dashboard on Kafka UI (http://localhost:8080)",
            title="System Overview",
            border_style="cyan",
        )
    )

    console.print("[bold yellow]Step 1: Initializing Kafka Topics...[/bold yellow]")
    try:
        subprocess.run([str(VENV_PYTHON), str(ROOT_DIR / "scripts" / "setup_topics.py")], check=True)
    except Exception as e:
        console.print(f"[bold red]Topic setup failed (Ensure Docker Kafka is running): {e}[/bold red]")
        return

    console.print("\n[bold green]Ready for Live Video Demonstration![/bold green]")
    console.print(
        "To perform the live demonstration across 3 terminal windows:\n\n"
        "  [cyan]Terminal 1 (Consumer & Aggregator):[/cyan]\n"
        f"    {VENV_PYTHON} src/consumer.py\n\n"
        "  [cyan]Terminal 2 (DLQ Monitor):[/cyan]\n"
        f"    {VENV_PYTHON} src/dlq_consumer.py\n\n"
        "  [cyan]Terminal 3 (Producer with Fault Injection):[/cyan]\n"
        f"    {VENV_PYTHON} src/producer.py --count 15 --interval 1.0 --fault-rate 0.3\n\n"
        "  [cyan]Browser (Kafka UI Dashboard):[/cyan]\n"
        "    Open http://localhost:8080 to inspect topics, schemas, and live messages.\n"
    )


if __name__ == "__main__":
    main()
