import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated
from uuid import UUID, uuid4

import typer
from dotenv import load_dotenv

from cassandra_2026.systems.filez.common import get_cluster_session

load_dotenv()
from cassandra_2026.systems.filez.model import StoredFile
from cassandra_2026.systems.filez.repo import FileRepository

app = typer.Typer(
    help="Upload and download files between local storage and Cassandra.",
    no_args_is_help=True,
    add_completion=False,
)


def _author_id_from_env() -> UUID:
    return UUID(os.getenv("FILEZ_AUTHOR_ID", "11111111-1111-1111-1111-111111111111"))


@app.command(no_args_is_help=True)
def upload(
    path: Annotated[Path, typer.Argument(help="Path to the local file to upload.")],
    filename: Annotated[
        str | None,
        typer.Option(
            "--filename", "-n", help="Override the filename stored in Cassandra."
        ),
    ] = None,
) -> None:
    """Upload a file to Cassandra."""
    if not path.exists() or not path.is_file():
        typer.echo(f"File not found: {path}", err=True)
        raise typer.Exit(code=1)

    stored_file = StoredFile(
        file_id=uuid4(),
        author_id=_author_id_from_env(),
        filename=filename or path.name,
        created_at=datetime.now(UTC),
        content=path.read_bytes(),
    )

    cluster, session = get_cluster_session()
    try:
        repo = FileRepository(session)
        asyncio.run(repo.insert_file(stored_file))
        typer.echo(f"Uploaded: {stored_file.filename}")
        typer.echo(f"file_id:   {stored_file.file_id}")
        typer.echo(f"author_id: {stored_file.author_id}")
    finally:
        cluster.shutdown()


@app.command(no_args_is_help=True)
def download(
    file_id: Annotated[UUID, typer.Argument(help="UUID of the file to download.")],
    destination: Annotated[
        Path,
        typer.Option("--destination", "-d", help="Directory to save the file to."),
    ] = Path("."),
) -> None:
    """Download a file from Cassandra. Saves to current directory by default."""
    cluster, session = get_cluster_session()
    try:
        repo = FileRepository(session)
        stored_file = asyncio.run(repo.get_file_by_id(file_id))
        if stored_file is None:
            typer.echo(f"File not found: {file_id}", err=True)
            raise typer.Exit(code=1)
        destination.mkdir(parents=True, exist_ok=True)
        output_path = destination / stored_file.filename
        output_path.write_bytes(stored_file.content)
        typer.echo(f"Saved: {output_path}")
    finally:
        cluster.shutdown()


@app.command(no_args_is_help=True)
def get(
    file_id: Annotated[UUID, typer.Argument(help="UUID of the file to inspect.")],
    with_content: Annotated[
        bool,
        typer.Option("--with-content", help="Also fetch file content and show size."),
    ] = False,
) -> None:
    """Print metadata of a stored file without downloading its content."""
    cluster, session = get_cluster_session()
    try:
        repo = FileRepository(session)
        if with_content:
            stored_file = asyncio.run(repo.get_file_by_id(file_id))
        else:
            stored_file = asyncio.run(repo.get_file_meta_by_id(file_id))
        if stored_file is None:
            typer.echo(f"File not found: {file_id}", err=True)
            raise typer.Exit(code=1)
        typer.echo(f"file_id:    {stored_file.file_id}")
        typer.echo(f"author_id:  {stored_file.author_id}")
        typer.echo(f"filename:   {stored_file.filename}")
        typer.echo(f"created_at: {stored_file.created_at.isoformat()}")
        if stored_file.content is not None:
            typer.echo(f"size:       {len(stored_file.content)} bytes")
    finally:
        cluster.shutdown()


@app.command("list-by-author")
def list_by_author() -> None:
    """List all files stored for the author set in FILEZ_AUTHOR_ID."""
    cluster, session = get_cluster_session()
    try:
        repo = FileRepository(session)
        files = asyncio.run(repo.get_files_by_author(_author_id_from_env()))
        if not files:
            typer.echo("No files found.")
            return
        for f in files:
            typer.echo(f"- {f.filename}")
            typer.echo(f"   file_id:    {f.file_id}")
            typer.echo(f"   created_at: {f.created_at.isoformat()}")
    finally:
        cluster.shutdown()


@app.command(no_args_is_help=True)
def delete(
    file_id: Annotated[UUID, typer.Argument(help="UUID of the file to delete.")],
) -> None:
    """Delete a file from Cassandra by its ID."""
    cluster, session = get_cluster_session()
    try:
        repo = FileRepository(session)
        asyncio.run(repo.delete_file_by_id(file_id))
        typer.echo(f"Deleted: {file_id}")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    app()
