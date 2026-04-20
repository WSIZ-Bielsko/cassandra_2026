from pathlib import Path
from typing import Annotated
from uuid import UUID

import typer

app = typer.Typer(
    help="Upload and download files between local storage and Cassandra.",
    no_args_is_help=True,
    add_completion=False,
)


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
    typer.echo(f"upload: {path}, filename={filename}")


@app.command(no_args_is_help=True)
def download(
    file_id: Annotated[UUID, typer.Argument(help="UUID of the file to download.")],
    destination: Annotated[
        Path,
        typer.Option("--destination", "-d", help="Directory to save the file to."),
    ] = Path("."),
) -> None:
    """Download a file from Cassandra. Saves to current directory by default."""
    typer.echo(f"download: {file_id} -> {destination}")


@app.command(no_args_is_help=True)
def get(
    file_id: Annotated[UUID, typer.Argument(help="UUID of the file to inspect.")],
) -> None:
    """Print metadata of a stored file without downloading its content."""
    typer.echo(f"get: {file_id}")


@app.command("list-by-author", no_args_is_help=True)
def list_by_author(
    author_id: Annotated[
        UUID, typer.Argument(help="UUID of the author whose files to list.")
    ],
) -> None:
    """List all files stored for a given author."""
    typer.echo(f"list-by-author: {author_id}")


if __name__ == "__main__":
    app()
