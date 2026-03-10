from __future__ import annotations

from ..__main__ import app


def test_cli_registers_install_completion_command() -> None:
    assert app.name == ("olap",)
    assert "--install-completion" in app._commands
    assert app.generate_completion(shell="zsh").splitlines()[0] == "#compdef olap"
