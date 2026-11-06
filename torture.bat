@echo off
if exist "%~dp0.venv\Scripts\python.exe" (
    "%~dp0.venv\Scripts\python.exe" "%~dp0torture.py" %*
) else (
    python "%~dp0torture.py" %*
)

