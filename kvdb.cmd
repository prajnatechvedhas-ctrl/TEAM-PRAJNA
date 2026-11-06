@echo off
if exist "%~dp0.venv\Scripts\python.exe" (
    "%~dp0.venv\Scripts\python.exe" "%~dp0kvdb.py" %*
) else (
    python "%~dp0kvdb.py" %*
)
