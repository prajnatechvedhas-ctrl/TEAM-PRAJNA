@echo off
if exist "%~dp0.venv\Scripts\python.exe" (
    "%~dp0.venv\Scripts\python.exe" "%~dp0vedasbase.py" %*
) else (
    python "%~dp0vedasbase.py" %*
)
