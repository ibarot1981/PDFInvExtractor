@echo off
cd /d D:\Irshad\Dev\Python\InvoiceDataExtractor

:: Check if venv exists
if exist "venv\Scripts\activate.bat" (
    echo 🔵 Activating virtual environment...
    call venv\Scripts\activate.bat
) else (
    echo ⚪ No virtual environment found. Running with system Python.
)

:: Start wrapper
echo 🚀 Starting wrapper.py...
python wrapper.py

pause
