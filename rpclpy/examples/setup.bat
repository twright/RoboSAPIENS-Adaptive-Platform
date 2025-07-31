@echo off
echo Installing dependencies for rpclpy examples...
pip install -r requirements.txt

echo.
echo Setting up Redis with Docker (optional)...
echo If you don't have Redis installed locally, you can use Docker:
echo docker run --name redis-server -p 6379:6379 -d redis

echo.
echo To run the examples:
echo 1. Start Redis server (locally or with Docker)
echo 2. Run: python test_example.py
echo    OR
echo    Run monitor_node.py and sensor_node.py in separate terminals

echo.
echo Press any key to continue...
pause > nul
