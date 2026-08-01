@echo off
title Valorant Win Predictor Backend
cd /d "C:\valorant project\valorant-round-prediction"
echo ==================================================
echo   Starting Valorant Win Predictor Backend Server
echo ==================================================
if exist "C:\Users\Someshwar Kumbar\AppData\Local\Programs\Python\Python314\python.exe" (
    "C:\Users\Someshwar Kumbar\AppData\Local\Programs\Python\Python314\python.exe" "C:\valorant project\valorant-round-prediction\backend\server.py"
) else (
    python "C:\valorant project\valorant-round-prediction\backend\server.py"
)
pause
