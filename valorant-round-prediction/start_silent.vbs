Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "C:\valorant project\valorant-round-prediction"
WshShell.Run "python backend/server.py", 0, False
Set WshShell = Nothing
