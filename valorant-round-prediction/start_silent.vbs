Set WshShell = CreateObject("WScript.Shell")

Dim pyPath, serverPath
pyPath = "C:\Users\Someshwar Kumbar\AppData\Local\Programs\Python\Python314\python.exe"
serverPath = "C:\valorant project\valorant-round-prediction\backend\server.py"

Set fso = CreateObject("Scripting.FileSystemObject")
If fso.FileExists(pyPath) And fso.FileExists(serverPath) Then
    WshShell.Run """" & pyPath & """" & " """ & serverPath & """", 0, False
Else
    WshShell.Run "python """ & serverPath & """", 0, False
End If

Set fso = Nothing
Set WshShell = Nothing
