<!--docs\quickstart-venv-vscode-powershell.md-->
QUICKSTART 


Using the Virtual Environment in VS Code terminal with PowerShell 
======================================================

This guide explains how to get started with Python virtual environments in the VS Code terminal with PowerShell. 


Activate an existing virtual environment 
---------------------------------------- 

```powershell
    .venv\Scripts\Activate.ps1
``` 

- The command line in the terminal will now begin with `(.venv)` 


Create a new Virtual Environment
----------------------------------- 

```powershell 
    python -m venv .venv 
``` 


Install Packages in a Virtal Environment 
---------------------------------------- 

- Verify that the command line in the terminal begins with `(.venv)`

```powershell 
    pip install python-dotenv 
```


Save Dependencies to Requirements File 
--------------------------------------  

```powershell 
    pip freeze > requirements.txt
```



