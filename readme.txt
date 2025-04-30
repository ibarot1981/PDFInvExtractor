> Install Git from https://git-scm.com

> During installation, accept all defaults (especially the one that adds Git to PATH).

> Install python from : https://python.org

> Clone Repo from Git 
	--> Navigate to folder where you want to save the project
	--> from the command prompt in that folder type without quotes : 
		"git clone https://github.com/ibarot1981/PDFInvExtractor.git"
	
> folder PDFInvExtractor will be created with all relevant files inside it.

> navigate inside that folder from the command prompt and create python virtual environment
	--> python -m venv venv

> Activate newly created venv :
	--> .\venv\Scripts\activate

> Install required packages in your venv:
	--> pip install -r requirements.txt

> Edit InvoiceLoader.bat and modify the directory path where the project files are kept

> close all command windows and open a new one and navigate to PDFInvExtractor folder

> Run InvoiceLoader.bat.

> Create windows startup to enable to start this bat file on windows startup.
