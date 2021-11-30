export FLASK_APP = csvbase.app
export FLASK_ENV = development

.venv: .venv/touchfile

.venv/touchfile: requirements.txt
	test -d .venv || virtualenv .venv --python=python3
	. .venv/bin/activate; pip install -r requirements.txt
	touch $@

serve: .venv
	python3 -m webbrowser -t 'http://localhost:6001'
	. .venv/bin/activate; flask run -p 6001
