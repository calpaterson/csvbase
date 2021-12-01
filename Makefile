export FLASK_APP = csvbase.app
export FLASK_ENV = development

.venv: .venv/touchfile

.venv/touchfile: requirements.txt
	test -d .venv || virtualenv .venv --python=python3
	. .venv/bin/activate; pip install -r requirements.txt
	touch $@

serve: .venv csvbase/static/bootstrap.min.css
	. .venv/bin/activate; flask run -p 6001

bootstrap-5.1.3-dist.zip:
	curl -O -L https://github.com/twbs/bootstrap/releases/download/v5.1.3/bootstrap-5.1.3-dist.zip

csvbase/static/bootstrap.min.css: bootstrap-5.1.3-dist.zip
	unzip -p bootstrap-5.1.3-dist.zip bootstrap-5.1.3-dist/css/bootstrap.min.css > $@
