PY=python3
run:
	PYTHONPATH=src:. $(PY) -m chargegrid.cli run
run_inc:
	PYTHONPATH=src:. $(PY) -m chargegrid.cli run --incremental
current:
	PYTHONPATH=src:. $(PY) -m chargegrid.cli current
test:
	PYTHONPATH=src:. $(PY) -m pytest -q
mock:
	PYTHONPATH=src:. $(PY) -m uvicorn mockapp.main:app --port 8000
clean:
	rm -rf .pytest_cache **/__pycache__ *.db
