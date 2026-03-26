.PHONY: setup install extract transform test sync

extract:
	. venv/bin/activate && python extraction/off_extractor.py

transform:
	. venv/bin/activate && cd off_dbt_project && dbt run

test:
	. venv/bin/activate && cd off_dbt_project && dbt test

sync:
	. venv/bin/activate && python sync/sync_manager.py
