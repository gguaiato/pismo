export PYTHONPATH=$(PWD)/challenge

SAMPLES_DIR=../sample-data
HDFS_INPUT_EVENTS_FOLDER=hdfs://localhost/input-events
HDFS_OUTPUT_EVENTS_FOLDER=hdfs://localhost/output-events

.PHONY: help
help: ## Shows help menu
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

clean:
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info

flake: clean
	@flake8 --max-line-length=80 --exclude challenge/tests $(PYTHONPATH)

test: flake
	@pytest -vv -rxs challenge/tests

.PHONY: clear-output-data
clear-output-data: ## Clears the output data
	@hdfs dfs -rm -r -f ${HDFS_OUTPUT_EVENTS_FOLDER}

.PHONY: clear-all
clear-all: ## Clears the all data in hdfs
	@hdfs dfs -rm -r -f ${HDFS_INPUT_EVENTS_FOLDER}
	@hdfs dfs -rm -r -f ${HDFS_OUTPUT_EVENTS_FOLDER}

.PHONY: reload-data
reload-data: clear-all ## Reloads data to hdfs
	$(MAKE) -C sample_data_generator load-sample-data-to-hdfs

.PHONY: load-new-data
load-new-data: ## Loads new data to hdfs
	$(MAKE) -C sample_data_generator load-sample-data-to-hdfs

.PHONY: run-local-with-sample
run-local-with-sample: reload-data ## Runs with sample data
	spark-submit challenge/events_job.py ${HDFS_INPUT_EVENTS_FOLDER} ${HDFS_OUTPUT_EVENTS_FOLDER}