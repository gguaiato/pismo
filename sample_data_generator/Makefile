SAMPLES_DIR=../sample-data
HDFS_INPUT_EVENTS_FOLDER=/input-events

.PHONY: help
help: ## Shows help menu
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: clear-sample-data
clear-sample-data: ## Clears sample data
	@rm -rf ${SAMPLES_DIR}

.PHONY: add-sample-data-to-hdfs
copy-sample-data-to-hdfs: ## Adds the sample data to local hdfs
	@hadoop fs -mkdir -p ${HDFS_INPUT_EVENTS_FOLDER}
	@hadoop fs -put ${SAMPLES_DIR}/* ${HDFS_INPUT_EVENTS_FOLDER}
	@echo 'Sample data copied to hdfs://${HDFS_INPUT_EVENTS_FOLDER}'

.PHONY: create-sample-data
create-sample-data: ## Creates sample data
	@python3 sample_generator.py --event_files 3 --events_per_file 5 --output_dir ${SAMPLES_DIR}
	@echo 'Sample data generated in ${SAMPLES_DIR}'

.PHONY: load-sample-data
load-sample-data-to-hdfs: clear-sample-data create-sample-data copy-sample-data-to-hdfs clear-sample-data ## Loads new sample data to local hdfs
	@echo 'New sample data loaded'
