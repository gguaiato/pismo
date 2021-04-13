SAMPLES_DIR=../sample-data

.PHONY: help
help: ## Show help menu
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: create-sample-data
clear-sample-data: ## Clears sample data
	@rm -rf ${SAMPLES_DIR}

.PHONY: create-sample-data
create-sample-data: ## Creates sample data
	@python3 sample_generator.py --event_files 3 --events_per_file 5 --output_dir ${SAMPLES_DIR}
	@echo 'Sample data generated in ${SAMPLES_DIR}'