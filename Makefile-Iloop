.PHONY: start qa test flake8 isort isort-save license stop clean logs test-travis setup network

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))


#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install and start the model service.
start:
	docker network inspect iloop || docker network create iloop
	docker-compose up -d --build

## Run all initialization targets.
setup: network

## Create the docker bridge network if necessary.
network:
	docker network inspect DD-DeCaF >/dev/null 2>&1 || \
		docker network create DD-DeCaF


## Run all QA targets
qa: test flake8 isort license

## Run the tests
test: start
	@echo "**********************************************************************"
	@echo "* Running tests."
	@echo "**********************************************************************"
	docker-compose exec web /bin/bash -c "py.test -vxs --cov=./iloop_to_model tests/"

## Run flake8
flake8:
	docker-compose run --rm web flake8 iloop_to_model tests

## Check import sorting
isort:
	docker-compose run --rm web isort --check-only --recursive iloop_to_model tests

## Sort imports and write changes to files
isort-save:
	docker-compose run --rm web isort --recursive iloop_to_model tests

## Verify license headers in source files
license:
	./scripts/verify_license_headers.sh iloop_to_model tests

## Shut down the Docker containers.
stop:
	docker-compose stop

## Remove all containers.
clean:
	docker-compose down

## Run the tests and report coverage (see https://docs.codecov.io/docs/testing-with-docker).
test-travis:
	$(eval ci_env=$(shell bash <(curl -s https://codecov.io/env)))
	docker-compose run --rm $(ci_env) web \
		/bin/sh -c "pytest -s --cov=src/model tests && codecov"

## Read the logs.
logs:
	docker-compose logs --tail="all" -f

#################################################################################
# PROJECT RULES                                                                 #
#################################################################################



#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := show-help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: show-help
show-help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
