.PHONY: start logs setup user stop clean wipe

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Install and start the iloop backend.
start:
	docker volume create --name=iloop-upload
	docker-compose up -d --build

## Read the logs.
logs:
	docker-compose logs --tail="all" -f

## Load data from fixtures into database. Only run this once!
setup: start
	@echo "**********************************************************************"
	@echo "* Running database migrations."
	@echo "**********************************************************************"
	docker-compose run --rm web python manage.py db upgrade
	@echo "**********************************************************************"
	@echo "* Loading fixtures content."
	@echo "**********************************************************************"
	docker-compose run --rm web python manage.py fixtures init
	@echo "**********************************************************************"
	@echo "* Congratulations! You can now access the iloop API."
	@echo "**********************************************************************"

## Create a unique superuser with personal access token.
user: start
	@echo "**********************************************************************"
	@echo "* Creating superuser."
	@echo "**********************************************************************"
	docker-compose run --rm web python manage.py user add_developer

## Shut down the Docker containers.
stop:
	docker-compose stop

## Remove all containers.
clean:
	docker-compose down
	@echo "If you really want to remove all data run 'make wipe' instead."

## Remove all containers and data volumes.
wipe:
	docker-compose down
	docker volume rm iloop-upload

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
