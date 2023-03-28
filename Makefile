.PHONY: generate
generate:
	python3 generate.py | jq

.PHONY: aggregate
aggregate:
	python3 generate.py | python3 aggregate.py | jq
