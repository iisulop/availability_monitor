.PHONY: activate-env create-env lint test

lint:
	cd monitor && make $@
	cd listener && make $@

test:
	cd monitor && make $@
	cd listener && make $@

install:
	cd monitor && make $@
	cd listener && make $@

sdist:
	cd monitor && make $@
	cd listener && make $@
