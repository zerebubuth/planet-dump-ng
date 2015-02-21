SUBDIRS=src

TESTS = \
	test/planet.xml.case \
	test/history.xml.case \
	test/planet.pbf.case \
	test/history.pbf.case \
	test/changesets.xml.case \
	test/discussions.xml.case
TEST_EXTENSIONS = .case
CASE_LOG_COMPILER = test/test-case-runner.sh

fmt: script/fmt.sh script/emacs-format-file.el
	@for file in `find -name "*.[ch]pp"`; do \
	  script/fmt.sh $$file; \
	done

