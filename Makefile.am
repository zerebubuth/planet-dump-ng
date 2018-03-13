SUBDIRS=src

TESTS = \
	test/planet.xml.case \
	test/history.xml.case \
	test/planet.pbf.case \
	test/history.pbf.case \
	test/changesets.xml.case \
	test/changesets-badchar.xml.case \
	test/changesets-empty.xml.case \
	test/discussions.xml.case \
	test/discussions-badchar.xml.case \
	test/discussions-long-comment.xml.case
TEST_EXTENSIONS = .case
CASE_LOG_COMPILER = test/test-case-runner.sh

fmt: script/fmt.sh script/emacs-format-file.el
	@for file in `find -name "*.[ch]pp"`; do \
	  script/fmt.sh $$file; \
	done

