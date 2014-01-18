SUBDIRS=src

TESTS=

fmt: script/fmt.sh script/emacs-format-file.el
	@for file in `find -name "*.[ch]pp"`; do \
	  script/fmt.sh $$file; \
	done

