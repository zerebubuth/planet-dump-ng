;;; File: emacs-format-file
;;; Original author:
;;; Stan Warford
;;; 17 May 2006
;;; Adapted from: http://www.cslab.pepperdine.edu/warford/BatchIndentationEmacs.html

(c-add-style "mystyle"
	     '((fill-column . 80)
	       (c++-indent-level . 2)
	       (c-basic-offset . 2)
	       (indent-tabs-mode . nil)
				 (c-hanging-colons-alist . ((case-label)
																		(label after)
																		(access-label after)
																		(member-init-intro before)
																		(inher-intro)))
	       (c-offsets-alist . ((statement-block-intro . +)
														 (substatement-open . 0)
														 (substatement-label . 0)
														 (label . 0)
														 (statement-cont . +)
														 (innamespace 0)
														 (member-init-intro . +)
														 (inher-intro . +)))))

(setq default-tab-width 2)

(defun emacs-format-function ()
   "Format the whole buffer."
   (c-set-style "mystyle")
   (indent-region (point-min) (point-max) nil)
   (untabify (point-min) (point-max))
   (save-buffer)
)