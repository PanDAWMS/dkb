# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = pyDKB
SOURCEDIR     = source
BUILDDIR      = build
APIDOC        = sphinx-apidoc

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

all: pdf html ;

clean:
	rm -rf build/
	rm -rf source/pyDKB

pdf: latexpdf
	mkdir -p build/pdf
	cp build/latex/*.pdf build/pdf

source/pyDKB:
	$(APIDOC) -T -f -M -o source/pyDKB ../Utils/Dataflow/pyDKB

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile source/pyDKB
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
