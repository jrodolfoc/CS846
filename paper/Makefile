.PHONY: all clean

all : paper.pdf

paper.pdf : paper.tex paper.bib tex/*
	pdflatex paper.tex
	bibtex paper.aux
	pdflatex paper.tex
	pdflatex paper.tex

clean:
	rm -f paper.aux paper.bbl paper.blg paper.log paper.out paper.pdf paper.toc paper.tex~ paper.bib~ Makefile~
	rm -f tex/conclusions.tex~ tex/data_collection.tex~ tex/future_work.tex~ tex/intro.tex~ tex/related_work.tex~
	rm -f tex/threats-to-validity.tex~ tex/results.tex~
