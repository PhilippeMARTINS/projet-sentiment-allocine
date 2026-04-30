# Makefile — Projet 3 : Sentiment Allociné
# Usage : make <cible>
# Sur Windows : installer make via winget (winget install GnuWin32.Make)
# ou utiliser directement les commandes Python listées ci-dessous

.PHONY: install run dashboard test clean

## Installe les dépendances (torch peut prendre plusieurs minutes)
install:
	pip install -r requirements.txt

## Lance le pipeline complet (scrape → transform → GCP → analyze)
run:
	python main.py

## Lance uniquement le scraping
scrape:
	python -m src.scraper

## Lance uniquement la transformation + sentiment NLP
transform:
	python -m src.transform

## Lance le dashboard Streamlit
dashboard:
	streamlit run app.py

## Lance les tests pytest
test:
	python -m pytest tests/ -v

## Supprime les fichiers temporaires Python
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +

## Affiche l'aide
help:
	@echo "Commandes disponibles :"
	@echo "  make install    — Installe les dépendances"
	@echo "  make run        — Lance le pipeline complet"
	@echo "  make scrape     — Lance uniquement le scraping"
	@echo "  make transform  — Lance uniquement la transformation NLP"
	@echo "  make dashboard  — Lance le dashboard Streamlit"
	@echo "  make test       — Lance les tests pytest"
	@echo "  make clean      — Nettoie les fichiers temporaires"