make:
	python main.py

list:
	PYTHONWARNINGS="ignore::DeprecationWarning" weaviate-cli get collection

empty:
	PYTHONWARNINGS="ignore::DeprecationWarning" weaviate-cli delete collection --collection TextChunk
	PYTHONWARNINGS="ignore::DeprecationWarning" weaviate-cli delete collection --collection ImageChunk

rebuild:
	make empty ; make ; make list