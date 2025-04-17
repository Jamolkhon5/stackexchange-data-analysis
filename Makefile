.PHONY: build run clean docker-build docker-run docker-clean download-data import-data queries

build:
	go build -o stackexchange-data-analysis ./cmd

run: build
	./stackexchange-data-analysis

clean:
	rm -f stackexchange-data-analysis

docker-build:
	docker-compose build

docker-run:
	docker-compose up -d

docker-clean:
	docker-compose down -v

download-data:
	mkdir -p data
	wget -O data/dba.stackexchange.com.7z https://archive.org/download/stackexchange/dba.stackexchange.com.7z
	wget -O data/dba.meta.stackexchange.com.7z https://archive.org/download/stackexchange/dba.meta.stackexchange.com.7z

import-data: docker-run
	docker-compose exec app ./stackexchange-data-analysis import

queries: import-data
	docker-compose exec app ./stackexchange-data-analysis queries

all: docker-build download-data import-data queries