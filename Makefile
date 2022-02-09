build:
	docker-compose build

up:
	docker-compose up

down:
	docker-compose down --remove-orphans

up-silent:
	docker-compose up -d

shell:
	docker exec -it tp1_nabson_fernando_marcos bin/bash