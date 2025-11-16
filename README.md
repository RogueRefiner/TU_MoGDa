# How to run: 
## Prerequisites
- Docker

## Install uv
https://docs.astral.sh/uv/getting-started/installation/

## Virtual Environemnt
- Setup the virtual environemnt with `uv venv` 
- Activate the virtual environment with `source .venv/bin/activate`
- Install all necessary python packages to run the project with `uv sync`
- Start the docker container in the root directory `docker-compose up`

## Neo4j
Installed in the docker container <br>
The database can be accessed at http://localhost:7474/browser/ <br>
`user=neo4j` <br>
`password=password`
