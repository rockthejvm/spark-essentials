@echo off
REM UPDATE: replaced "docker rm -f $(docker ps -aq)" which killed ALL containers on the machine
docker compose down -v
