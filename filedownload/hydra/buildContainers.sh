# get rid of old stuff
docker rmi -f $(docker images | grep "^<none>" | awk "{print $3}")
docker rm $(docker ps -q -f status=exited)

docker kill hydra
docker rm hydra


docker build -f Dockerfile -t maayanlab/hydra .

docker push maayanlab/hydra

