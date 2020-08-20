# get rid of old stuff
docker rmi -f $(docker images | grep "^<none>" | awk "{print $3}")
docker rm $(docker ps -q -f status=exited)

docker kill hydratest
docker rm hydratest


docker build -f Dockerfile -t maayanlab/hydratest .

docker push maayanlab/hydratest

