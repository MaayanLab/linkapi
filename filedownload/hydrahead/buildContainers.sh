# get rid of old stuff
docker rmi -f $(docker images | grep "^<none>" | awk "{print $3}")
docker rm $(docker ps -q -f status=exited)

docker kill hydrahead
docker rm hydrahead


docker build -f Dockerfile -t maayanlab/hydrahead .

docker push maayanlab/hydrahead

