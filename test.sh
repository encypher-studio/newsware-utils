# Runs tests that need to be run on a Linux docker image

docker build --tag "nwfstest" .
docker run "nwfstest"