docker build -t benchy-test -f Dockerfile.test .

docker run --rm --network interview benchy-test