docker build -t benchy .
cat data/query_params.csv | docker run --rm -i --network interview benchy