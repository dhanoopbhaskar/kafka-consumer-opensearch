# kafka-consumer-opensearch

Related repository - [kafka-producer-wikimedia](https://github.com/dhanoopbhaskar/kafka-producer-wikimedia)

## [Docker quickstart](https://opensearch.org/docs/latest/#docker-quickstart)

- Install and start Docker Desktop. 
- Run the following commands:

    
        docker pull opensearchproject/opensearch:2.0.1
   
        docker run -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" opensearchproject/opensearch:2.0.1

- In a new terminal session, run:

    
        curl -XGET --insecure -u 'admin:admin' 'https://localhost:9200'

- Create your first index.


        curl -XPUT --insecure -u 'admin:admin' 'https://localhost:9200/my-first-index'

- Add some data to your newly created index.


        curl -XPUT --insecure -u 'admin:admin' 'https://localhost:9200/my-first-index/_doc/1' -H 'Content-Type: application/json' -d '{"Description": "To be or not to be, that is the question."}'

- Retrieve the data to see that it was added properly.


        curl -XGET --insecure -u 'admin:admin' 'https://localhost:9200/my-first-index/_doc/1'

- After verifying that the data is correct, delete the document.


        curl -XDELETE --insecure -u 'admin:admin' 'https://localhost:9200/my-first-index/_doc/1'

- Finally, delete the index.


        curl -XDELETE --insecure -u 'admin:admin' 'https://localhost:9200/my-first-index/'



## Start Zookeeper
    zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

if installed using brew,

    zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties

## Start Kafka server
    kafka-server-start /usr/local/etc/kafka/server.properties

if installed using brew,

    kafka-server-start /opt/homebrew/etc/kafka/server.properties

## Create a topic
    kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic wikimedia.recentChanges

## Read from the topic
    kafka-console-consumer --bootstrap-server localhost:9092 --topic wikimedia.recentChanges