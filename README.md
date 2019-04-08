# Spark structured streaming

Docker Compose will help you build and start all the services that are part of this assignment. By running `docker-compose up --build` from the root folder of this project you can start kafka, zookeeper, the paymentservice and your new service. `docker ps` will show you the currently running docker containers.
In case you would like to terminate all containers, run `docker-compose down`

From within the docker containers all nodes are available by their name in the docker-compose file. (f.e. Kafka can be reached via `kafka:9092` from any docker container in the docker-compose).


### 1. Consume and Print
 As initial task we would like to consume the payment events from kafka and simply print them to the log. 


### 2. Running Aggregate on Tour Value
We would like to know how much value a driver is generating over his whole lifetime. On every batch iteration sum the tour_value coming from the payment service and calculate a cumulative sum.

### 3. Time Based Aggregate
In order to evaluate the driver's more recent performance life time aggregates do not really help. We would like to understand how drivers perform more recently though.
So,Aggregate in a 1 minute interval the performance of drivers based on their tour value and the number of tours they did. (each payment event == successful tour)

### 4. Produce Kafka Message
We need to inform another service about the drivers we identified in task 3. Please write a kafka producer, that sends an event for every driver identified in task 3. into a new topic.
