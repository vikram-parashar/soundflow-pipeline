@ credits to [Eventism](https://github.com/Interana/eventsim). Thanks for creating this project.
## Eventsim

Eventsim is a program that generates event data for testing and demos. It's written in Scala, because we are big data hipsters (at least sometimes). It's designed to replicate page requests for a fake music web site (picture something like Spotify); the results look like real use data, but are totally fake. You can configure the program to create as much data as you want: data for just a few users for a few hours, or data for a huge number of users of users over many years. You can write the data to files, or pipe it out to Apache Kafka.

You can use the fake data for product development, correctness testing, demos, performance testing, training, or in any other place where a stream of real looking data is useful. You probably shouldn't use this data to research machine learning algorithms, and definitely shouldn't use it to understand how real people behave.


#### BUILD
```bash
docker build -t soundflow-events:v1 .
```
#### AND RUN
```bash
docker run -v {$PWD}/output:/app/output soundflow-events:v1 
```
