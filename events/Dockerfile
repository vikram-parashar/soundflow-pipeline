FROM eclipse-temurin:8-jre-alpine

WORKDIR /app

COPY . /app

RUN chmod +x bin/eventsim.sh

ENTRYPOINT ["sh", "bin/eventsim.sh"]
