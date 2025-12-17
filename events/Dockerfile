FROM eclipse-temurin:8-jre-alpine

WORKDIR /app

COPY . /app

RUN chmod +x bin/eventsim.sh

CMD ["sh", "bin/eventsim.sh", "-c", "configs/Accordion-config.json", "--from", "365", "--nusers", "1000", "--growth-rate", "0.01", "output"]
