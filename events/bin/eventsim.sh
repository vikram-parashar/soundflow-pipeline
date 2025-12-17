#! /bin/bash
java -XX:+AggressiveOpts -XX:+UseG1GC -XX:+UseStringDeduplication -Xmx8G -jar /app/target/eventsim-assembly-1.0.jar $*
