from scraper_config import CONSUMER, KafkaException
import scraper_producer
import sys
# Read messages from Kafka, print to stdout
try:
    while True:
        msg = CONSUMER.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Proper message
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                (msg.topic(), msg.partition(), msg.offset(),
                                str(msg.key())))
            
            scraper_producer.scrap(msg.value())

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down consumer to commit final offsets.
    CONSUMER.close()