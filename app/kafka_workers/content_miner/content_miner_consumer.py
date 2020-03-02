from content_miner_config import CONSUMER, KafkaException, logger
import content_miner_producer
import sys

def tika_consume():
    try:
        while True:
            msg = CONSUMER.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # msg.commit()
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                    (msg.topic(), msg.partition(), msg.offset(),
                                    str(msg.key())))
                if(msg.value()):
                    content_miner_producer.send(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        CONSUMER.close()

if __name__=='__main__':
    logger.info("Init tika consumer loop")
    import nltk
    nltk.download('punkt')
    while True:
        tika_consume()