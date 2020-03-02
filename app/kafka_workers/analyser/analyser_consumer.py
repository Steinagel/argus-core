from analyser_config import CONSUMER, KafkaException, logger
import analyser_delivery
import sys

def analysis_consume():
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
                    analyser_delivery.analyse(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        CONSUMER.close()

if __name__=='__main__':
    logger.info("Init analysis consumer loop")
    while True:
        analysis_consume()