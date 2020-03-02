FROM amd64/ibmjava:sfj
COPY tika_server/tika-server-1.23.jar /tika_server/
RUN apt-get update
RUN apt-get --yes install curl
RUN apt-get --yes install tesseract-ocr
RUN apt-get install tesseract-ocr-deu
RUN apt-get install tesseract-ocr-ita
RUN apt-get install tesseract-ocr-fra
CMD java -jar /tika_server/tika-server-1.23.jar --host=0.0.0.0 --port=9998