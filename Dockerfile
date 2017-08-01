FROM python:3-alpine
MAINTAINER Cyril Margorin <comargo@gmail.com>
ADD requirements.txt /tmp/requirements.txt
RUN pip install -qr /tmp/requirements.txt
EXPOSE 8080
WORKDIR /meterdigitizer-app
VOLUME /meterdigitizer-app/db
VOLUME /meterdigitizer-app/config
ADD . .
CMD ["python", "launch.py"]
