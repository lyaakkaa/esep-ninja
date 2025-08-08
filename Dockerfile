FROM python:3.9

ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y gettext libgettextpo-dev

RUN mkdir -p static media

COPY ./requirements.txt /requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install --upgrade -r /requirements.txt

ADD . /updaterninja
WORKDIR /updaterninja

COPY ./entrypoint.dev.sh /entrypoint.dev.sh
RUN chmod +x /entrypoint.dev.sh

CMD ["/entrypoint.dev.sh"]