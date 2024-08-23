FROM openjdk:22-bullseye
RUN apt update && apt install python3-pip -y
ENV PIP_BREAK_SYSTEM_PACKAGES=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# Application specific
WORKDIR /code
COPY requirements.txt /code/
RUN pip3 install -r requirements.txt
COPY ./consumer /code/




