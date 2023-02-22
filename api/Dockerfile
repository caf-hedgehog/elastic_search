FROM python:3.11-slim-bullseye
ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip && pip install fastapi uvicorn
RUN pip install pandas 
RUN pip install beautifulsoup4 
RUN pip install requests 
RUN pip install elasticsearch==7.10.* 
RUN pip install aiohttp
RUN pip install faker

RUN mkdir /app
COPY ./ /app
WORKDIR /app

ENTRYPOINT ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002"]

EXPOSE 8002
