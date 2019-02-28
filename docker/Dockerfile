FROM python:3.4-alpine
WORKDIR /app
COPY . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 9999
CMD ["python", "API.py"]
