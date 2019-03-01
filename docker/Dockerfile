FROM amancevice/pandas:0.24.1-slim
WORKDIR /app
COPY . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 9999
CMD ["python", "API.py"]

