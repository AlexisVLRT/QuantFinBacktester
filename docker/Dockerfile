FROM jericksanjuan/python3-scipy
WORKDIR /app
COPY . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 9999
CMD ["python", "API.py"]

