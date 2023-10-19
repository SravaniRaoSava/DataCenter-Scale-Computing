From python

WORKDIR /app/lab2
COPY pipeline.py pipeline.py
RUN pip install pandas sqlalchemy psycopg2

ENTRYPOINT ["python","pipeline.py"]