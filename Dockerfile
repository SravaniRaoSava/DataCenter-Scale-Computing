From python

WORKDIR /app
COPY pipeline.py HW1_pipeline.py
COPY covid_worldwide.csv Source.csv
RUN pip install pandas

ENTRYPOINT ["bash"]