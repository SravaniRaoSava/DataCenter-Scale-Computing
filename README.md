The main objective of this extensive project is to explore rental vehicle data analytics in order to identify important information about customer behavior, 
preferred rental locations, and the level of popularity of particular car models. 
The fundamental goal is to arm vehicle rental companies with the knowledge they need to improve customer experience, streamline operations, pricing policies, 
and focused advertising campaigns.
The main goal of this analysis is to identify the critical indications that impact rental decisions by identifying patterns that lie behind 
the wide range of client preferences.
The analysis of rental locations is a crucial component that illuminates the specific regional characteristics that are in demand.
Rental companies can improve their offers by identifying the agencies and models that are most popular with clients. 
This allows them to potentially increase or diversify their fleets in order to better meet market demands.

# DCSC project Demo

We have launched and AWS EC2 RHEL 9 instance to install and configure required tools for the project.

## Login to AWS EC2 instance using gitbash from windows or macOS Terminal

```bash
 ssh -i .ssh/"kafka.pem" ec2-user@ec2-44-202-119-172.compute-1.amazonaws.com
```

## Install/configure

### Install kafka

1. Installation

   ```bash
   wget https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz
   ```

   Unzip the file and move the kafka to `/opt/kafka`

   ```bash
   tar xzf kafka_2.12-3.6.0.tgz

   mv kafka_2.12-3.6.0 /opt/kafka
   ```

2. configure kafka properties and copy systemd file to start the service

3. enable kafka and start service

```bash
systemctl enable kafka
systemctl start kafka
```

### zookeeper

1. enable and start the zookeeper service

```bash
systemctl enable zookeeper
systemctl start zookeeper
```

### check kafka and zookeeper status

check the status of both kafka and zookeeper

```bash
systemctl status kafka

systemctl status zookeeper

```

### Install docker

1. Install docker

   `dnf install docker`

2. check the docker service

   `systemctl status podman`

### Install/configure postgres using docker

Search for docker postgres image

`docker search postgres`

Pull the docker postgres image

`docker pull docker.io/library/postgres`

verify the image

`docker images`

Run the docker container to start the postgres

`docker run --name some-postgres -e POSTGRES_PASSWORD_FILE=/run/secrets/postgres-passwd -d postgres`

Verify the postgres pgadmin

```
http://44.202.119.172/pgadmin4/browser/
```

## Clone kafka python ETL scripts

1. change the user to kafka user in ec2 instance to configure ETL scripts

   `sudo su - kafka`

2. clone from github

   ```bash
   git clone https://github.com/AkithaPinisetti2107/DCSC_Final_Project.git
   ```

3. cd to DCSC project directory

4. run the scrits to test using pyhon3

   `python3 DCSC_kafka_data_producer.py /dev/null`

   Here using `/dev/null` to re-direct console output.

5. Validate the kafka producer message throuch kafka consumer command.

   ```bash
   /opt/kafka/bin/kafka-console-consumer.sh --topic dcsc --bootstrap-server localhost:9092 --from-beginning
   ```

6. once you confirm messages flowing through kafka, run the postgres python script to load the kafka json messages into postgresql database

   `python3 DCSC_kafka_psql.py`

7. validate the data in postgresql database either using `pgadmin4` UI or simply running `psql` commands from ec2-instance.

## Schedule ETL Jobs to ingest data into kafka and postgresql

once the data is confirmed we can schedule the job using `crontab` to run the ETL scripts periodically.

```bash
    crontab -e # this command will open crontab entries for kafka user

    # produce messages to kafka every hour
    0 0 */1 * *  /home/kafka/DCSC_Final_Project/python3 DCSC_kafka_data_producer.py /dev/null

    # consume kafka message and load to postgresql every 10th minute of hour
    0 0 */10 * *  /home/kafka/DCSC_Final_Project/python3 DCSC_kafka_psql.py /dev/null
```

Validate crontab

`crontab -l`

## Tableau Visualization
Integrate the postgresql into Tableau for visualization.


<img width="527" alt="image" src="https://github.com/AkithaPinisetti2107/DCSC_Final_Project/assets/152043128/0879e9a8-d545-4377-a935-dce7c6e9ee8d">
