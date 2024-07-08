#!/bin/bash

echo "--- Task: Installing RabbitMQ "
sudo apt-get install rabbitmq-server
echo "--- RabbitMQ Installtion Exited"

echo "--- Task: Starting RabbitMQ"
sudo systemctl start rabbitmq-server

echo "--- Task: Enabling RabbitMQ Management"
sudo rabbitmq-plugins enable rabbitmq_management

echo "RabbitMQ Setup Exited"
echo "Username: guest	Password: guest"
echo "RabbitMQ dashboard can be accessed with the following URL : <server_ip>:15672"
read -n 1 -s -r -p "press any key to exit..."