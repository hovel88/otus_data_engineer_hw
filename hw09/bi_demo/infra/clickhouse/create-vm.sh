#!/bin/bash

yc compute instance create \
    --format json \
    --name clickhouse \
    --hostname clickhouse \
    --ssh-key ~/.ssh/yc_empty_rsa.pub \
    --platform standard-v2 \
    --memory 16G \
    --cores 4 \
    --core-fraction 20 \
    --preemptible \
    --create-boot-disk image-folder-id=standard-images,image-family=ubuntu-2004-lts,size=100,auto-delete=true \
    --network-interface subnet-name=network-ru-central1-d,nat-ip-version=ipv4 \
    --zone ru-central1-d \
    --async
