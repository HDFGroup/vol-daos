#!/bin/bash
orterun -np 1 -H localhost --report-uri $PWD/daos.uri --enable-recovery daos_server -c 1 -d $PWD -o $PWD/daos_server_sockets.yml
