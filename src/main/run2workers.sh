#!/bin/bash

./wc worker localhost:7777 localhost:7778&
./wc worker localhost:7777 localhost:7779&
