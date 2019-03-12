#!/bin/bash
echo "building myservice"
( cd myservice ; sbt assembly )

echo "building paymentservice"
( cd paymentservice ; sbt assembly )
