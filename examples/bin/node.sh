#!/bin/bash -eu

## Initialization script for druid nodes
## Runs druid nodes as a daemon
## Environment Variables used by this script -
## DRUID_LIB_DIR - directory having druid jar files, default=lib
## DRUID_CONF_DIR - directory having druid config files, default=conf/druid
## DRUID_LOG_DIR - directory used to store druid logs, default=log
## DRUID_PID_DIR - directory used to store pid files, default=var/druid/pids
## HADOOP_CONF_DIR - directory used to store hadoop config files

usage="Usage: node.sh nodeType (start|stop|status)"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

nodeType=$1
shift

command=$1

###Masking
## use any one of the following: RUNTIME_MASKING_OPTS or REVEAL_HIP_METRICS_OPTS.
#
## (1) For runtime_masking
#to mask a Count metric/aggregator for datasource 'a', u need to specify 2metrics in the list 'a.count' & 'a.special_count_metric'
#masking zero counts can happen only if the flag 'DATASPARK_DO_MASKING' is set to true first, i.e. flag 'DATASPARK_DO_MASKING' takes precedence.
#RUNTIME_MASKING_OPTS=" -DMASK_ZERO_COUNTS=false -DDATASPARK_DO_MASKING=true -DDATASPARK_EXTRAPOLATION_FACTOR=1.35 -DDATASPARK_PRIVACY_THRESHOLD=200 -DDATASPARK_MASKED_METRICS=data.uniq,data.special_count_metric,data.sum,data.count,data2.latencyMs,data2.uniq,data2.special_count_metric,data2.sum,data2.count "
# or
## (2) if REVEAL_HIP_METRICS_OPTS are set then that means masking has been done at indexing time, so dont use runtime masking and send the hipkey with post request to broker.
#REVEAL_HIP_METRICS_OPTS=" -DREVEAL_HIP_METRIC=false -DHIP_METRIC_KEY=b275822c7da805d368bcd56fb614e95C# "

if [ -n "$RUNTIME_MASKING_OPTS" ] && [ -n "$REVEAL_HIP_METRICS_OPTS" ]; then
    echo "Both Runtime Masking and Revealing HIP metrics options have been set. Only one should be set."
    echo "If metrics are masked at indexing time then use 'REVEAL_HIP_METRICS_OPTS' ."
    echo "If metrics are not masked at indexing time then use 'RUNTIME_MASKING_OPTS'. i.e. mask at runtime ."
    exit 2
fi

LIB_DIR="${DRUID_LIB_DIR:=lib}"
CONF_DIR="${DRUID_CONF_DIR:=conf/druid}"
LOG_DIR="${DRUID_LOG_DIR:=log}"
PID_DIR="${DRUID_PID_DIR:=var/druid/pids}"

pid=$PID_DIR/$nodeType.pid

case $command in

  (start)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $nodeType node running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
    if [ ! -d "$PID_DIR" ]; then mkdir -p $PID_DIR; fi
    if [ ! -d "$LOG_DIR" ]; then mkdir -p $LOG_DIR; fi
    nohup java `cat $CONF_DIR/$nodeType/jvm.config | xargs` $REVEAL_HIP_METRICS_OPTS $RUNTIME_MASKING_OPTS -cp $CONF_DIR/_common:$CONF_DIR/$nodeType:$LIB_DIR/*:$HADOOP_CONF_DIR io.druid.cli.Main server $nodeType > $LOG_DIR/$nodeType.log
    nodeType_PID=$!
    echo $nodeType_PID > $pid
    echo "Started $nodeType node ($nodeType_PID)"
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo Stopping process `cat $pid`...
        kill $TARGET_PID
      else
        echo No $nodeType node to stop
      fi
      rm -f $pid
    else
      echo No $nodeType node to stop
    fi
    ;;

   (status)
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo RUNNING
        exit 0
      else
        echo STOPPED
      fi
    else
      echo STOPPED
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac
