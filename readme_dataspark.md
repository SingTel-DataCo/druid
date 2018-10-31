readme by dataspark.
-------------------

BUILDING_NOTES:
---------------
While building you might encounter errors thrown by codestyle plugin of maven.

To avoid code-style check kindly compile using command 'mvn clean package -P no-code-style'

PATCHES:
-------------
MIP-635: 
--------
(1) mvn clean package -P no-code-style

(2) cd $SRC_HOME/server/target

(3) cp druid-server-0.13.0-SNAPSHOT.jar to $DRUID_0_12_0_INSTALLATION_HOME/lib/druid-server-0.12.0.jar 

(4) edit $DRUID_0_12_0_INSTALLATION_HOME/conf/druid/broker/runtime.properties and set 

'druid.broker.http.unusedConnectionTimeout=PT1M'

'druid.broker.http.readTimeout=PT15M'

(5) restart broker
