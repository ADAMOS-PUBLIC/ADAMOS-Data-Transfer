# ADAMOS Data Transfer

Library that copies devices and sub-devices including their external IDs, measurements, events, alarms from a source tenant to a target tenant.

## How to build the library

The library is built using Maven. Run `mvn package` to build a runnable jar containing all dependencies.

## How to run the Library

Place the runnable jar together with a config.properties file in the same directory. Add the following parameters to config.properties:


* sourceUrl: URL of the source tenant (e.g. https://<tenant>.<domain>.com)
* sourceUsername: username of a user that can access the source tenant
* sourcePassword: password of user specified with sourceUsername
* targetUrl: URL of the target tenant (e.g. https://<tenant>.<domain>.com)
* targetUsername: username of a user that can access the target tenant
* targetPassword: password of user specified with targetUsername

To run the library call `java -jar <jar-file> [deviceId]`.
