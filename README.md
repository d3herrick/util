# util
Contains code for various utility functions.

## Logging

All code uses slf4j logging facade, meaning you'll need to enable a logging implementation.  This code was tested using log4j2.

## Tests

All unit tests were developed with JUnit5, using features supported by JUnit5 only.

For classes that use services of AWS, for example, you will need to provide the credentials for an AWS account provisioned with the requisite permissions.  Unit tests describe the specific AWS permissions they require.

