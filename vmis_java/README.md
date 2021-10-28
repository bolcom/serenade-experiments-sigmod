# VMIS-Java

This repository contains an implementation of VMIS-kNN in Java, which stores the historical session data in Java hashmaps; the purpose of this variant is to evaluate the effects of not having full control over the memory management during the similarity computation (and instead relying on a garbage collector)

### getting started
The vmis_java requires a java jdk and maven to be installed.

Convert the source code and package the application using:
```bash
mvn clean package
```

and execute the application:
```bash
./start.sh path/to/training_data_1m.txt path/to/test_data_1m.txt predictions.txt latencies.txt
```


