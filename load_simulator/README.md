#Load simulator


####
This repository contains a load simulator that was created to introduce many requests per second on the Serenade application via an external system.
This turned out useful during the development of the Serenade application. Although this work is not described in our paper, we make this code available.

We first prepare the data by sorting all events on the 'Time' column. The load generator can then replay this file which provides a more realistic simulation.
```console
bin/dsort.sh ../datasets/ecomm-clicks-50m_train_real.txt ../datasets/ecomm-clicks-50m_train_real.txt
```

```console
mvn clean gatling:test -Dgatling.simulationClass=serenadeserving.SerenadeSimilation
```
