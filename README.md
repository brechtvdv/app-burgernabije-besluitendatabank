# Burgernabije Besluitendatabank (back-end)

[The back-end for BNB](https://burgernabije-besluitendatabank-dev.s.redhost.be/), a project that uses linked data to empower everyone in Flanders to consult the decisions made by their local authorities.

This project has different important moving parts:
- The harvester (which is available in [the app-lblod-harvester repository](https://github.com/lblod/app-lblod-harvester)). This processes government-provided data into consumable [data endpoints, which you can view here](#what-endpoints-can-be-used)
- The back-end (this repository). This is a docker-compose configuration that combines the front-end together with other services.
- The front-end (which is available in [the frontend-burgernabije-besluitendatabank repo](https://github.com/lblod/frontend-burgernabije-besluitendatabank)). This is an Ember frontend 


You can check out more info on besluitendatabanken [here](https://lokaalbestuur.vlaanderen.be/besluitendatabank), and the [back-end](https://github.com/lblod/frontend-burgernabije-besluitendatabank) here. The front-end repo only contains front-end specific information, back-and and general project info will be added here.



## Tutorials
You can run this app in a few different ways
- Only run the front-end and use the existing back-end. [Instructions for this can be found in the frontend repo](https://github.com/lblod/frontend-burgernabije-besluitendatabank)
- Run the back-end with your own consumers & front-end included. [Instructions for this are found below](#basic-setup)

**Pre-requisites**: Docker & Docker-Compose installed. Some parts of the tutorials may use drc as an alias for docker-compose

### Basic setup
First, clone the repository
```bash
git clone https://github.com/lblod/app-burgernabije-besluitendatabank.git
cd app-burgernabije-besluitendatabank.git
```

#### Selecting endpoints
[You can view the existing endpoints here](#what-endpoints-can-be-used)

```yml
services:
  mandatendatabank-consumer:
   environment:
      DCR_SYNC_BASE_URL: "https://example.com/"
  op-public-consumer:
   environment:
      DCR_SYNC_BASE_URL: "https://example.com/"
  besluiten-consumer:
    environment:
      DCR_SYNC_BASE_URL: "https://example.com/"
```

#### (Optional) Set uuid-generation cronjob
```yml
services:
  uuid-generation:
    environment:
      RUN_CRON_JOBS: "true"
      CRON_FREQUENCY: "0 * * * *"
```

#### (Optional) Enable Plausible Analytics
```yml
services:
  frontend:
    environment:
      EMBER_PLAUSIBLE_APIHOST: "https://example.com"
      EMBER_PLAUSIBLE_DOMAIN: "example.com"
```

Then start the server using `docker-compose up --detach`


### Sync data external data consumers
The procedure below describes how to set up the sync for besluiten-consumer. 
The procedures should be the similar for `op-public-consumer` and `mandatendatabank-consumer`. If there are variations in the steps for these consumers, it will be noted.

The synchronization of external data sources is a structured process divided into three key stages. The first stage, known as 'initial sync', requires manual interventions primarily due to performance considerations. Following this, there's a post-processing stage, where depending on the delta-consumer stream, it may be necessary to initiate certain background processes to ensure system consistency. The final stage involves transitioning the system to the 'normal operation' mode, wherein all functions are designed to be executed automatically.

##### 1. Initial sync
##### From scratch
Setting up the sync should happen work with the following steps:

- ensure docker-compose.override.yml has AT LEAST the following information

```yml
version: '3.7'

services:
#(...) there might be other services

  besluiten-consumer:
    environment:
      DCR_SYNC_BASE_URL: "https://harvesting-self-service.lblod.info/" # you choose endpoint here
      DCR_DISABLE_DELTA_INGEST: "true"
      DCR_DISABLE_INITIAL_SYNC: "true"
# (...) there might be other information
```

- start the stack. `drc up -d`. Ensure the migrations have run and finished `drc logs -f --tail=100 migrations`
- Now the sync can be started. Ensure you update the `docker-compose.override.yml` to

```yml
version: '3.7'

services:
#(...) there might be other services

  besluiten-consumer:
    environment:
      DCR_SYNC_BASE_URL: "https://harvesting-self-service.lblod.info/" # you choose endpoint here
      DCR_DISABLE_DELTA_INGEST: "false" # <------ THIS CHANGED
      DCR_DISABLE_INITIAL_SYNC: "false" # <------ THIS CHANGED
      BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES: "true"
# (...) there might be other information
```

- start the sync `drc up -d besluiten-consumer`.
  Data should be ingesting.
  Check the logs `drc logs -f --tail=200 besluiten-consumer`

##### In case of a re-sync
In some cases, you may need to reset the data due to unforeseen issues. The simplest method is to entirely flush the triplestore and start afresh. However, this can be time-consuming, and if the app possesses an internal state that can't be recreated from external sources, a more granular approach would be necessary. We will outline this approach here. Currently, it involves a series of manual steps, but we hope to enhance the level of automation in the future.

###### besluiten-consumer

- step 1: ensure the app is running and all migrations ran.
- step 2: ensure the besluiten-consumer stopped syncing, `docker-compose.override.yml` should AT LEAST contain the following information
```yml
version: '3.7'

services:
#(...) there might be other services

  besluiten-consumer:
    environment:
      DCR_DISABLE_DELTA_INGEST: "true"
      DCR_DISABLE_INITIAL_SYNC: "true"
     # (...) there might be other information e.g. about the endpoint

# (...) there might be other information
```
- step 3: `docker-compose up -d besluiten-consumer` to re-create the container.
- step 4: We need to flush the ingested data. Sample migrations have been provided.
```
cp ./config/sample-migrations/flush-besluiten-consumer.sparql-template ./config/migrations/local/[TIMESTAMP]-flush-besluiten-consumer.sparql
docker-compose restart migrations
```
- step 5: Once migrations a success, further `besluiten-consumer` data needs to be flushed too.
```
docker-compose exec besluiten-consumer curl -X POST http://localhost/flush
docker-compose logs -f --tail=200 besluiten-consumer
```
  - This should end with `Flush successful`.
- step 6: Proceed to consuming data from scratch again, ensure `docker-compose.override.yml` should AT LEAST contain the following information
```yml
version: '3.7'

services:
#(...) there might be other services

  besluiten-consumer:
    environment:
      DCR_DISABLE_DELTA_INGEST: "false"
      DCR_DISABLE_INITIAL_SYNC: "false"
      BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES: "true"
     # (...) there might be other information e.g. about the endpoint

# (...) there might be other information
```
- step 8: Run `docker-compose up -d`
- step 9: This might take a while if `docker-compose logs besluiten-consumer |grep success Returns: Initial sync http://redpencil.data.gift/id/job/URI has been successfully run`; you should be good. (Your computer will also stop making noise)

###### op-public-consumer & mandatendatabank-consumer
As of the time of writing, there is some overlap between the two data producers due to practical reasons. This issue will be resolved eventually. For the time being, if re-synchronization is required, it's advisable to re-sync both consumers.
The procedure is identical to the one for besluiten-consumer, but with a bit of an extra synchronsation hassle. 
For both consumers you will need to first run steps 1 up to and including step 5. Once these steps completed for both consumers, you can proceed and start ingesting the data again.

#### 2. post-processing
For all delta-streams, you'll have to run `docker-compose restart resources cache`.
##### search
In order to trigger a full mu-search reindex, you can execute `sudo bash ./scripts/reset-elastic.sh` (the stack must be up).
It takes a while to reindex, please consider using a small dataset to speed it up.

#### 3. switch to 'normal operation' mode
Essentially, we want to force the data to go through mu-auth again, which is responsible for maintaining the cached data in sync. So ensure in `docker-compose.override.yml` the following.
```yml
version: '3.7'

services:
#(...) there might be other services

  besluiten-consumer:
    environment:
      DCR_DISABLE_DELTA_INGEST: "false"
      DCR_DISABLE_INITIAL_SYNC: "false"
      BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES: 'false' # <------ THIS CHANGED
     # (...) there might be other information e.g. about the endpoint

# (...) there might be other information
```
Again, a the time of writing, the same configuration is valid for the other consumers.
After updating `docker-compose.override.yml`, don't forget `docker-compose up -d`
Ensure the flag `BYPASS_MU_AUTH_FOR_EXPENSIVE_QUERIES` is set to `false` for **EVERY CONSUMER**
#### What endpoints can be used?
##### besluiten-consumer

- Production data: https://harvesting-self-service.prod.lblod.info/
- QA data: https://harvesting-self-service.lblod.info/
- DEV data: https://dev.harvesting-self-service.lblod.info/

##### mandatendatabank-consumer

- Production data: https://loket.lokaalbestuur.vlaanderen.be/
- QA data: https://loket.lblod.info/
- DEV data: https://dev.loket.lblod.info/

##### op-public-consumer

- Production data: https://organisaties.abb.vlaanderen.be/
- QA data: https://organisaties.abb.lblod.info/
- DEV data: https://dev.organisaties.abb.lblod.info/


## Reference

### Models

This project is built around the following structure:
![Diagram for the relationship models](https://data.vlaanderen.be/doc/applicatieprofiel/besluit-publicatie/html/overview.jpg)

Source: [data.vlaanderen.be](https://data.vlaanderen.be/doc/applicatieprofiel/besluit-publicatie/)
