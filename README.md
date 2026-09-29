Entity Indexer - a service, which crawls the pages of configured sites, extract from them the named entities and stores into a database for later analysis.

Components:

1. extrent - a gRPC service for extracting Named Entities from given text.
Implemented using:
- rusTy for entities extraction (rust binding to spaCy library);
- tokio, tonic, prost for code generation from protobuf and service infrastructure.  

extrent binaries:  
 * extrent-server: service, listening for incoming requests; 
extrent service bind address is defined via env. var. EXTRENT_BIND_ADDR, default 127.0.0.1:8090.  
The default spaCy model used is 'en_core_web_sm', can be specified via SPACY_MODEL_NAME env. var. 
But in any case, before launching extrent-server, one need to install spaCy and download spaCy model to be used, presumably in a python virtual environment:  
    (venv) pip install spacy  
    (venv) python -m spacy download spacy_model_name  
  
 * extrent-client: client for testing of named entity extraction from specified url page (via SOURCE_URL env. var.) or file (via env. var. SOURCE_FILE). The address of extrent server specified in EXTRENT_RPC_ADDR env. var., default 127.0.0.1:8090.

2. idxent - the main service, which on demand scans specified list of sites, extracts named entities from them using extrent remote service, and store information about site, its pages, and pages'entities into postgres database.
idxent service binds to address specified via IDXENT_BIND_ADDR env. var., default 127.0.0.1:8080.
The list of sites, depth of scanning, the subset of entity kinds to extract and address of extrent service to use are specified in config.toml configuration file.  
idxent service exposes REST interface, implemented with axum:

|method   |path                      |function                        |
|---------|--------------------------|--------------------------------|   
|GET      | /index/sites             | return list of sites to index  |
|DELETE   | /index/sites             | clear list of sites            |
|PUT      | /index/sites/SITE_URL    | enlist a site for indexing     |
|DELETE   | /index/sites/SITE_URL    | delist a site from indexing    |
|GET      | /index/start             | start indexing sites           |
|GET      | /index/stop              | stop indexing sites            |

3. postgres instance: used to store extracted information. idxent service connects to postgres using DATABASE_URL env. var., which should be set before launching idxent. The work of idxent with postgres is implemented using sqlx.

