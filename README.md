# flt.azf.parallel-csv-to-cosmos
# Azure Functions: Parallel CSV to Cosmos

This project contains the implementation of an Azure Function with the responsability to process a bulk csv file into a CosmosDB.
It is built to ensure large files (>100000 entries and >100Mbs) are able to be processed in bulk and within the serveless constraints.
The implementation uses Azure Functions Durable FanIn/FanOut patterns to support parallel computation.

This repo is part of an article available at https://filipelteixeira.com

Notes: 
- Current project does not support failed uploads retry.
- Big file is split into chunks within the storage, there is no implemented feature to clean it (like a timer function, manual, etc.)

## Installation

https://docs.microsoft.com/en-us/azure/azure-functions/functions-develop-local

## Usage

https://docs.microsoft.com/en-us/azure/azure-functions/functions-deployment-technologies

Function reads the configuration file to get the file name, the Azure Storage account settings and container and the CosmosDb connection configs.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)