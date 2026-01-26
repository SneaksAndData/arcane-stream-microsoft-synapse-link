#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
import os

MODEL_JSON = """{
                  "name": "cdm",
                  "description": "cdm",
                  "version": "1.0",
                  "entities": [
                    {
                      "$type": "LocalEntity",
                      "name": "currency",
                      "description": "currency",
                      "annotations": [
                        {
                          "name": "Athena:PartitionGranularity",
                          "value": "Year"
                        },
                        {
                          "name": "Athena:InitialSyncState",
                          "value": "Completed"
                        },
                        {
                          "name": "Athena:InitialSyncDataCompletedTime",
                          "value": "1/1/2020 0:00:00 PM"
                        }
                      ],
                      "attributes": [
                        {
                          "name": "Id",
                          "dataType": "guid",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkCreatedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkModifiedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "iseuro",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypeassetdep_jp",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypeprice",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypepurch",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypesales",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "ltmroundofftypelineamount",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysdatastatecode",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "currencycode",
                          "dataType": "string",
                          "maxLength": 3,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 3
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "currencycodeiso",
                          "dataType": "string",
                          "maxLength": 3,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 3
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundingprecision",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffassetdep_jp",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffprice",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffpurch",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffsales",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "symbol",
                          "dataType": "string",
                          "maxLength": 5,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 5
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "txt",
                          "dataType": "string",
                          "maxLength": 120,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 120
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "exchratemaxvariationpercent_mx",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "decimalscount_mx",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "ltmroundofflineamount",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifieddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifiedtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "createdby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "createdtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "recversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "partition",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysrowversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "recid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "tableid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "versionnumber",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createdon",
                          "dataType": "dateTimeOffset",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedon",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "IsDelete",
                          "dataType": "boolean",
                          "maxLength": -1
                        }
                      ],
                      "partitions": []
                    },
                    {
                      "$type": "LocalEntity",
                      "name": "dimensionattributelevelvalue",
                      "description": "dimensionattributelevelvalue",
                      "annotations": [
                        {
                          "name": "Athena:PartitionGranularity",
                          "value": "Year"
                        },
                        {
                          "name": "Athena:InitialSyncState",
                          "value": "Completed"
                        },
                        {
                          "name": "Athena:InitialSyncDataCompletedTime",
                          "value": "1/1/2020 0:00:00 PM"
                        }
                      ],
                      "attributes": [
                        {
                          "name": "Id",
                          "dataType": "guid",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkCreatedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkModifiedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "sysdatastatecode",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevalue",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevaluegroup",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "displayvalue",
                          "dataType": "string",
                          "maxLength": 30,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 30
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "ordinal",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "backingrecorddataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifieddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifiedtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "createdby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "createdtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "recversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "partition",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysrowversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "recid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "tableid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "versionnumber",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createdon",
                          "dataType": "dateTimeOffset",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedon",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "IsDelete",
                          "dataType": "boolean",
                          "maxLength": -1
                        }
                      ],
                      "partitions": []
                    },
                    {
                      "$type": "LocalEntity",
                      "name": "dimensionattributelevel",
                      "description": "dimensionattributelevel",
                      "annotations": [
                        {
                          "name": "Athena:PartitionGranularity",
                          "value": "Year"
                        },
                        {
                          "name": "Athena:InitialSyncState",
                          "value": "Completed"
                        },
                        {
                          "name": "Athena:InitialSyncDataCompletedTime",
                          "value": "1/1/2020 0:00:00 PM"
                        }
                      ],
                      "attributes": [
                        {
                          "name": "Id",
                          "dataType": "guid",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkCreatedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkModifiedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "sysdatastatecode",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevalue",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevaluegroup",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "displayvalue",
                          "dataType": "string",
                          "maxLength": 30,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 30
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "ordinal",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "backingrecorddataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifieddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifiedtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "createdby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "createdtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "recversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "partition",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysrowversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "recid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "tableid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "versionnumber",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createdon",
                          "dataType": "dateTimeOffset",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedon",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "IsDelete",
                          "dataType": "boolean",
                          "maxLength": -1
                        }
                      ],
                      "partitions": []
                    }
                  ]
                }"""

AZURITE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10001/devstoreaccount1'
CONTAINER = "cdm-e2e"

blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
def upload_blob_file(blob_service_client: BlobServiceClient, container_name: str, blob_name: str, content: str):
    blob_service_client.get_container_client(container=container_name).upload_blob(name=blob_name, data=content.encode('utf-8'), overwrite=True)

def create_container():
   # Create a container for Azurite for the first run
   blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
   try:
      blob_service_client.create_container(CONTAINER)
   except Exception as e:
      print(e)

def create_blobs():
    upload_blob_file(blob_service_client, CONTAINER, "model.json", MODEL_JSON)


create_container()
upload_blob_file(blob_service_client, CONTAINER, "Changelog/changelog.info", FOLDERS[-1])
