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

CONTENT = """000010b5-0000-0000-21aa-a55001000000,"2/3/2025 9:52:15 AM","2/3/2025 9:52:15 AM",0,0,0,0,4,0,0,0,0,1,0,0,0,0,,0,0,0,0,0,0,0,0,0,0,0,,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000",,449,"#000000015008B2F2",,5646075567,0,0,0,,"10296360910",0,,0,0,,,,,-1,0,0,0,0,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000",0,0,,,,0,,,,0,"2025-02-03T09:52:01.0000000Z",,0,"1900-01-01T00:00:00.0000000Z",,0,"6800",1515211003,5637144576,1052856296,5648001569,4277,1052856296,"1900-01-01T00:00:00.0000000+00:00","2025-02-03T09:52:01.0000000Z",
000010b5-0000-0000-22aa-a55001000000,"2/3/2025 9:52:15 AM","2/3/2025 9:52:15 AM",0,0,0,0,4,0,0,0,0,1,0,0,0,0,,0,0,0,0,0,0,0,0,0,0,0,,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000",,449,"#000000015008B2F6",,5646075568,0,0,0,,"10295460616",0,,0,0,,,,,-1,0,0,0,0,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000",0,0,,,,0,,,,0,"2025-02-03T09:52:02.0000000Z",,0,"1900-01-01T00:00:00.0000000Z",,0,"6800",782622300,5637144576,1052856331,5648001570,4277,1052856331,"1900-01-01T00:00:00.0000000+00:00","2025-02-03T09:52:02.0000000Z",
000010b5-0000-0000-23aa-a55001000000,"2/3/2025 9:52:15 AM","2/3/2025 9:52:15 AM",0,0,0,0,4,0,0,0,0,1,0,0,0,0,,0,0,0,0,0,0,0,0,0,0,0,,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000",,449,"#000000015008B2F2",,5646075569,0,0,0,,"21479361068",0,,0,0,,,,,-1,0,0,0,0,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000",0,0,,,,0,,,,0,"2025-02-03T09:52:02.0000000Z",,0,"1900-01-01T00:00:00.0000000Z",,0,"6800",1137199275,5637144576,1052856347,5648001571,4277,1052856347,"1900-01-01T00:00:00.0000000+00:00","2025-02-03T09:52:02.0000000Z",
000010b5-0000-0000-24aa-a55001000000,"2/3/2025 9:52:15 AM","2/3/2025 9:52:15 AM",0,0,0,0,4,0,0,0,0,1,0,0,0,0,,0,0,0,0,0,0,0,0,0,0,0,,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000",,449,"#000000015008B2F6",,5646075570,0,0,0,,"50457401001",0,,0,0,,,,,-1,0,0,0,0,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000",0,0,,,,0,,,,0,"2025-02-03T09:52:03.0000000Z",,0,"1900-01-01T00:00:00.0000000Z",,0,"6800",1144801195,5637144576,1052856365,5648001572,4277,1052856365,"1900-01-01T00:00:00.0000000+00:00","2025-02-03T09:52:03.0000000Z",
000010b5-0000-0000-25aa-a55001000000,"2/3/2025 9:52:15 AM","2/3/2025 9:52:15 AM",0,0,0,0,4,0,0,0,0,1,0,0,0,0,,0,0,0,0,0,0,0,0,0,0,0,,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000",,449,"#000000015008B2F6",,5646075571,0,0,0,,"50457455778",0,,0,0,,,,,-1,0,0,0,0,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000",0,0,,,,0,,,,0,"2025-02-03T09:52:03.0000000Z",,0,"1900-01-01T00:00:00.0000000Z",,0,"6800",821437634,5637144576,1052856393,5648001573,4277,1052856393,"1900-01-01T00:00:00.0000000+00:00","2025-02-03T09:52:03.0000000Z",
000010b5-0000-0000-26aa-a55001000000,"2/3/2025 9:52:15 AM","2/3/2025 9:52:15 AM",0,0,0,0,4,0,0,0,0,1,0,0,0,0,,0,0,0,0,0,0,0,0,0,0,0,,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000","2025-02-15T00:00:00.0000000",,449,"#000000015008B2F2",,5646075572,0,0,0,,"45049301291",0,,0,0,,,,,-1,0,0,0,0,"1900-01-01T00:00:00.0000000","1900-01-01T00:00:00.0000000",0,0,,,,0,,,,0,"2025-02-03T09:52:04.0000000Z",,0,"1900-01-01T00:00:00.0000000Z",,0,"6800",568310987,5637144576,1052856403,5648001574,4277,1052856403,"1900-01-01T00:00:00.0000000+00:00","2025-02-03T09:52:04.0000000Z",
"""

MODEL_JSON = """{
                  "name": "cdm",
                  "description": "cdm",
                  "version": "1.0",
                  "entities": [
                    {
                      "$type": "LocalEntity",
                      "name": "inventtrans",
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
                                              "name": "groupreftype_ru",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "intercompanyinventdimtransferred",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "invoicereturned",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "packingslipreturned",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "statusissue",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "statusreceipt",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "storno_ru",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "stornophysical_ru",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "transchildtype",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "valueopen",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "valueopenseccur_ru",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "itmskipvarianceupdate",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "itmmustskipadjustment",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "sysdatastatecode",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "activitynumber",
                                              "dataType": "string",
                                              "maxLength": 50,
                                              "cdm:traits": [
                                                {
                                                  "traitReference": "is.constrained",
                                                  "arguments": [
                                                    {
                                                      "name": "maximumLength",
                                                      "value": 50
                                                    }
                                                  ]
                                                }
                                              ]
                                            },
                                            {
                                              "name": "costamountadjustment",
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
                                              "name": "costamountoperations",
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
                                              "name": "costamountphysical",
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
                                              "name": "costamountposted",
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
                                              "name": "costamountseccuradjustment_ru",
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
                                              "name": "costamountseccurphysical_ru",
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
                                              "name": "costamountseccurposted_ru",
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
                                              "name": "costamountsettled",
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
                                                      "value": 16
                                                    }
                                                  ]
                                                }
                                              ]
                                            },
                                            {
                                              "name": "costamountsettledseccur_ru",
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
                                              "name": "costamountstd",
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
                                              "name": "costamountstdseccur_ru",
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
                                              "name": "dateclosed",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "dateclosedseccur_ru",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "dateexpected",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "datefinancial",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "dateinvent",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "datephysical",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "datestatus",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "grouprefid_ru",
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
                                              "name": "inventdimfixed",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "inventdimid",
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
                                              "name": "inventdimidsales_ru",
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
                                              "name": "inventtransorigin",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "inventtransorigindelivery_ru",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "inventtransoriginsales_ru",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "inventtransorigintransit_ru",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "invoiceid",
                                              "dataType": "string",
                                              "maxLength": 50,
                                              "cdm:traits": [
                                                {
                                                  "traitReference": "is.constrained",
                                                  "arguments": [
                                                    {
                                                      "name": "maximumLength",
                                                      "value": 50
                                                    }
                                                  ]
                                                }
                                              ]
                                            },
                                            {
                                              "name": "itemid",
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
                                              "name": "markingrefinventtransorigin",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "packingslipid",
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
                                              "name": "pdscwqty",
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
                                              "name": "pdscwsettled",
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
                                              "name": "pickingrouteid",
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
                                              "name": "projadjustrefid",
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
                                              "name": "projcategoryid",
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
                                              "name": "projid",
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
                                              "name": "qty",
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
                                              "name": "qtysettled",
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
                                                      "value": 16
                                                    }
                                                  ]
                                                }
                                              ]
                                            },
                                            {
                                              "name": "qtysettledseccur_ru",
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
                                                      "value": 16
                                                    }
                                                  ]
                                                }
                                              ]
                                            },
                                            {
                                              "name": "returninventtransorigin",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "revenueamountphysical",
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
                                              "name": "shippingdateconfirmed",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "shippingdaterequested",
                                              "dataType": "dateTime",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "taxamountphysical",
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
                                              "name": "timeexpected",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "transchildrefid",
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
                                              "name": "voucher",
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
                                              "name": "voucherphysical",
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
                                              "name": "nonfinancialtransferinventclosing",
                                              "dataType": "int64",
                                              "maxLength": -1
                                            },
                                            {
                                              "name": "loadid",
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
                                              "name": "receiptid",
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
                                              "name": "itmcosttypeid",
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
                                              "name": "itmcosttransrecid",
                                              "dataType": "int64",
                                              "maxLength": -1
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
                    }
                  ]
                }"""

AZURITE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://10.1.0.6:10001/devstoreaccount1'
CONTAINER = "cdm-e2e"
# Get the current date and time
now = datetime.utcnow()

# Subtract 6 hours
start_time = now - timedelta(hours=6)

# Generate formatted strings for each hour
FOLDERS = [(start_time + timedelta(hours=i)).strftime("%Y-%m-%dT%H.%M.%SZ") for i in range(8)]

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
    blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
    for folder in FOLDERS:
        upload_blob_file(blob_service_client, CONTAINER, f"{folder}/inventtrans/2020.csv", CONTENT)
        upload_blob_file(blob_service_client, CONTAINER, f"{folder}/inventtrans_modified/2020.csv", CONTENT)

    upload_blob_file(blob_service_client, CONTAINER, "model.json", MODEL_JSON)

create_container()
create_blobs()
