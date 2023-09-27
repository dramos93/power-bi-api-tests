# Gerando Token

[Link - Vídeo de referência de como foi feito](https://www.youtube.com/watch?v=APj3MFt2w5I/)

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType,
    ArrayType,
    IntegerType,
)
import requests
import msal

tenant_id = "cef04b19-7776-4a94-b89b-375c77a8f936"
application_id = "b31e4f89-10f1-4e5a-bc5a-a4e152d32514"
username = dbutils.secrets.get("keyvault", "PeoplePowerBIEmailUser")
password = dbutils.secrets.get("keyvault", "PeoplePowerBIPassword")
authotity_url = "https://login.microsoftonline.com/" + tenant_id
scopes = ["https://analysis.windows.net/powerbi/api/.default"]

client = msal.PublicClientApplication(application_id, authority=authotity_url)

response = client.acquire_token_by_username_password(
    username=username, password=password, scopes=scopes
)

token = response["access_token"]

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
```

# Acessando os endpoints
## Groups
### List - Groups

[Doc Link](https://learn.microsoft.com/pt-br/rest/api/power-bi/groups/get-groups#code-try-0)

```python
endpoint = "https://api.powerbi.com/v1.0/myorg/groups"
response = requests.get(endpoint, headers=headers)
if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()
```

### List - Groups Users

[Doc Link](https://learn.microsoft.com/pt-br/rest/api/power-bi/groups/get-group-users)

```python
group_id = "5bcd35cf-5db3-43a7-b782-a8626e535043"  # id do grupo: People Analytics - Saúde e Segurança
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/users"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()
```

## Dataflows
### List - Dataflow

[Doc Link](https://learn.microsoft.com/pt-br/rest/api/power-bi/dataflows/get-dataflows)

```python

endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    schema = "objectId string, name string, description string, configuredBy string, users array<string>"
    spark.createDataFrame(response_ok, schema).display()
```

### Post - Refresh Dataflow

[Doc API](https://learn.microsoft.com/pt-br/rest/api/power-bi/dataflows/refresh-dataflow)

```python
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows/{dataflow_id}/refreshes"

# json={"notifyOption": "MailOnFailure", "retryCount": 1},
data = {
    "notifyOption": "NoNotification",
}
response = requests.post(
    url=endpoint,
    json=data,
    headers=headers,
)
if response.ok:
    print("Fluxo atualizado com sucesso.")
```

### List - Dataflow Transactions

[Doc Api](https://learn.microsoft.com/pt-br/rest/api/power-bi/dataflows/get-dataflow-transactions)

```python
dataflow_id = "607e177a-59ef-4d58-9615-c828b8e9c4ad"
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows/{dataflow_id}/transactions"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()
```

### List - Upstream Dataflows In Group

[Doc API](https://learn.microsoft.com/pt-br/rest/api/power-bi/dataflows/get-upstream-dataflows-in-group)

```python
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows/{dataflow_id}/upstreamDataflows"
response = requests.get(endpoint, headers=headers)

if response.ok:
    if len(response.json().get("value")) > 0:
        response_ok = response.json().get("value")
        spark.createDataFrame(response_ok).display()
    else:
        print(response.json())

```

## Datasets
### List - Dataset From Group

[Doc API](https://learn.microsoft.com/pt-br/rest/api/power-bi/datasets/get-datasets-in-group)

```python
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("webUrl", StringType(), True),
            StructField("addRowsAPIEnabled", BooleanType(), True),
            StructField("configuredBy", StringType(), True),
            StructField("isRefreshable", BooleanType(), True),
            StructField("isEffectiveIdentityRequired", BooleanType(), True),
            StructField("isEffectiveIdentityRolesRequired", BooleanType(), True),
            StructField("isOnPremGatewayRequired", BooleanType(), True),
            StructField("targetStorageMode", StringType(), True),
            StructField("createdDate", StringType(), True),
            StructField("createReportEmbedURL", StringType(), True),
            StructField("qnaEmbedURL", StringType(), True),
            StructField("description", StringType(), True),
            StructField(
                "upstreamDatasets", StringType(), True
            ),  # Pode ser uma lista de strings ou outra estrutura apropriada
            StructField(
                "users", StringType(), True
            ),  # Pode ser uma lista de strings ou outra estrutura apropriada
            StructField(
                "queryScaleOutSettings",
                StructType(
                    [
                        StructField("autoSyncReadOnlyReplicas", BooleanType(), True),
                        StructField(
                            "maxReadOnlyReplicas", StringType(), True
                        ),  # Pode ser IntegerType se necessário
                    ]
                ),
                True,
            ),
        ]
    )
    spark.createDataFrame(response_ok, schema).display()
```

### List -  Refresh History In Group

[Doc API](https://learn.microsoft.com/pt-br/rest/api/power-bi/datasets/get-refresh-history-in-group)

```python
dataset_id = (
    "82e3f38f-7e5b-4292-adfc-34b367745c1b"  # id do dataset: Usage Metrics Report
)
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()
```

### List - Dataset Users In Group

[Doc API](https://learn.microsoft.com/pt-br/rest/api/power-bi/datasets/get-dataset-users-in-group)

```python
endpoint = (
    f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/users"
)
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()
```

### Post - Refresh Dataset In Group

[Doc API](https://learn.microsoft.com/pt-br/rest/api/power-bi/datasets/refresh-dataset-in-group)

```python
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes "
response = requests.post(
    url=endpoint,
    json={"notifyOption": "MailOnFailure", "retryCount": 1},
    headers=headers,
)
print(response.json())
if response.ok:
    response_ok = response.json()
    spark.createDataFrame(response_ok).display()
```
