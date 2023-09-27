# Databricks notebook source
# MAGIC %md
# MAGIC # Gerando Token

# COMMAND ----------

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

# COMMAND ----------

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
# https://www.youtube.com/watch?v=APj3MFt2w5I/

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md
# MAGIC # Acessando os endpoints

# COMMAND ----------

# MAGIC %md
# MAGIC ## Groups

# COMMAND ----------

# MAGIC %md
# MAGIC ### List - Groups

# COMMAND ----------

# DBTITLE 1,Acessando os endpoints do Power BI
endpoint = "https://api.powerbi.com/v1.0/myorg/groups"
response = requests.get(endpoint, headers=headers)
if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### List - Groups Users

# COMMAND ----------

group_id = "5bcd35cf-5db3-43a7-b782-a8626e535043"  # id do grupo: People Analytics - Saúde e Segurança
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/users"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataflows

# COMMAND ----------

# MAGIC %md
# MAGIC ### List - Dataflow
# MAGIC [Doc Link](https://learn.microsoft.com/pt-br/rest/api/power-bi/dataflows/get-dataflows)

# COMMAND ----------

endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    schema = "objectId string, name string, description string, configuredBy string, users array<string>"
    spark.createDataFrame(response_ok, schema).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post - Refresh Dataflow
# MAGIC [Doc API](https://learn.microsoft.com/pt-br/rest/api/power-bi/dataflows/refresh-dataflow)

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### List - Dataflow Transactions
# MAGIC
# MAGIC [Doc Api](https://learn.microsoft.com/pt-br/rest/api/power-bi/dataflows/get-dataflow-transactions)

# COMMAND ----------

dataflow_id = "607e177a-59ef-4d58-9615-c828b8e9c4ad"
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows/{dataflow_id}/transactions"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### List - Upstream Dataflows In Group
# MAGIC
# MAGIC [Doc API](https://learn.microsoft.com/pt-br/rest/api/power-bi/dataflows/get-upstream-dataflows-in-group)

# COMMAND ----------

endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/dataflows/{dataflow_id}/upstreamDataflows"
response = requests.get(endpoint, headers=headers)

if response.ok:
    if len(response.json().get("value")) > 0:
        response_ok = response.json().get("value")
        spark.createDataFrame(response_ok).display()
    else:
        print(response.json())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### List - Dataset From Group

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### List -  Refresh History In Group

# COMMAND ----------

dataset_id = (
    "82e3f38f-7e5b-4292-adfc-34b367745c1b"  # id do dataset: Usage Metrics Report
)
endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### List - Dataset Users In Group

# COMMAND ----------

endpoint = (
    f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/users"
)
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json().get("value")
    spark.createDataFrame(response_ok).display()

# COMMAND ----------

endpoint = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshSchedule"
response = requests.get(endpoint, headers=headers)

if response.ok:
    response_ok = response.json()
    response_ok["odata_context"] = response_ok["@odata.context"]
    response_ok["days"] = list(response_ok["days"])
    response_ok["times"] = list(response_ok["times"])
    response_ok.pop("@odata.context")
    schema = StructType(
        [
            StructField("days", ArrayType(StringType()), True),
            StructField("times", ArrayType(StringType()), True),
            StructField("enabled", BooleanType(), True),
            StructField("localTimeZoneId", StringType(), True),
            StructField("notifyOption", StringType(), True),
            StructField("odata_context", StringType(), True),
        ]
    )
    spark.createDataFrame(response_ok, schema).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Post - Refresh Dataset In Group
# MAGIC
# MAGIC
# MAGIC **Descrição**: 

# COMMAND ----------

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