# Databricks notebook source
json_data = {
  "type": "service_account",
  "project_id": "datamaster-434121",
  "private_key_id": "5fffab8eca162d358ad98f52d768028009cd5ccd",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCV4sqJaOvXA8My\n7JveMSDPiLexWE9mDp6BO+i4wjHwPJGfJYyxCNnWylf5httIRpUv+p0wq5dkvRru\n7xsb43SpZPHtCWkBpDJY5e9hkM9JQShUaYjK0o9BH5eZy7rEu8FExSlcBgDhwGFm\nh9sr+9QaIgxif4bxRmG87vytb+a/wQHq6TbehTM4RTHsG4qFap63FkwNsAw+YNXe\nJquMjlLBBwQ0y0H1lzCPmSJwy2by5kQzGBjCu70u/xLuNmDWt/I3OS+UCU2ITEor\nBBBnPoBOhZ5x021HcqE4Tlt/xuA5w6DZREPXz6tuhQNtT+3XYdmgxQPgWZfR9swm\nincFtzvdAgMBAAECggEAC943jc5QQEf0vJNGGz+WGsC3VNRKgyj8rXx2Jqz/wcc1\n+51ASONLxmlismgWdGBcXz5vlGM921Tq7UjjA2ANBH2w90UjuqTK9MWOIRJTi3gc\njqSFl5O5Ep3DBgM+8GVyffTDm90A1F9EhxcSSGcKCiCwjNsOrwuU4R4mRUBjS8MI\nSx7hs4kmos29XKwF4JAUNaRaayqaUeVm68WXI1uVjNNC4YszhhY9JcmSrUf5M+im\nMiZH76W66Uvuv5y964s4JKloOq5e9t3Kn06JPcN3v65gdJireRRZN6nIb1cM+jwe\nkDkj4MQcXLb65TINH7KFtMWKsUOpXcjcXAKjY7DCQQKBgQDNOATUk2nwOOTdnyCL\ne63P+3vR/P2ePjCjoVjcDHs05ShdumhlRtmztFBptLgB1m/qCiVr5byz8V8UgNS+\nnT5stzvMQD6hPhtk2GwyLxa+mHMU72/jyXBJyb25sEq7Br/BcJ5vms78SA4EAUs+\n5g/KCslVEWKRrTLjCc+uAE4aHQKBgQC6+ZocJXK+sHmEkJi/ZQFatCBc6JnJ5TeL\nI46ucASqq92tmpvWiezMcPxbNPuhNEweQVgZYvN9RSGZ4ojpzfjEHxXf7/+5JED6\nlIfuYr5rFn/aDwfDLLFiWJ/MC6vu3q0Ol0tPj/5F26v87y80bMGYPEWJJwf2nmic\n/F4Ru9X8wQKBgBQYBRkPahMUbwxoNVaTAJzZwfD4tZiV/Es5VCsPqcyvF3m989Te\n0BgxcqI3CMFdTs36ullLQGPaIXYveyVC3kbk1h0UuU0ueJ4yNr3fMHnvEW2eWDVU\nlwMSltoksdyIN6RoM4s3/EZlg8HOoqCBBFDDJ52Fu0IKVG44mrYt33fdAoGBAKzK\nQI5kWS86fYJx8odl+6NsNUBHQOBg8TynhpNUOCvVmo52BDB21Bx3Ce/r/eMTJokL\nUDRdyrFo8s14mZigXZY5OAj0jXn2tAoeu6QlIt5qM4s8Oqs3IVLGnh5+ZszggkOq\ns9F6O70pbj7Yd+JV428hvA0sweI6sGjrnK98zBvBAoGBAIiX616/eWRbzcCjKZeA\n5194LG0GvxSg51sXxU3Kl7GLCzepPW3RBXCS0+z/bp9abzXq95Kigkll94Euvxej\n0q4Ua21g5mcLm0kL72Sn18/B9VCwLlOMwZ+riq9hNYRyobLpb/+e8kTE16Ir8H0j\nAtD/baDIo1+XtD3UdaVBtjHm\n-----END PRIVATE KEY-----\n",
  "client_email": "datamaster-434121@appspot.gserviceaccount.com",
  "client_id": "114142912056624525312",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/datamaster-434121%40appspot.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}


# COMMAND ----------

import json

json_string = json.dumps(json_data, indent=4)

# Criar o diretório no DBFS se ele não existir
dbutils.fs.mkdirs("/FileStore/keyfiles/")

# Caminho onde o arquivo será salvo no DBFS
file_path = "/FileStore/keyfiles/your_key.json"

# Escrever a string JSON no arquivo usando dbutils.fs.put
dbutils.fs.put(file_path, json_string, overwrite=True)

print(f"Arquivo JSON salvo em {file_path}")

# Listar arquivos na pasta onde o arquivo foi salvo
dbutils.fs.ls("/FileStore/keyfiles/")

# COMMAND ----------

file_contents = dbutils.fs.head("dbfs:/FileStore/keyfiles/your_key.json")
print(file_contents)
