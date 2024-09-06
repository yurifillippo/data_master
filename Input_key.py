# Databricks notebook source
json_data = {
  "type": "service_account",
  "project_id": "datamaster01",
  "private_key_id": "11d1b079162364785491ad2d770c8b6ff390a2cb",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDLZBGu1fibmnOB\noN7WuKWuhcN/1ie9Qn1zfJSdApKehF92iU60lo669WqeOxvGUw/0M7qxhcjGdkuB\nlmB/FB5P0vH5mzDXUiSt/KVNMtujdL7axbR/hvQZ/HwkHwdLZH6JxC2DNTkI/BEB\nEPvNcTZ+12V+enVR+lv7wlaea8OQFew8VJuSibBg54A7pEKoz1Wt3UwH3EnadyMl\nUQWveJGrKGEvJFOpAKDrvhrFSdKbcFT7K66GXNOmsQRfG5yfkx1XnTEcdA7iTtNJ\naz+mfjPMNGQD5GUa/jw8u7TK8XvBNtxTfZH90+hojGZRuqY1wUoAWK2CKmNtev+7\n2/i3tWM9AgMBAAECggEAM3e3Ic7zgiyrXfofaaaABpHCzu6aT8IrjLXRxYmJIKPO\npGhKilgu9hB0UmYSuTT7rIgKjjGUhOQaZ0huUrn8kaaHpOooidQ3g++SxN5BxjMc\nuK+e5UZ5Lro9j8ZqSiG8A5CpE7K8JBHlG6f4kBw99gr7m+3RElWQQT0EPkJ20lbG\nJ1LgB6UzS1IGQOvpDZR2JmW80X749eQ+iYh6JJysNag3MDaLbMopVK+oFPjsintZ\nfSfJLOZMJceOv0dXFF0ij16cVdToVybH3K2Ahbbn8L4CFpIfSaG/BWcdXCjt5JLv\nEi6mh5Jr/GilGMrf19uNsaMIkytZR01Ldaqubr2t4QKBgQDyUdvp2ieG+vYvVrch\njhCyBVMGXRG8DwGL9qMbIdLYnQz9BOXCYHiubEHPjj8NLVkn/6GKnedxa4vn+phj\n4VGMP9fz2JyvTWPQxfX7mfhSBlSXGxiHvHGcJK226MLpySiOfF81MAH7xwdXOEwc\nFCgxM63hjr5jTveBDGXJuazs+wKBgQDW35aW4iqrDqzA20Kt3OnN2UJ5LOTR4zaK\nfzYx+heR/tlfGF973h4kp1u93jxEssEKRfxliKLvsMaxV7LqRKymwgxv86zrj0A8\nIZG2WnShEXu6UJasKU7eeIf1rrZBvMRGmUi1n19Esvv1g6LEYKZ4y+o82cxhJLXW\nUcJwTEOLJwKBgBSTiOYBpUub8d2xMlnCE37aXuNycbgTWiFFbzI1nA7SvzCZAt2P\njY/aF3iFbqsyx5hS56e6otWJuVaYe+o3TtJm9XJ6WUu1eZ1Xwfx8ZY6phPNT1LBp\n+we0QZ//gdWSRERdIUSF8BUuIFxyplvYBlWvOrulgFs0cGXf5KDNiHaTAoGAOTZr\neF1cExPjN66qAHUz49WXd4BWpPQkz5ezrHb007DR3Bo3QloQGJ5fNRz5WyllcQDV\nhYtuJvggz5OUVgBXIEfG3AA76LTJ27jsfWt0ZnarRR60H78X+vCI8wfEC5jhghLK\nY2G+EiK+J88XBcbeTIevJOixHKSTri8y4IfN7D0CgYEAxf8NTOjUrBIKkUoMhyQC\nxYn1JdPkrBmvGIYch/0BRy63Yh8pRdLxN5LqPVoK3clu1WHb7dnDG5vThFoH2G0S\nruRrosjSvPrdO8XId6I0k6uUyCnXY000E9FTzdx8Xr/8BwRlMLEFnZF/BEoIDdyQ\nX+k8xekoCljXaM+pgO02DkY=\n-----END PRIVATE KEY-----\n",
  "client_email": "data-master-account@datamaster01.iam.gserviceaccount.com",
  "client_id": "115579238823557687236",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/data-master-account%40datamaster01.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

#####################################################

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


#####################################################


file_contents = dbutils.fs.head("dbfs:/FileStore/keyfiles/your_key.json")
print(file_contents)
