import io
import json
import oci
import requests
from fdk import response

def handler(ctx, data: io.BytesIO = None):
    signer = oci.auth.signers.get_resource_principals_signer()
    body = json.loads(data.getvalue())
    url = body.get("url")
    method = body.get("method")
    httpbody = body.get("body")
    headers = body.get("headers")
    auth = body.get("auth")
    target_objectname = body.get("target_objectname")
    target_bucket = body.get("target_bucket")
    if (target_bucket == None or target_objectname == None or url == None):
      resp_data = {"status":"400", "info":"Required parameters have not been supplied - target_objectname, target_bucket, url need to be supplied"}
      return response.Response(
            ctx, response_data=resp_data, headers={"Content-Type": "application/json"}
      )

    try:
      object_storage_client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
      namespace = object_storage_client.get_namespace().data

      resp = None
      if auth is not None and auth == "OCI":
        if method == None or method == "GET":
          resp = requests.get(url, auth=signer, headers=headers)
        elif method == "POST":
          resp = requests.post(url, json=httpbody, auth=signer, headers=headers)
        elif method == "PUT":
          resp = requests.post(url, json=httpbody, auth=signer, headers=headers)
      else:
        session = requests.Session()
        if method == None or method == "GET":
          resp = session.get(url, stream=True, headers=headers)
        elif method == "POST":
          resp = session.post(url, json=httpbody, headers=headers, stream=True)
        elif method == "PUT":
          resp = session.put(url, json=httpbody, headers=headers, stream=True)

      with resp as part:
        object_storage_client.put_object(namespace,
                            target_bucket,
                            target_objectname,
                            part.content)

      resp_data = {"status":"200"}
      return response.Response( ctx, response_data=resp_data, headers={"Content-Type": "application/json"})
    except oci.exceptions.ServiceError as inst:
      return response.Response( ctx, response_data=inst, headers={"Content-Type": "application/json"})

