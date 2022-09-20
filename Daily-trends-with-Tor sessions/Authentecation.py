from google.oauth2 import service_account
from google.cloud.storage import client

service_account_info = {
      "type": "service_account",
      "project_id": "",
      "private_key_id": "",
      "private_key": "",
      "client_email": "",
      "client_id": "",
      "auth_uri": "",
      "token_uri": "",
      "auth_provider_x509_cert_url": "",
      "client_x509_cert_url": ""
    }

Tor_password = "Add your tor password"

def google_credit():
  credentials = service_account.Credentials.from_service_account_info(service_account_info)

  clientele = client.Client(
      credentials=credentials,
      project=credentials.project_id,
  )
  return clientele

def save_file(local_filename, remote_filename,client, BUCKET):
  bucket = client.get_bucket(BUCKET)
  blob = bucket.blob(remote_filename)
  blob.upload_from_filename(local_filename)