import os
import json
import tempfile
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import datetime
import tarfile

# Control flag
runGcloud = True

# Configuration
REGION = "europe-west2"
REPO = "looker-files"
IMAGE_NAME = "daily_leavers_download"
JOB_NAME = "daily-leavers-download"
SOURCE_TAR = "source.tar.gz"


def googleAuth():
    try:
        # 1. Try Application Default Credentials (Cloud Run)
        credentials, project_id = default()
        print("✅ Authenticated with ADC")
        return credentials, project_id

    except DefaultCredentialsError:
        print("⚠️ ADC not available, trying GOOGLE_CLOUD_SECRET env var...")

        # 2. Codespaces (secret stored in env var)
        secret_json = os.getenv('GOOGLE_CLOUD_SECRET')
        if secret_json:
            service_account_info = json.loads(secret_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            project_id = service_account_info.get('project_id')
            print("✅ Authenticated with service account from env var")
            return credentials, project_id

        # 3. Local dev (service account file path)
        file_path = os.getenv("GCP")
        if file_path and os.path.exists(file_path):
            credentials = service_account.Credentials.from_service_account_file(file_path)
            with open(file_path) as f:
                project_id = json.load(f).get("project_id")
            print("✅ Authenticated with service account from file")
            return credentials, project_id

        raise Exception("❌ No valid authentication method found")

# Step 1: Package source code
def create_tarball():
    print("📦 Creating tarball with Python tarfile module...")
    with tarfile.open(SOURCE_TAR, "w:gz") as tar:
        for root, dirs, files in os.walk("."):
            if "__pycache__" in root or ".git" in root:
                continue
            for file in files:
                if file.endswith(".tar.gz"):
                    continue
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, ".")
                tar.add(filepath, arcname=arcname)
    print(f"✅ Created {SOURCE_TAR}")

# Step 2: Upload to GCS
def upload_source(credentials, bucket_name):
    print("📤 Uploading tarball to GCS...")
    storage = build("storage", "v1", credentials=credentials)
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
    object_name = f"cloudbuild/source-{timestamp}.tar.gz"

    media = MediaFileUpload(SOURCE_TAR, resumable=True)
    request = storage.objects().insert(bucket=bucket_name, name=object_name, media_body=media)
    request.execute()
    print("✅ Tarball uploaded to gcs bucket")
    return object_name

# Step 3: Trigger Cloud Build
def trigger_cloud_build(credentials, project_id, object_name, bucket_name, tag):
    print("🔨 Triggering Cloud Build...")
    cloudbuild = build("cloudbuild", "v1", credentials=credentials)

    build_request = {
        "source": {
            "storageSource": {
                "bucket": bucket_name,
                "object": object_name,
            }
        },
        "steps": [
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": ["build", "-t", tag, "."]
            },
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": ["push", tag]
            }
        ],
        "images": [tag]
    }

    build_op = cloudbuild.projects().builds().create(projectId=project_id, body=build_request).execute()
    print("✅ Cloud Build started. Build ID:", build_op["metadata"]["build"]["id"])

def _update_containers(job, tag, project_id):
    """Navigate the job structure, inject image + PROJECT_ID env var, return updated job or None."""
    def inject(containers):
        containers[0]["image"] = tag
        containers[0].setdefault("env", [])
        env_vars = {env["name"]: env for env in containers[0]["env"]}
        env_vars["PROJECT_ID"] = {"name": "PROJECT_ID", "value": project_id}
        containers[0]["env"] = list(env_vars.values())

    # v2 spec paths
    if "spec" in job:
        try:
            template = job["spec"]["template"]
            if "spec" in template and "template" in template["spec"]:
                inject(template["spec"]["template"]["spec"]["containers"])
                print("✅ Updated using v2 spec path")
                return job
        except KeyError as e:
            print(f"Failed v2 spec path: {e}")
        try:
            template = job["spec"]["template"]
            if "template" in template:
                inject(template["template"]["spec"]["containers"])
                print("✅ Updated using alternative v2 spec path")
                return job
        except KeyError as e:
            print(f"Failed alternative v2 spec path: {e}")

    # v1 template path
    if "template" in job:
        try:
            template = job["template"]
            if "template" in template and "containers" in template["template"]:
                inject(template["template"]["containers"])
                print("✅ Updated using v1 template path")
                return job
        except KeyError as e:
            print(f"Failed v1 template path: {e}")

    print("❌ Could not find container image path in job structure")
    print("Full job structure:")
    print(json.dumps(job, indent=2))
    return None

# Step 4: Update Job (without running)
def update_job_only(credentials, project_id, tag):
    run_client = build("run", "v2", credentials=credentials)
    name = f"projects/{project_id}/locations/{REGION}/jobs/{JOB_NAME}"

    try:
        job = run_client.projects().locations().jobs().get(name=name).execute()
        job = _update_containers(job, tag, project_id)
        if job is None:
            return

        run_client.projects().locations().jobs().patch(name=name, body=job).execute()
        print("✅ Cloud Run job updated (not executed)")

    except Exception as e:
        print(f"❌ Error updating job: {e}")
        raise

# Step 5: Update and Run Job
def update_and_run_job(credentials, project_id, tag):
    run_client = build("run", "v2", credentials=credentials)
    name = f"projects/{project_id}/locations/{REGION}/jobs/{JOB_NAME}"

    try:
        job = run_client.projects().locations().jobs().get(name=name).execute()
        print("🔍 Job structure keys:", list(job.keys()))
        job = _update_containers(job, tag, project_id)
        if job is None:
            return

        run_client.projects().locations().jobs().patch(name=name, body=job).execute()
        print("✅ Cloud Run job updated")

        response = run_client.projects().locations().jobs().run(name=name, body={}).execute()
        print("🚀 Job execution started.")
        execution_name = response.get("metadata", {}).get("name", "")
        if execution_name:
            print(f"📋 Execution name: {execution_name.split('/')[-1]}")
            print(f"🔗 Logs: {response.get('metadata', {}).get('logUri', 'N/A')}")
        print(json.dumps(response, indent=2))

    except Exception as e:
        print(f"❌ Error updating/running job: {e}")
        raise


if __name__ == "__main__":
    credentials, project_id = googleAuth()

    assert project_id, "Could not determine PROJECT_ID from credentials"

    BUCKET_NAME = f"gcf-artifacts-{project_id}"
    TAG = f"{REGION}-docker.pkg.dev/{project_id}/{REPO}/{IMAGE_NAME}:latest"

    create_tarball()
    object_name = upload_source(credentials, BUCKET_NAME)
    trigger_cloud_build(credentials, project_id, object_name, BUCKET_NAME, TAG)

    if runGcloud:
        update_and_run_job(credentials, project_id, TAG)
    else:
        update_job_only(credentials, project_id, TAG)