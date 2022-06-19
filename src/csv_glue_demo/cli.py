import boto3
from botocore.exceptions import ClientError
import glob
import os
import typer

app = typer.Typer(
    help="""csv glue cli to manage demo steps.""",
    no_args_is_help=True,
    add_completion=False,
)

# the top level location for sample data files
sample_data_path = "sample-data"


@app.command()
def upload_csv_files(
    day: int = typer.Argument(...),
    bucket: str = typer.Option(
        ..., "--bucket", "-b", help="target data bucket name", envvar="AWS_BUCKET"
    ),
) -> None:
    """load a 'day' of csv data to the s2 partition"""
    data_path = f"./data/csv-sample/p_day={day}"
    object_path = f"{sample_data_path}/csv-sample/p_day={day}"
    typer.secho(f"Loading csv data: p_day={day} to {object_path}/")

    files = glob.glob(f"{data_path}/*.csv")

    s3_client = boto3.client("s3")
    # upload file to s3
    for f in files:
        filename = os.path.basename(f)
        try:
            resp = s3_client.upload_file(f, bucket, f"{object_path}/{filename}")
            typer.secho(f"  uploaded: s3://{bucket}/{object_path}/{filename}")
        except ClientError as err:
            typer.secho(f">> error: {err}", err=True)
            return


@app.command()
def reset_s3_files(
    bucket: str = typer.Option(
        ..., "--bucket", "-b", help="target data bucket name", envvar="AWS_BUCKET"
    ),
    confirm: bool = typer.Option(
        ..., prompt=f"Remove all files from s3 bucket?", confirmation_prompt=True
    ),
) -> None:
    """reset the s3 data in the bucket"""
    if confirm == False:
        typer.secho(
            f"Skipping s3 file delete from s3://{bucket}/{sample_data_path}/",
            fg="yellow",
        )
        raise typer.Exit()

    s3_client = boto3.client("s3")

    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=sample_data_path)
    if 'Contents' in objects:
        typer.secho(f"Removing files in bucket s3://{bucket}/{sample_data_path}/", fg="yellow")
        print(f"objects: {objects}")
        for obj in objects['Contents']:
            typer.secho(f"  deleting: {obj['Key']}", fg="yellow")
            s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
    else:
        typer.secho(f"No files found in bucket s3://{bucket}/{sample_data_path}/", fg="yellow")