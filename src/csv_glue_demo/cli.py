import boto3
from botocore.exceptions import ClientError
import glob
import pandas as pd
from pathlib import Path
import typer

app = typer.Typer(
    help="""csv glue cli to manage demo steps.""",
    no_args_is_help=True,
    add_completion=False,
)

def upload_s3_files(bucket, prefix, files):
    """
    Upload a directory of files to the bucket path
    """
    s3_client = boto3.client('s3')
    for f in files:
        filename = Path(f).name
        s3_client.upload_file(f, bucket, f"{prefix}/{filename}")


@app.command()
def upload_data_files(
    day: int = typer.Argument(...),
    bucket: str = typer.Option(
        ..., "--bucket", "-b", help="target data bucket name", envvar="AWS_BUCKET"
    ),
) -> None:
    """load a 'day' of data files to the s3 partition"""
    csv_objects_path = f"sample-data/csv-sample/p_day={day}"
    typer.secho(f"Loading csv data files: p_day={day} to {csv_objects_path}/")
    parquet_objects_path = f"sample-data/parquet-sample/p_day={day}"
    typer.secho(f"Loading parquet data files: p_day={day} to {parquet_objects_path}/")

    csv_files = glob.glob(f"./{csv_objects_path}/*.csv")
    parquet_files = glob.glob(f"./{parquet_objects_path}/*.parquet")

    typer.secho(f"  uploading {len(csv_files)} csv files")
    upload_s3_files(bucket, csv_objects_path, csv_files)
    typer.secho(f"  uploading {len(parquet_files)} parquet files")
    upload_s3_files(bucket, parquet_objects_path, parquet_files)



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
            f"Skipping s3 file delete from s3://{bucket}/sample-data/",
            fg="yellow",
        )
        raise typer.Exit()

    sample_data_prefix = "sample-data"

    s3_client = boto3.client("s3")

    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=sample_data_prefix)
    if 'Contents' in objects:
        typer.secho(f"Removing files in bucket s3://{bucket}/{sample_data_prefix}/", fg="yellow")
        print(f"objects: {objects}")
        for obj in objects['Contents']:
            typer.secho(f"  deleting: {obj['Key']}", fg="yellow")
            s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
    else:
        typer.secho(f"No files found in bucket s3://{bucket}/{sample_data_prefix}/", fg="yellow")


@app.command()
def convert_csv_to_parquet(
    input_path: str = typer.Argument(..., help="input directory containing csv files"),
    output_path: str = typer.Argument(..., help="output directory for writing parquet files")
) -> None:
    """Convert a directory of csv files to corresponding parquet with all fields as strings."""
    typer.secho(f"Converting csv to parquet: input-path: {input_path} | output-path: {output_path}")

    if not Path(input_path).exists():
        typer.secho(f'ERROR: input path not found: "{input_path}"', err=True, fg="red")
        raise typer.Abort()
    if not Path(input_path).is_dir():
        typer.secho(f'ERROR: input path is not a directory: "{input_path}"', err=True, fg="red")
        raise typer.Abort()
    
    # check the output directory already exists
    if not Path(output_path).exists():
        typer.secho(f'ERROR: output path not found: "{Path(output_path)}/"', err=True, fg="red")
        raise typer.Abort()
    if not Path(output_path).is_dir():
        typer.secho(f'ERROR: output path is not a directory: "{output_path}"', err=True, fg="red")
        raise typer.Abort()
    
    in_files = glob.glob(f"{input_path}/*.csv")
    if len(in_files) < 1:
        typer.secho(f"WARN: no csv files found in {input_path}", fg="yellow")

    for in_filename in in_files:
        df = pd.read_csv(in_filename, 
            header=0,
            low_memory=False,
            dtype=str)
        df.fillna('', inplace=True)

        out_filename = f"{output_path}/{Path(in_filename).stem}.parquet"
        df.to_parquet(
            out_filename,
            index=False,
            compression="snappy",
        )
        typer.secho(f" . converted: {in_filename} to {out_filename}")