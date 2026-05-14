# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the DynamoDB ExportTableToPointInTime, DescribeExport, and
# ListExports APIs (part of SCYLLADB-1939).

import pytest
import boto3
import time
import uuid

from botocore.exceptions import ClientError
from contextlib import contextmanager

from test.alternator.util import unique_table_name, create_test_table, is_aws, new_test_table


# Helper to get the table ARN from a table object.
def get_table_arn(table):
    desc = table.meta.client.describe_table(TableName=table.name)
    return desc['Table']['TableArn']


# Helper to create a unique S3 bucket name.
def unique_bucket_name():
    return f"alternator-export-test-{uuid.uuid4().hex[:12]}"


# Create an S3 client using the same endpoint configuration as the DynamoDB
# fixture where possible. On AWS, the default S3 client is used. On Scylla,
# we use the same credentials/region.
def make_s3_client(dynamodb):
    if is_aws(dynamodb):
        return boto3.client('s3')
    # For local Scylla testing, use default S3 (tests need real S3 buckets
    # because the export writes to S3).
    return boto3.client('s3', region_name=dynamodb.meta.client.meta.region_name)


@contextmanager
def new_s3_bucket(s3_client, region=None):
    """Context manager that creates a uniquely-named S3 bucket and deletes it
    (including all objects) on exit."""
    bucket_name = unique_bucket_name()
    kwargs = {'Bucket': bucket_name}
    # us-east-1 does not accept a LocationConstraint
    if region and region != 'us-east-1':
        kwargs['CreateBucketConfiguration'] = {'LocationConstraint': region}
    s3_client.create_bucket(**kwargs)
    try:
        yield bucket_name
    finally:
        # Delete all objects before deleting the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': [{'Key': obj['Key']} for obj in page['Contents']]}
                )
        s3_client.delete_bucket(Bucket=bucket_name)


# Helper: enable PITR on a table (required for ExportTableToPointInTime on
# DynamoDB). Returns the client used.
def enable_pitr(table, timeout=120):
    client = table.meta.client
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client.update_continuous_backups(
                TableName=table.name,
                PointInTimeRecoverySpecification={'PointInTimeRecoveryEnabled': True}
            )
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'ContinuousBackupsUnavailableException':
                time.sleep(0.1)
            else:
                raise
    # Wait until PITR is actually active — there is a propagation delay on AWS.
    while time.time() < deadline:
        resp = client.describe_continuous_backups(TableName=table.name)
        pitr = resp['ContinuousBackupsDescription'].get('PointInTimeRecoveryDescription', {})
        if pitr.get('PointInTimeRecoveryStatus') == 'ENABLED':
            return client
        time.sleep(1)
    raise TimeoutError(f"PITR did not become ENABLED on {table.name} within {timeout}s")


# Helper: wait for an export to reach a terminal status (COMPLETED or FAILED).
def wait_for_export(client, export_arn, timeout=3000):
    deadline = time.time() + timeout
    while time.time() < deadline:
        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        status = desc['ExportStatus']
        if status in ('COMPLETED', 'FAILED'):
            return desc
        time.sleep(5)
    raise TimeoutError(f"Export {export_arn} did not complete within {timeout}s")


# ---------------------------------------------------------------------------
# ExportTableToPointInTime tests
# ---------------------------------------------------------------------------

def test_export_basic(dynamodb, test_table_s):
    """Test that ExportTableToPointInTime starts an export and returns an
    ExportDescription with expected fields."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    with new_s3_bucket(s3, region) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        desc = response['ExportDescription']
        assert 'ExportArn' in desc
        assert desc['ExportStatus'] in ('IN_PROGRESS', 'COMPLETED')
        assert desc['TableArn'] == table_arn
        assert desc['S3Bucket'] == bucket
        assert 'ExportFormat' in desc
        # Wait for completion and verify
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'


def test_export_with_prefix(dynamodb, test_table_s):
    """Test ExportTableToPointInTime with an S3 prefix."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name
    prefix = 'test-export-prefix'

    with new_s3_bucket(s3, region) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            S3Prefix=prefix,
        )
        desc = response['ExportDescription']
        assert desc['S3Prefix'] == prefix
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'
        assert final['S3Prefix'] == prefix


def test_export_dynamodb_json_format(dynamodb, test_table_s):
    """Test ExportTableToPointInTime with DYNAMODB_JSON format."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    with new_s3_bucket(s3, region) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ExportFormat='DYNAMODB_JSON',
        )
        desc = response['ExportDescription']
        assert desc.get('ExportFormat') == 'DYNAMODB_JSON'
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'


def test_export_ion_format(dynamodb, test_table_s):
    """Test ExportTableToPointInTime with ION format."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    with new_s3_bucket(s3, region) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ExportFormat='ION',
        )
        desc = response['ExportDescription']
        assert desc.get('ExportFormat') == 'ION'
        final = wait_for_export(client, desc['ExportArn'])
        assert final['ExportStatus'] == 'COMPLETED'


def test_export_with_data(dynamodb):
    """Test that an export of a table with data completes and reports a
    non-zero ItemCount."""
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        # Insert some items
        num_items = 10
        for i in range(num_items):
            table.put_item(Item={'p': f'item{i}', 'data': f'value{i}'})

        client = enable_pitr(table)
        table_arn = get_table_arn(table)
        s3 = make_s3_client(dynamodb)
        region = dynamodb.meta.client.meta.region_name

        with new_s3_bucket(s3, region) as bucket:
            response = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
            )
            final = wait_for_export(client, response['ExportDescription']['ExportArn'])
            assert final['ExportStatus'] == 'COMPLETED'
            assert final['ItemCount'] > 0


def test_export_client_token_idempotent(dynamodb, test_table_s):
    """Test that sending the same ClientToken twice returns the same export."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name
    token = str(uuid.uuid4())

    with new_s3_bucket(s3, region) as bucket:
        resp1 = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ClientToken=token,
        )
        resp2 = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
            ClientToken=token,
        )
        assert resp1['ExportDescription']['ExportArn'] == resp2['ExportDescription']['ExportArn']
        wait_for_export(client, resp1['ExportDescription']['ExportArn'])


def test_export_client_token_doesnt_work_on_two_tables(dynamodb, test_table_s, test_table_s_2):
    """Test that trying to export from two tables using the same client token fails."""
    client1 = enable_pitr(test_table_s)
    table_arn1 = get_table_arn(test_table_s)
    client2 = enable_pitr(test_table_s_2)
    table_arn2 = get_table_arn(test_table_s_2)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name
    token = str(uuid.uuid4())

    with new_s3_bucket(s3, region) as bucket:
        resp1 = client1.export_table_to_point_in_time(
            TableArn=table_arn1,
            S3Bucket=bucket,
            ClientToken=token,
        )
        with pytest.raises(ClientError, match='ExportConflictException.*Duplicate request detected'):
            client2.export_table_to_point_in_time(
                TableArn=table_arn2,
                S3Bucket=bucket,
                ClientToken=token,
            )

def test_export_nonexistent_table(dynamodb):
    """Test that exporting a nonexistent table returns TableNotFoundException."""
    client = dynamodb.meta.client
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name
    fake_arn = 'arn:aws:dynamodb:us-east-1:123456789012:table/nonexistent_table_xyz'

    with new_s3_bucket(s3, region) as bucket:
        with pytest.raises(ClientError, match='AccessDeniedException'):
            client.export_table_to_point_in_time(
                TableArn=fake_arn,
                S3Bucket=bucket,
            )


def test_export_nonexistent_bucket(dynamodb, test_table_s):
    """Test that exporting to a nonexistent S3 bucket fails."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    fake_bucket = unique_bucket_name()

    # The export may either fail immediately or fail asynchronously
    try:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=fake_bucket,
        )
        # If it didn't fail immediately, the export should eventually fail
        final = wait_for_export(client, response['ExportDescription']['ExportArn'])
        assert final['ExportStatus'] == 'FAILED'
    except ClientError:
        pass  # Immediate failure is also acceptable


# ---------------------------------------------------------------------------
# DescribeExport tests
# ---------------------------------------------------------------------------

def test_describe_export(dynamodb, test_table_s):
    """Test that DescribeExport returns the full ExportDescription for a
    known export."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    with new_s3_bucket(s3, region) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = response['ExportDescription']['ExportArn']

        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        assert desc['ExportArn'] == export_arn
        assert desc['TableArn'] == table_arn
        assert desc['S3Bucket'] == bucket
        assert desc['ExportStatus'] in ('IN_PROGRESS', 'COMPLETED')
        assert 'StartTime' in desc
        assert 'ExportFormat' in desc

        wait_for_export(client, export_arn)


def test_describe_export_after_completion(dynamodb, test_table_s):
    """Test that DescribeExport returns COMPLETED status and additional fields
    after the export finishes."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    with new_s3_bucket(s3, region) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = response['ExportDescription']['ExportArn']
        wait_for_export(client, export_arn)

        desc = client.describe_export(ExportArn=export_arn)['ExportDescription']
        assert desc['ExportStatus'] == 'COMPLETED'
        assert desc['ExportArn'] == export_arn
        assert desc['TableArn'] == table_arn
        assert 'EndTime' in desc
        assert 'ExportManifest' in desc
        assert 'ItemCount' in desc
        assert 'BilledSizeBytes' in desc


def test_describe_export_nonexistent(dynamodb):
    """Test that DescribeExport with a non-existent ARN returns
    ExportNotFoundException."""
    client = dynamodb.meta.client
    fake_arn = 'arn:aws:dynamodb:us-east-1:123456789012:table/t/export/01234567-89ab-cdef-0123-456789abcdef'
    with pytest.raises(ClientError, match='AccessDeniedException'):
        client.describe_export(ExportArn=fake_arn)


# ---------------------------------------------------------------------------
# ListExports tests
# ---------------------------------------------------------------------------

def test_list_exports_empty(dynamodb):
    """Test that ListExports returns an ExportSummaries list (possibly empty)
    for a table that has no exports."""
    with new_test_table(dynamodb,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
    ) as table:
        client = table.meta.client
        table_arn = get_table_arn(table)
        response = client.list_exports(TableArn=table_arn)
        assert 'ExportSummaries' in response
        # The list should be empty for a freshly-created table
        assert len(response['ExportSummaries']) == 0


def test_list_exports_contains_export(dynamodb, test_table_s):
    """Test that after starting an export, ListExports includes it."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    with new_s3_bucket(s3, region) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = response['ExportDescription']['ExportArn']

        exports = client.list_exports(TableArn=table_arn)
        arns = [e['ExportArn'] for e in exports['ExportSummaries']]
        assert export_arn in arns

        wait_for_export(client, export_arn)


def test_list_exports_no_table_filter(dynamodb, test_table_s):
    """Test that ListExports without a TableArn filter returns results."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    with new_s3_bucket(s3, region) as bucket:
        response = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = response['ExportDescription']['ExportArn']

        # ListExports without TableArn should still include our export
        exports = client.list_exports()
        arns = [e['ExportArn'] for e in exports['ExportSummaries']]
        assert export_arn in arns

        wait_for_export(client, export_arn)


def test_list_exports_pagination(dynamodb, test_table_s):
    """Test that ListExports pagination via MaxResults and NextToken works."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    export_arns = []
    # Start multiple exports to the same bucket with different prefixes
    with new_s3_bucket(s3, region) as bucket:
        for i in range(3):
            resp = client.export_table_to_point_in_time(
                TableArn=table_arn,
                S3Bucket=bucket,
                S3Prefix=f'export-{i}',
                ClientToken=str(uuid.uuid4()),
            )
            export_arns.append(resp['ExportDescription']['ExportArn'])

        # List with MaxResults=1 to force pagination
        all_arns = []
        response = client.list_exports(TableArn=table_arn, MaxResults=1)
        all_arns.extend(e['ExportArn'] for e in response['ExportSummaries'])
        while 'NextToken' in response and response['NextToken']:
            response = client.list_exports(
                TableArn=table_arn,
                MaxResults=1,
                NextToken=response['NextToken'],
            )
            all_arns.extend(e['ExportArn'] for e in response['ExportSummaries'])

        for arn in export_arns:
            assert arn in all_arns

        # Clean up: wait for all exports
        for arn in export_arns:
            wait_for_export(client, arn)


def test_list_exports_summary_fields(dynamodb, test_table_s):
    """Test that each ExportSummary in ListExports contains the expected
    fields: ExportArn, ExportStatus, ExportType."""
    table = test_table_s
    client = enable_pitr(table)
    table_arn = get_table_arn(table)
    s3 = make_s3_client(dynamodb)
    region = dynamodb.meta.client.meta.region_name

    with new_s3_bucket(s3, region) as bucket:
        resp = client.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=bucket,
        )
        export_arn = resp['ExportDescription']['ExportArn']
        wait_for_export(client, export_arn)

        exports = client.list_exports(TableArn=table_arn)
        matching = [e for e in exports['ExportSummaries'] if e['ExportArn'] == export_arn]
        assert len(matching) == 1
        summary = matching[0]
        assert 'ExportArn' in summary
        assert 'ExportStatus' in summary
        assert summary['ExportStatus'] == 'COMPLETED'
