# Hive Glue Catalog Sync Agent

The Hive Glue Catalog Sync Agent is a software module that can be installed and configured within a Hive Metastore server, and provides outbound synchronisation to the [AWS Glue Data Catalog](https://aws.amazon.com/glue) for tables stored on Amazon S3. This enables you to seamlessly create objects on the AWS Catalog as they are created within your existing Hadoop/Hive environment without any operational overhead or tasks.

This project provides a jar that implements the [MetastoreEventListener](https://hive.apache.org/javadocs/r1.2.2/api/org/apache/hadoop/hive/metastore/MetaStoreEventListener.html) interface of Hive to capture all create and drop events for tables and partitions in your Hive Metastore. It then connects to Amazon Athena in your AWS Account, and runs these same commands against the Glue Catalog, to provide syncronisation of the catalog over time. Your Hive Metastore and Yarn cluster can be anywhere - on the cloud or on your own data center.

![architecture](architecture.png)

Within the [HiveGlueCatalogSyncAgent](src/main/java/com/amazonaws/services/glue/catalog/HiveGlueCatalogSyncAgent.java), the DDL from metastore events is captured and written to a ConcurrentLinkedQueue. 

![internals](internals.png)

This queue is then drained by a separate thread that writes ddl events to Amazon Athena via a JDBC connection. This architecture ensures that if your Yarn cluster becomes disconnected from the Cloud for some reason, that Catalog events will not be dropped.

## Supported Events

Today the Catalog Sync Agent supports the following MetaStore events:

* CreateTable
* AddPartition
* DropTable
* DropPartition

## Installation

You can build the software yourself by configuring Maven and issuing `mvn package`, which will result in the binary being built to `aws-glue-catalog-sync-agent/target/HiveGlueCatalogSyncAgent-1.3.2-SNAPSHOT.jar`, or alternatively you can download the jar from [s3://awslabs-code-us-east-1/HiveGlueCatalogSyncAgent/HiveGlueCatalogSyncAgent-1.3.2-SNAPSHOT.jar](https://s3.amazonaws.com/awslabs-code-us-east-1/HiveGlueCatalogSyncAgent/HiveGlueCatalogSyncAgent-1.3.2-SNAPSHOT.jar). You can also run `mvn assembly:assembly`, which generates a mega jar including dependencies `aws-glue-catalog-sync-agent/target/HiveGlueCatalogSyncAgent-1.3.2-SNAPSHOT-complete.jar` also found [here](https://s3.amazonaws.com/awslabs-code-us-east-1/HiveGlueCatalogSyncAgent/HiveGlueCatalogSyncAgent-1.3.2-SNAPSHOT-complete.jar).

## Required Dependencies

If you install the base `HiveGlueCatalogSyncAgent-1.3.2-SNAPSHOT.jar` jar into your cluster, you must also ensure you have the following dependencies available:

* SLF4J (1.7.36+)
* Logback (1.2.3+)
* AWS Java SDK Core, Cloudwatch Logs (1.12.177+)
* org.antlr.stringtemplate (4.0.2)
* Hive Metastore (3.1.2)
* Hive Common (3.1.2)
* Hive Exec (3.1.2)

## Configuration Instructions
# S3 
Create or decide on a bucket (and a prefix) where results from Athena will be stored. You'll need to update the below IAM policy with the designated bucket.

# IAM

First, Create a new IAM policy with the following permissions (update the policy with your bucket):
	
````json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:CreateDatabase",
                "glue:DeleteDatabase",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:UpdateDatabase",
                "glue:CreateTable",
                "glue:DeleteTable",
                "glue:BatchDeleteTable",
                "glue:UpdateTable",
                "glue:GetTable",
                "glue:GetTables",
                "glue:BatchCreatePartition",
                "glue:CreatePartition",
                "glue:DeletePartition",
                "glue:BatchDeletePartition",
                "glue:UpdatePartition",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:BatchGetPartition"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "athena:*",
                "logs:CreateLogGroup"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:ListMultipartUploadParts",
                "s3:AbortMultipartUpload",
                "s3:CreateBucket",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::<my-bucket>",
                "arn:aws:s3:::<my-bucket>/*"
            ]
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:log-group:HIVE_METADATA_SYNC:*:*"
        }
    ]
}
````

Then:

 1. Create an IAM role and attach the policy to it
 2. If your Hive Metastore runs on EC2, attach the IAM Role to this instance. Otherwise, create an IAM user and generate an access and secret key.

# Hive Configuration
Add the following keys to hive-site-xml:

- `hive.metastore.event.listeners` - com.amazonaws.services.glue.catalog.HiveGlueCatalogSyncAgent
 - `glue.catalog.athena.jdbc.url` - The url to use to connect to Athena (default: `jdbc:awsathena://athena.**us-east-1**.amazonaws.com:443`) 
 - `glue.catalog.athena.s3.staging.dir` - The bucket & prefix used to store Athena's query results
- `glue.catalog.user.key` - If not using an instance attached IAM role, the IAM access key.
- `glue.catalog.user.secret` - If not using an instance attached IAM role, the IAM access secret.
- `glue.catalog.dropTableIfExists` - Should an already existing table be dropped and created (default: true)
- `glue.catalog.createMissingDB` - Should DBs be created if they don't exist (default:true)
- `glue.catalog.athena.suppressAllDropEvents` - prevents propagation of DropTable and DropPartition events to the remote environment


Add the Glue Sync Agent's jar to HMS' classpath and restart.

You should see newly created external tables and partitions replicated to Glue Data Catalog and logs in CloudWatch Logs.

## Integration Tests

The project includes end-to-end integration tests that provision an EMR cluster with a standalone Hive Metastore, run catalog operations via Spark, sync to the Glue Data Catalog, and validate the results using Athena.

### What the tests cover

- Hive external tables (Parquet, ORC, CSV) with partitions, schema changes, and drops
- Iceberg tables (unpartitioned, partitioned, schema evolution, snapshot operations)
- Iceberg-specific validation: `table_type=ICEBERG`, `metadata_location` with `s3://` prefix, Athena readability
- Table ownership filtering, conflict handling, and batch operations
- Full list of scenarios in `integration-tests/test-scenarios.md`

### Prerequisites

- AWS CLI configured with credentials that can create EMR clusters, IAM roles, Glue catalog entries, and run Athena queries
- Python 3 with `boto3` installed (for the validation script)
- A VPC subnet ID where the EMR cluster will be launched (must have connectivity to AWS service endpoints — see `integration-tests/README.md` for details)
- An S3 bucket for test data, EMR logs, and the sync agent JAR
- If using Lake Formation: the EMR EC2 instance profile role needs Database Creator and Data Location permissions (see `integration-tests/README.md` for details)

### Running via Maven

The integration tests are behind a Maven profile and won't run during normal builds. To run them:

```bash
# Build the project first (produces the fat JAR)
mvn clean package

# Run integration tests
mvn verify -Pintegration-test \
  -Dtest.s3.bucket=my-test-bucket \
  -Dtest.subnet.id=subnet-0abc123def456
```

Optional parameters:

| Property | Default | Description |
|---|---|---|
| `test.aws.region` | `us-east-1` | AWS region for EMR and Glue |
| `test.emr.release` | `emr-7.1.0` | EMR release label |
| `test.catalog.id` | *(account default)* | Glue Catalog ID |

### What happens during the run

1. The sync agent fat JAR, bootstrap script, and SQL step scripts are uploaded to S3
2. A CloudFormation stack (`integration-tests/cfn/emr-integ-test.yaml`) creates an EMR cluster with:
   - Standalone HMS (Derby-backed, not Glue as the metastore)
   - A bootstrap action that installs the sync agent JAR into `/usr/lib/hive/auxlib/`
   - `hive.metastore.event.listeners` configured to the sync agent class
   - Iceberg support enabled via `spark-defaults` and `iceberg-defaults`
   - 60-second idle auto-termination so the cluster shuts down shortly after steps complete
3. Spark SQL steps run on the cluster to create tables, insert data, alter schemas, and drop tables
4. The sync agent (running as an HMS listener) pushes events to the Glue Data Catalog
5. A Python validation script checks every synced table via the Glue API and runs Athena queries to confirm readability
6. On exit (pass or fail), the cleanup routine deletes the CloudFormation stack, Glue catalog entries, and S3 test data

### Running standalone (without Maven)

You can also run the tests directly if you already have a cluster or want more control:

```bash
cd integration-tests

# If you already have an EMR cluster with the sync agent installed:
export EMR_CLUSTER_ID=j-XXXXXXXXXXXXX
export TEST_S3_BUCKET=my-test-bucket
./run-tests.sh

# Or run the full lifecycle (provision → test → teardown):
export TEST_S3_BUCKET=my-test-bucket
export SUBNET_ID=subnet-0abc123def456
./run-integ-tests.sh
```

### Project structure

```
integration-tests/
├── cfn/
│   └── emr-integ-test.yaml          # CloudFormation template for EMR cluster
├── emr-steps/
│   ├── 01_create_hive_tables.sql     # Parquet, ORC, CSV external tables
│   ├── 02_create_iceberg_tables.sql  # Iceberg table scenarios
│   ├── 03_alter_and_drop.sql         # ALTER TABLE, DROP PARTITION
│   └── 04_drop_table.sql            # DROP TABLE (runs after sync window)
├── scripts/
│   └── bootstrap-install-agent.sh    # EMR bootstrap to install the JAR
├── validate/
│   └── validate_sync.py             # GDC metadata + Athena readability checks
├── run-integ-tests.sh               # Full lifecycle orchestrator
├── run-tests.sh                     # Step submission + validation (existing cluster)
├── test-scenarios.md                # Detailed test case descriptions
└── README.md                        # Integration test documentation
```

----

Apache 2.0 Software License

see [LICENSE](LICENSE) for details

