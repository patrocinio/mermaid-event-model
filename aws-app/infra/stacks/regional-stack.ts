// ─────────────────────────────────────────────────────────────
// Shared infrastructure for model: model
// Target: AWS-native CDK — infra/stacks/regional-stack.ts
// Cost tier: MINIMAL — a low-cost footprint for demos/PoCs: one NAT
// gateway and a single-node ElastiCache Redis (cache.t4g.micro, no
// replicas, no Multi-AZ failover). NOT for production.
// The COMMON stack, emitted once from the whole model: VPC, the DynamoDB
// global-table reference, a regional Kinesis stream, ElastiCache Redis,
// the command/query/projector Lambdas, and the API Gateway. Deployed
// identically per region. Source of truth is the model .md — regenerate.
// ─────────────────────────────────────────────────────────────
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as elasticache from 'aws-cdk-lib/aws-elasticache';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as eventsources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as path from 'path';

export interface RegionalStackProps extends cdk.StackProps {
    regionLabel: string;
    globalTable: dynamodb.Table;
    isPrimary: boolean;
    // Name of the SageMaker endpoint automation slices invoke for inference.
    // Optional: when omitted, InvokeEndpoint is granted account-wide and the
    // handler errors at runtime until an endpoint name is supplied.
    sagemakerEndpointName?: string;
}

// Complete infrastructure for one region (deploy to each region for
// active-active). Per-slice handlers live at the entry paths referenced
// below; regenerate a slice with the AWS (CDK/TS) button to fill them in.
export class RegionalStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props: RegionalStackProps) {
        super(scope, id, props);

        // ── Networking (Multi-AZ) ──
        const vpc = new ec2.Vpc(this, 'Vpc', {
            maxAzs: 3,
            natGateways: 1,
            subnetConfiguration: [
                { cidrMask: 24, name: 'Public', subnetType: ec2.SubnetType.PUBLIC },
                { cidrMask: 24, name: 'Private', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
            ],
        });

        const lambdaSg = new ec2.SecurityGroup(this, 'LambdaSg', {
            vpc, description: 'Lambda functions security group', allowAllOutbound: true,
        });
        const redisSg = new ec2.SecurityGroup(this, 'RedisSg', {
            vpc, description: 'ElastiCache Redis security group', allowAllOutbound: false,
        });
        redisSg.addIngressRule(lambdaSg, ec2.Port.tcp(6379), 'Lambda to Redis');

        // ── Event distribution — regional Kinesis stream ──
        const stream = new kinesis.Stream(this, 'EventStream', {
            streamName: `hotel-events-${props.regionLabel}`,
            shardCount: 2,
            retentionPeriod: cdk.Duration.hours(168),
        });

        // ── Read model — Multi-AZ ElastiCache Redis ──
        const subnetGroup = new elasticache.CfnSubnetGroup(this, 'RedisSubnetGroup', {
            description: `Redis subnet group - ${props.regionLabel}`,
            subnetIds: vpc.privateSubnets.map((s) => s.subnetId),
            cacheSubnetGroupName: `hotel-redis-${props.regionLabel}`,
        });
        const redisReplicationGroup = new elasticache.CfnReplicationGroup(this, 'RedisCluster', {
            replicationGroupDescription: `Hotel read model - ${props.regionLabel}`,
            engine: 'redis',
            engineVersion: '7.1',
            cacheNodeType: 'cache.t4g.micro',
            numNodeGroups: 1,
            replicasPerNodeGroup: 0,
            automaticFailoverEnabled: false,
            multiAzEnabled: false,
            cacheSubnetGroupName: subnetGroup.cacheSubnetGroupName,
            securityGroupIds: [redisSg.securityGroupId],
            atRestEncryptionEnabled: true,
            transitEncryptionEnabled: true,
            autoMinorVersionUpgrade: true,
            replicationGroupId: `hotel-cache-${props.regionLabel}`,
        });
        redisReplicationGroup.addDependency(subnetGroup);
        const redisEndpoint = redisReplicationGroup.attrPrimaryEndPointAddress;
        const redisPort = redisReplicationGroup.attrPrimaryEndPointPort;

        // ── Compute — Lambda (ARM64, X-Ray) ──
        const commonProps: Partial<nodejs.NodejsFunctionProps> = {
            runtime: lambda.Runtime.NODEJS_20_X,
            architecture: lambda.Architecture.ARM_64,
            memorySize: 512,
            tracing: lambda.Tracing.ACTIVE,
            bundling: { minify: true, sourceMap: true, target: 'es2022' },
        };

        // ── DCB event-store indexes (required on props.globalTable) ─────────
        // A Dynamic Consistency Boundary is queried by tag value, so the event
        // table must expose one GSI per tag axis used across the model:
        //   • gsi_email: partitionKey = 'tag_email' (string), sortKey = 'seq' (string)
        //   • gsi_roomNumber: partitionKey = 'tag_roomNumber' (string), sortKey = 'seq' (string)
        //   • gsi_bookingId: partitionKey = 'tag_bookingId' (string), sortKey = 'seq' (string)
        //   • gsi_paymentId: partitionKey = 'tag_paymentId' (string), sortKey = 'seq' (string)
        //   • gsi_roomType: partitionKey = 'tag_roomType' (string), sortKey = 'seq' (string)
        // The table's own key schema is: partitionKey = 'eventId' (string),
        // sortKey = 'seq' (string). Guard items reuse it: eventId = 'TAGPOS#<axis>#<value>',
        // seq = 'POS'. Declare these GSIs where props.globalTable is created; add
        // them with addGlobalSecondaryIndex(...) if the table is defined in this app:
        /*
          props.globalTable.addGlobalSecondaryIndex({
            indexName: 'gsi_email',
            partitionKey: { name: 'tag_email', type: dynamodb.AttributeType.STRING },
            sortKey: { name: 'seq', type: dynamodb.AttributeType.STRING },
            projectionType: dynamodb.ProjectionType.ALL,
          });
          props.globalTable.addGlobalSecondaryIndex({
            indexName: 'gsi_roomNumber',
            partitionKey: { name: 'tag_roomNumber', type: dynamodb.AttributeType.STRING },
            sortKey: { name: 'seq', type: dynamodb.AttributeType.STRING },
            projectionType: dynamodb.ProjectionType.ALL,
          });
          props.globalTable.addGlobalSecondaryIndex({
            indexName: 'gsi_bookingId',
            partitionKey: { name: 'tag_bookingId', type: dynamodb.AttributeType.STRING },
            sortKey: { name: 'seq', type: dynamodb.AttributeType.STRING },
            projectionType: dynamodb.ProjectionType.ALL,
          });
          props.globalTable.addGlobalSecondaryIndex({
            indexName: 'gsi_paymentId',
            partitionKey: { name: 'tag_paymentId', type: dynamodb.AttributeType.STRING },
            sortKey: { name: 'seq', type: dynamodb.AttributeType.STRING },
            projectionType: dynamodb.ProjectionType.ALL,
          });
          props.globalTable.addGlobalSecondaryIndex({
            indexName: 'gsi_roomType',
            partitionKey: { name: 'tag_roomType', type: dynamodb.AttributeType.STRING },
            sortKey: { name: 'seq', type: dynamodb.AttributeType.STRING },
            projectionType: dynamodb.ProjectionType.ALL,
          });
        */

        // Command handler (write side) — EVENT_TABLE_NAME + Kinesis, grants R/W.
        // grantReadWriteData covers Query on the table + its GSIs and
        // TransactWriteItems (the DCB conditional append).
        const commandHandler = new nodejs.NodejsFunction(this, 'CommandHandler', {
            ...commonProps,
            entry: path.join(__dirname, '../../src/commands/handler.ts'),
            handler: 'handler',
            functionName: `hotel-command-${props.regionLabel}`,
            timeout: cdk.Duration.seconds(10),
            environment: {
                EVENT_TABLE_NAME: props.globalTable.tableName,
                KINESIS_STREAM_NAME: stream.streamName,
                // SageMaker endpoint invoked by automation slices for inference.
                // Provide the deployed endpoint name via the SAGEMAKER_ENDPOINT_NAME
                // context/env; empty until an endpoint exists (the handler then errors).
                SAGEMAKER_ENDPOINT_NAME: props.sagemakerEndpointName ?? '',
            },
        });
        props.globalTable.grantReadWriteData(commandHandler);
        stream.grantWrite(commandHandler);

        // Allow the command Lambda to invoke the model endpoint. Scoped to a
        // named endpoint when provided, else to any endpoint in this account.
        commandHandler.addToRolePolicy(new iam.PolicyStatement({
            actions: ['sagemaker:InvokeEndpoint'],
            resources: [props.sagemakerEndpointName
                ? `arn:aws:sagemaker:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:endpoint/${props.sagemakerEndpointName}`
                : `arn:aws:sagemaker:${cdk.Stack.of(this).region}:${cdk.Stack.of(this).account}:endpoint/*`],
        }));

        // Query handler (read side) — reads Redis only, in the VPC.
        const queryHandler = new nodejs.NodejsFunction(this, 'QueryHandler', {
            ...commonProps,
            entry: path.join(__dirname, '../../src/queries/handler.ts'),
            handler: 'handler',
            functionName: `hotel-query-${props.regionLabel}`,
            timeout: cdk.Duration.seconds(5),
            vpc,
            vpcSubnets: { subnets: vpc.privateSubnets },
            securityGroups: [lambdaSg],
            environment: { REDIS_HOST: redisEndpoint, REDIS_PORT: redisPort, REDIS_TLS: 'true' },
        });

        // Projector (read side) — DynamoDB Streams → Redis, in the VPC.
        const projectorHandler = new nodejs.NodejsFunction(this, 'ProjectorHandler', {
            ...commonProps,
            entry: path.join(__dirname, '../../src/projector/handler.ts'),
            handler: 'handler',
            functionName: `hotel-projector-${props.regionLabel}`,
            timeout: cdk.Duration.seconds(30),
            vpc,
            vpcSubnets: { subnets: vpc.privateSubnets },
            securityGroups: [lambdaSg],
            environment: { REDIS_HOST: redisEndpoint, REDIS_PORT: redisPort, REDIS_TLS: 'true' },
        });
        props.globalTable.grantStreamRead(projectorHandler);
        // Primary region owns the stream→projector mapping (cross-region stream
        // mapping is configured post-deploy).
        if (props.isPrimary) {
            projectorHandler.addEventSource(
                new eventsources.DynamoEventSource(props.globalTable, {
                    startingPosition: lambda.StartingPosition.TRIM_HORIZON,
                    batchSize: 25,
                    retryAttempts: 5,
                    bisectBatchOnError: true,
                })
            );
        }

        // ── API Gateway (prod stage, throttled, CORS) ──
        const api = new apigateway.RestApi(this, 'HotelApi', {
            restApiName: `Hotel API (${props.regionLabel})`,
            deployOptions: {
                stageName: 'prod',
                tracingEnabled: true,
                metricsEnabled: true,
                throttlingRateLimit: 1000,
                throttlingBurstLimit: 2000,
            },
            defaultCorsPreflightOptions: {
                allowOrigins: apigateway.Cors.ALL_ORIGINS,
                allowMethods: apigateway.Cors.ALL_METHODS,
            },
        });

        const apiResource = api.root.addResource('api');
        const recordsResource = apiResource.addResource('records');
        recordsResource.addMethod('POST', new apigateway.LambdaIntegration(commandHandler));
        recordsResource.addMethod('GET', new apigateway.LambdaIntegration(queryHandler));
        const recordByIdResource = recordsResource.addResource('{id}');
        recordByIdResource.addMethod('GET', new apigateway.LambdaIntegration(queryHandler));
    }
}
