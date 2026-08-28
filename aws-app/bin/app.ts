#!/usr/bin/env node
// CDK app entry for the hotel occupancy-forecast model.
//
// This is the ONLY hand-authored infrastructure: it owns the DynamoDB event
// table (with the DCB GSIs the model requires) and instantiates the GENERATED
// RegionalStack (infra/stacks/regional-stack.ts) that wires the Lambdas, Redis,
// Kinesis, API Gateway and the SageMaker InvokeEndpoint grant.
//
// The table lives in its own stack so RegionalStack can receive it via props
// (the generated stack expects `globalTable: dynamodb.Table`).
import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import { RegionalStack } from '../infra/stacks/regional-stack';

// DCB tag axes declared across the model's commands — one GSI each so a
// boundary branch can be queried by tag value. Derived from blueprint_dsl_dcb.
const DCB_AXES = ['bookingId', 'email', 'paymentId', 'roomNumber', 'roomType'];

class EventStoreStack extends cdk.Stack {
  public readonly table: dynamodb.Table;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Event store: PK = eventId, SK = seq. Guard (TAGPOS) items reuse the same
    // schema. Streams on so the projector can fold events into the read model.
    this.table = new dynamodb.Table(this, 'EventTable', {
      tableName: 'HotelEvents',
      partitionKey: { name: 'eventId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'seq', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      stream: dynamodb.StreamViewType.NEW_IMAGE,
      removalPolicy: cdk.RemovalPolicy.DESTROY, // demo/PoC — not for production data
    });

    // One GSI per DCB axis: partitionKey = tag_<axis>, sortKey = seq.
    for (const axis of DCB_AXES) {
      this.table.addGlobalSecondaryIndex({
        indexName: `gsi_${axis}`,
        partitionKey: { name: `tag_${axis}`, type: dynamodb.AttributeType.STRING },
        sortKey: { name: 'seq', type: dynamodb.AttributeType.STRING },
        projectionType: dynamodb.ProjectionType.ALL,
      });
    }
  }
}

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const eventStore = new EventStoreStack(app, 'HotelEventStore', { env });

new RegionalStack(app, 'HotelRegionalPrimary', {
  env,
  regionLabel: 'primary',
  globalTable: eventStore.table,
  isPrimary: true,
  // Supply a real endpoint name here once the SageMaker model is deployed.
  // Left undefined for synth: InvokeEndpoint is then granted account-wide and
  // the automation handler errors at runtime until an endpoint exists.
  sagemakerEndpointName: process.env.SAGEMAKER_ENDPOINT_NAME,
});

app.synth();
