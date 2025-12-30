import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as iam from "aws-cdk-lib/aws-iam";
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from "constructs";
import { SES_REGION } from "../env";
import { DynamoEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { StartingPosition } from "aws-cdk-lib/aws-lambda";

export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const imagesBucket = new s3.Bucket(this, "images", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    //create table same as labs. images will be added using commands
    const imagesTable = new dynamodb.Table(this, "ImagesTable", {
      stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: { name: "ImageName", type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      tableName: "ImagesTable",
    });

    // Integration infrastructure

    const rejectionQueue = new sqs.Queue(this, "rejection-queue", {
      queueName: "RejectionQueue",
      receiveMessageWaitTime: cdk.Duration.seconds(10),
    });

    const imageProcessQueue = new sqs.Queue(this, "img-created-queue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
      deadLetterQueue: {
        queue: rejectionQueue,
        maxReceiveCount: 1,
      }
    });


    // Lambda functions

    const mailerFn = new lambdanode.NodejsFunction(this, "mailer-function", {
      runtime: lambda.Runtime.NODEJS_18_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(3),
      entry: `${__dirname}/../lambdas/mailer.ts`,
    });

    const rejectionMailerFn = new lambdanode.NodejsFunction(this, "rejection-mailer-function", {
      runtime: lambda.Runtime.NODEJS_18_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(3),
      entry: `${__dirname}/../lambdas/rejectionMailer.ts`,
    });

    const deleteMailerFn = new lambdanode.NodejsFunction(this, "deletion-mailer-function", {
      runtime: lambda.Runtime.NODEJS_18_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(3),
      entry: `${__dirname}/../lambdas/deletionMailer.ts`,
    });

    const processImageFn = new lambdanode.NodejsFunction(this, "process-image-function", {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: `${__dirname}/../lambdas/processImage.ts`,
      timeout: cdk.Duration.seconds(15),
      memorySize: 1024,
      environment: {
        TABLE_NAME: "ImagesTable",
        REGION: SES_REGION,
      }
    }
    );

    const processDeleteFn = new lambdanode.NodejsFunction(this, "process-delete-function", {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: `${__dirname}/../lambdas/processDelete.ts`,
      timeout: cdk.Duration.seconds(3),
      memorySize: 1024,
    }
    );

    const processUpdateFn = new lambdanode.NodejsFunction(this, "process-update-function", {
      runtime: lambda.Runtime.NODEJS_18_X,
      memorySize: 1024,
      timeout: cdk.Duration.seconds(3),
      entry: `${__dirname}/../lambdas/processUpdate.ts`,
    });

    const mainTopic = new sns.Topic(this, "MainTopic",
      {
        displayName: "Main Topic",
      }
    );


    // Event triggers

    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(mainTopic)
    );

    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_REMOVED,
      new s3n.SnsDestination(mainTopic)
    );

    //turn all subscriptions into one with a filter policy

    mainTopic.addSubscription(
      new subs.SqsSubscription(imageProcessQueue, {
        filterPolicyWithMessageBody: {
          Records: sns.FilterOrPolicy.policy({
            eventName: sns.FilterOrPolicy.filter(
              sns.SubscriptionFilter.stringFilter({
                matchPrefixes: ["ObjectCreated:Put"],
              })
            ),
          })
        }
      })
    );

    mainTopic.addSubscription(
      new subs.LambdaSubscription(processDeleteFn, {
        filterPolicyWithMessageBody: {
          Records: sns.FilterOrPolicy.policy({
            eventName: sns.FilterOrPolicy.filter(
              sns.SubscriptionFilter.stringFilter({
                allowlist: ["ObjectRemoved:Delete"],
              })
            ),
          })
        }
      })
    );

    mainTopic.addSubscription(
      new subs.LambdaSubscription(processUpdateFn, {
        filterPolicy: {
          comment_type: sns.SubscriptionFilter.stringFilter({
            allowlist: ['Description']
          }),
        },
      })
    );

    mainTopic.addSubscription(new subs.LambdaSubscription(mailerFn, {
      filterPolicyWithMessageBody: {
        Records: sns.FilterOrPolicy.policy({
          eventName: sns.FilterOrPolicy.filter(
            sns.SubscriptionFilter.stringFilter({
              allowlist: ["ObjectCreated:Put"],
            })
          ),
        })
      }
    })
    );

    const newImageEventSource = new events.SqsEventSource(imageProcessQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(10),
    });

    const newImageEventRejectSource = new events.SqsEventSource(rejectionQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(10),
    });

    processImageFn.addEventSource(newImageEventSource);

    rejectionMailerFn.addEventSource(newImageEventRejectSource);

    deleteMailerFn.addEventSource(new DynamoEventSource(imagesTable, {
      startingPosition: StartingPosition.TRIM_HORIZON,
      batchSize: 5,
      bisectBatchOnError: true,
    }));

    // Permissions

    imagesBucket.grantRead(processImageFn);
    imagesTable.grantWriteData(processImageFn);
    imagesTable.grantReadWriteData(processDeleteFn);
    imagesTable.grantReadWriteData(processUpdateFn);

    mailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    processImageFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ["sqs:SendMessage"],
      resources: [rejectionQueue.queueArn]
    }));

    rejectionMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    deleteMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ses:SendEmail",
          "ses:SendRawEmail",
          "ses:SendTemplatedEmail",
        ],
        resources: ["*"],
      })
    );

    // Output

    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });

    new cdk.CfnOutput(this, "mainTopic", {
      value: mainTopic.topicArn,
    });

  }
}