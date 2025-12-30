/* eslint-disable import/extensions, import/no-absolute-path */
import { SNSHandler, SNSEvent } from 'aws-lambda';
import {
    S3Client,
} from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, DeleteCommand } from "@aws-sdk/lib-dynamodb";
import { SES_REGION } from "env";

const ddb = createDDbDocClient();
const s3 = new S3Client();

export const handler: SNSHandler = async (event: SNSEvent) => {
    console.log("Event ", event);
    for (const record of event.Records) {

        const snsMessage = JSON.parse(record.Sns.Message);

        if (snsMessage.Records) {
            console.log("Record body ", JSON.stringify(snsMessage));
            for (const messageRecord of snsMessage.Records) {
                const s3e = messageRecord.s3;
                const srcBucket = s3e.bucket.name;
                // Object key may have spaces or unicode non-ASCII characters.
                const srcKey = decodeURIComponent(s3e.object.key.replace(/\+/g, " "));
                // Infer the image type from the file suffix.
                const ddbParams = {
                    TableName: "ImagesTable",
                    Key: {
                        ImageName: srcKey,
                    }
                };

                try {
                    await ddb.send(new DeleteCommand(ddbParams));
                    console.log("Deleted ${srcKey}")
                }
                catch (error) {
                    console.log("Couldn't delete ${srcKey}")
                    throw new Error("Couldn't delete ${srcKey}")
                }

            }
        }
    }
}


function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({ region: SES_REGION });
    const marshallOptions = {
        convertEmptyValues: true, removeUndefinedValues: true, convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}