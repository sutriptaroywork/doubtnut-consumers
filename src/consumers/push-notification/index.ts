import path from "path";
import * as _ from "lodash";
import moment from "moment";
import admin from "firebase-admin";
import { InputMeta } from "../../interfaces";
import {common} from "../../helpers/common";
import { config, publishRaw } from "../../modules";
const pathToServiceAccount = path.resolve(config.fcm);
const serviceAccount = require(pathToServiceAccount);


admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
});

process.on("uncaughtException", (e, origin) => {
    console.log("Process_error_occured_from:\n", origin);
    console.log("Process_error:\n", e);
});

const FEED_EVENTS = ["post_detail"];

async function pushToGCM(gcmData) {
    const message = {
        tokens: gcmData.to,
        data: gcmData.data,
    };
    console.log(message.data);
    try {
        const response = await admin.messaging().sendMulticast(message);
        console.log("Firebase message response", response);
        return response;
    } catch (e) {
        console.error("Firebase message error:", e);
        return {};
    }
}

export async function onMsg(msg: { data: any; meta: InputMeta }[]) {
    try {
        for (let i = 0; i < msg.length; i++) {
            const { meta, data } = msg[i];
            const ts = new Date(meta.ts);

            let mongoData: any;
            const producedAt = moment(meta.ts);
            const consumedAt = moment();
            if (consumedAt.diff(producedAt, "hours") > 6) {
                mongoData = {
                    data,
                    type: "push_notification",
                    studentId: meta.studentId,
                    producedAt: ts,
                    createdAt: new Date(),
                    response: "Notification Consumed After Max time limit of 6 hours",
                };
            } else {
                // remove certain sid based on snid
                if (data && data.s_n_id && meta.gcmId && meta.gcmId.length){
                    const snidMapping: any = common.getSNID();
                    if (snidMapping && !_.isEmpty(snidMapping)){
                        const sidList: any = meta.studentId;
                        let allCampaignSId = []; // all campaign user from original list
                        let allEligibleCampaignSId = []; // all eligible campaign user

                        const snidKeys = Object.keys(snidMapping);
                        for (let j = 0; j < snidKeys.length; j++){
                            allCampaignSId = [...allCampaignSId, ..._.intersection(snidMapping[snidKeys[j]], sidList)];
                            if (snidKeys[j] !== "null" && (snidKeys[j] === "all" || data.s_n_id.startsWith(snidKeys[j]))){ // snid pattern check for all keys, if exit take intersection
                                allEligibleCampaignSId = [...allEligibleCampaignSId, ..._.intersection(snidMapping[snidKeys[j]], sidList)];
                            }
                        }

                        const sidCampaignCheck = {}; // 1 send, 2 not send
                        for (let j = 0; j < allCampaignSId.length; j++){ // set all campaign user sid inactive
                            sidCampaignCheck[allCampaignSId[j]] = 2;
                        }
                        for (let j = 0; j < allEligibleCampaignSId.length; j++){ // set eligible campaign user sid active
                            sidCampaignCheck[allEligibleCampaignSId[j]] = 1;
                        }

                        const newStuList = [];
                        const newGCMIdList = [];
                        for (let j = 0; j < sidList.length; j++){ // now iterate over original sid and check in sidCampaignCheck if not fount then non campaign user(pass) if value=1 then pass(eligible camaign user)
                            if (!sidCampaignCheck[sidList[j]] || sidCampaignCheck[sidList[j]] === 1){
                                newStuList.push(sidList[j]);
                                newGCMIdList.push(meta.gcmId[j]);
                            }
                        }
                        meta.studentId = newStuList; // assign the updated sid
                        meta.gcmId = newGCMIdList; // assign the updated gcmid
                    }
                }

                meta.gcmId = _.compact(meta.gcmId);
                if (meta.gcmId.length === 0)
                {
                    return;
                }

                const gcmArrayBatch = _.chunk(meta.gcmId, 100);
                const gcmData = {
                    data,
                    type: "push_notification",
                    studentId: meta.studentId,
                    to: meta.gcmId,
                    producedAt: ts,
                    createdAt: new Date(),
                };

                if (!_.isEmpty(gcmData.data.message) && typeof(gcmData.data.message) == "string" ){
                    gcmData.data.message = _.truncate(gcmData.data.message, { length: 200});
                }
                Object.keys(gcmData.data).forEach(key => {
                    if (typeof(gcmData.data[key]) != "string" ) {
                        gcmData.data[key] = JSON.stringify(gcmData.data[key]);
                    }
                });

                // if s_n_id exist, populate it in notification_id key
                try {
                    if ("s_n_id" in gcmData.data && gcmData.data.data) {
                        gcmData.data.data = JSON.parse(gcmData.data.data);
                        gcmData.data.data.notification_id = gcmData.data.s_n_id + "_" + producedAt.format("YYYYMMDD");
                        gcmData.data.data = JSON.stringify(gcmData.data.data);
                    }
                }
                catch (e)
                {
                    console.log(e);
                }

                const promises = [];
                gcmArrayBatch.forEach(element => {
                    gcmData.to = element;
                    promises.push(pushToGCM(gcmData));
                });
                const result = await Promise.all(promises);

                console.log(result);
                const gcmResult = {
                    successCount: 0,
                    failureCount: 0,
                };
                result.forEach(element => {
                    gcmResult.successCount += element.successCount;
                    gcmResult.failureCount += element.failureCount;
                });
                console.log("gcmResult", gcmResult, gcmData );
                const gcmResultAt = new Date();
                mongoData = {...gcmData, response: gcmResult, gcmResultAt};
            }
            /**
             * Switch off Mongo Logging
             * // not send gcm list to mongo for logs
             * if (mongoData.hasOwnProperty("to")) {
             *     delete mongoData.to;
             * }
             * publishRaw("consumer.push.notification.log.mongo", mongoData);
             */
        }
    } catch (e) {
        console.error("Error:: ", e);
    }
}

export const opts = [{
    topic: "api-server.push.notification",
    fromBeginning: true,
    numberOfConcurrentPartitions: 10,
    autoCommitAfterNumberOfMessages: 500,
    autoCommitIntervalInMs: 60000,
},
 {
    topic: "bull-cron.push.notification",
    fromBeginning: false,
    numberOfConcurrentPartitions: 20,
    autoCommitAfterNumberOfMessages: 500,
    autoCommitIntervalInMs: 60000,
},
{
    topic: "micro.push.notification",
    fromBeginning: true,
    numberOfConcurrentPartitions: 10,
    autoCommitAfterNumberOfMessages: 500,
    autoCommitIntervalInMs: 60000,
},
{
    topic: "newton.push.notification",
    fromBeginning: true,
    numberOfConcurrentPartitions: 10,
    autoCommitAfterNumberOfMessages: 500,
    autoCommitIntervalInMs: 60000,
}];
