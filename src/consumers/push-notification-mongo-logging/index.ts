import * as _ from "lodash";
import { InputMeta } from "../../interfaces";
import { getMongoDB } from "../../modules";

let db;
const FEED_EVENTS = ["post_detail"];

async function insertToMongo(message) {
    // console.log("MESSAGE", message)
    if (FEED_EVENTS.includes(message.data.event)) {
        // A FEED EVENT --> Comment or a Reply to a comment
        let club_action;
        let data;
        try {
            data = JSON.parse(message.data.data);
            club_action = data.club_action;
            //  console.log("JSON PARSE SUCCCESSFUL",data,club_action);
        } catch (e) {
            // logger.info(res.ops[0]);
            console.error(e, data);
        }


        if (club_action) {
            message.club_action = club_action;
            const options = {
                upsert:true,
                returnNewDocument : true,
            };
            // console.log("UPDATE/UPSERTING", message);
            return await db.collection("notification").findOneAndUpdate(
                {club_action},
                {$set: {...message}},
                options);

        } else {
            // THIS SHOULD NEVER BE CALLED
            console.log("SHOULD NOT HAVE HAPPENED");
            return await db.collection("notification").insertOne(message);
        }

    }
    return await db.collection("notification").insertOne(message);
}

export async function onMsg(msg: { data: any; meta: InputMeta }[]) {
    try {
        for (let i = 0; i < msg.length; i++) {
            const data = msg[i];
            db = await getMongoDB("newton", "doubtnut");
            if (!_.isEmpty(data)) {
                await insertToMongo(data);
            }
        }
    } catch (e) {
        console.error("Error:: ", e);
    }
}

export const opts = [{
    topic: "consumer.push.notification.log.mongo",
    fromBeginning: true,
    numberOfConcurrentPartitions: 1,
    autoCommitAfterNumberOfMessages: 500,
    autoCommitIntervalInMs: 60000,
}];
