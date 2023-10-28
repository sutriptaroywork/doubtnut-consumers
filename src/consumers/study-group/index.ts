import { getMongoDB } from "../../modules";

const collectionName = {
    private_group: "chatroom_messages",
    public_group: "public_group_messages_2022",
    study_chat: "study_chat_messages_new",
};

let db;

async function insertToMongo(message) {
    console.log("MESSAGE", message);
    const groupPrefix = message.room_id.split("-")[0];
    if (groupPrefix === "sg") {
        await db.collection(collectionName.private_group).insertOne(message);
    } else if (groupPrefix === "sc") {
        await db.collection(collectionName.study_chat).insertOne(message);
    } else {
        await db.collection(collectionName.public_group).insertOne(message);
    }
    return true;
}

export async function onMsg(msg: { data: any }[]) {
    try {
        for (let i = 0; i < msg.length; i++) {
            const { data } = msg[i];
            db = await getMongoDB("studygroup", "feed");
            console.log(db);

            const insertObj = {
                ...data,
                consumed_at: Date.now(),
            };
            await insertToMongo(insertObj);
        }
    } catch (e) {
        console.error("Error: ", e);
    }
}

export const opts = [{
    topic: "micro.study.group",
    fromBeginning: true,
    autoCommitAfterNumberOfMessages: 500,
    autoCommitIntervalInMs: 60000,
}];
