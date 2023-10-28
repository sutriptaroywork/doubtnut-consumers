import { config, getMongoDB } from "../../modules";
let db;

export async function onMsg(msg: any) {
    try {
        db  = await getMongoDB("event", "branch");
        for (let i = 0; i < msg.length; i++) {
            const json = msg[i].data.data;
            json.created_at = new Date();
            await db.collection("events").insertOne(json);
        }
    } catch (e) {
        console.log(e);
    }
}

export const opts = [{
    topic: "hook-server.events.insert",
    fromBeginning: true,
}];
