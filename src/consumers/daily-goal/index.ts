// import { Kafka } from "kafkajs";

import { publishRaw } from "../../modules";
import { dailyGoalChecker } from "./daily-goal-checker";

// const kafka = new Kafka({
//     clientId: "consumer-app",
//     brokers: config.kafkaHosts,
// });

// const producer = kafka.producer();
// (async () => {
//     await producer.connect();
//     console.log("connected producer");
// })();

export async function onMsg(msg: { data: { sid: number; versionCode: number; locale: string; gcmId: string; questionId: number; parentId: number }}[]) {
    for (let i = 0; i < msg.length; i++) {
        const { data } = msg[i];
        const { sid, versionCode, locale, gcmId, questionId, parentId } = data;

        const notificationDetails = await dailyGoalChecker.storeDoubtFeedTopic(sid, questionId, parentId, locale);

        if (notificationDetails.sendNotification) {
            const notificationData = {
                event: "doubt_feed",
                title: locale === "hi" ? `${notificationDetails.topic} का डाउट फीड है तैयार!` : `${notificationDetails.topic} ka Doubt feed hai ready!`,
                message: locale === "hi" ? "पढ़ना शुरू करें और आज का लक्ष्य/गोल पूरा करें!" : "Padhna karo shuru aur aaj ka goal karo complete!",
                image: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/FF973C29-A318-DF8D-95BA-B09F6013B4AE.webp",
                firebase_eventtag: "generated_df_notification",
                data: {
                    random: 1,
                },
            };
            if (versionCode >= 921) {
                notificationData.event = "doubt_feed_2";
                notificationData.title = locale === "hi" ? `${notificationDetails.topic} का डेली गोल है तैयार!` : `${notificationDetails.topic} ka Daily Goal hai ready!`;
                notificationData.message = locale === "hi" ? "पढ़ना शुरू करें और आज का लक्ष्य/गोल पूरा करें!" : "Padhna karo shuru aur aaj ka goal karo complete!";
                notificationData.image = "https://d10lpgp6xz60nq.cloudfront.net/daily_feed_resources/daily-goal-generated.webp";
            }
            const value = { meta: { studentId: [sid], gcmId: [gcmId], ts: Date.now() }, data: notificationData };
            // producer.send({
            //     topic: "api-server.push.notification",
            //     messages: [{
            //         value: JSON.stringify(value),
            //     }],
            // });
            publishRaw("api-server.push.notification", value);

        }
    }
}

export const opts = [{
    topic: "api-server.store.daily.goal",
    fromBeginning: false,
    numberOfConcurrentPartitions: 1,
}];
