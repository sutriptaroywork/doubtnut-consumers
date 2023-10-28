import * as _ from "lodash";
import moment from "moment";

import { RowDataPacket } from "mysql2";

import { mysql, publishRaw } from "../../modules";

const studyGroupMsgTemplate = {
    message: {
        widget_data: {
            child_widget: {
                widget_data: {
                    title: "", // content
                },
                widget_type: "text_widget",
            },
            created_at: null,
            type: 0,
            student_img_url: null,
            student_id: null,
            widget_display_name: "text_widget",
            cta_text: null,
            deeplink: null,
        },
        widget_type: "widget_study_group_parent",
    },
    room_id: null,
    student_id: null,
    room_type: "public_groups",
    is_active: true,
    is_deleted: false,
};

const studyGroupImgMsgTemplate = {
    message: {
        widget_data: {
            child_widget: {
                widget_data: {
                    question_image: "", // https://d10lpgp6xz60nq.cloudfront.net/images/sg_thumbnail_81692214_1629178584.jpeg_study_group_81692214_1629178584.jpeg
                    deeplink: "doubtnutapp://full_screen_image?ask_que_uri={link}&title=English%20Seekho%20Contest",
                    id: "question",
                    card_ratio: "16:9",
                },
                widget_type: "widget_asked_question",
            },
            created_at: null,
            type: 0,
            student_img_url: null,
            title: "", // content
            student_id: null,
            widget_display_name: "Image",
            cta_text: null,
            deeplink: null,
        },
        widget_type: "widget_study_group_parent",
    },
    room_id: null,
    student_id: null,
    room_type: "public_groups",
    is_active: true,
    is_deleted: false,
};

async function getStudyGroupsBySid(studentId) {
    const sql = "SELECT sgm.student_id,sg.group_id, sg.group_type from study_group_members sgm left join study_group sg on sgm.study_group_id = sg.id where student_id = ? and  sg.group_type = 2 and sgm.is_active = 1 group by sg.group_id";
    return mysql.con.query<RowDataPacket[]>(sql, [studentId]).then(x => x[0]);
}

async function getDNP(name) {
    const sql = "SELECT name, value from dn_property dnp where bucket= \"quiztfs\" and name = ? and is_active = 1";
    return mysql.con.query<RowDataPacket[]>(sql, [name]).then(x => x[0]);
}

async function insertPostToTesla(db, feedPost) {
    return db.collection("tesla").insertOne(feedPost);
}

// msgType 1 - text, 2 - image
function getStudyGroupMsg(groupType, msgType, groupId, imageUrl, studentId, studentImgUrl, studentName, msgText) {
    let msg = null;
    if (msgType === 1) {
        msg = { ...studyGroupMsgTemplate };
        console.log(msg);
        msg.message.widget_data.child_widget.widget_data.title = msgText;
    }
    if (msgType === 2) {
        msg = { ...studyGroupImgMsgTemplate };
        msg.message.widget_data.child_widget.widget_data.question_image = imageUrl;
        msg.message.widget_data.child_widget.widget_data.deeplink = "doubtnutapp://daily_practice";
        // `doubtnutapp://full_screen_image?ask_que_uri=${imageUrl}&title=English%20Seekho%20Contest`;
    }

    msg.room_id = groupId;
    msg.student_id = parseInt(studentId, 10);
    msg.message.widget_data.student_id = studentId;
    msg.message.widget_data.created_at = moment().add(5, "hours").add(30, "minutes").valueOf();
    msg.message.widget_data.student_img_url = studentImgUrl;
    msg.message.widget_data.title = studentName;


    return {
        data: msg,
    };
}

function getRandomMsg([...allMsg]) {
    return allMsg.sort(() => Math.random() - Math.random())[0];
}

export async function onMsg(msg: { data: any }[]) {
    try {
        for (let i = 0; i < msg.length; i++) {
            const { data } = msg[i];
            // const teslaMongoDb = await getMongoDB("teslaFeed", "doubtnut");

            // const feedPostImageDNP = await getDNP("feed_post_image");
            // // const feedPostImage = _.get(feedPostImageDNP, "[0].value", "2022/02/04/12-53-40-114-PM_WhatsApp%20Image%202022-02-04%20at%206.22.33%20PM.webp");
            // const feedPostImage = _.get(feedPostImageDNP, "[0].value", null);

            // const feedPostMsgDNP = await getDNP("feed_post_message");
            // const feedPostMsgList = feedPostMsgDNP.map(eachMsg => eachMsg.value);
            // feedPostMsgList.unshift("Oo mere yaar, English se ho jaayega pyaar, bas try toh Karo ye English Practice ek baar -- https://doubtnut.app.link/gNRqhLGEsnb");

            // const currentFeedPostMsg = getRandomMsg(feedPostMsgList);

            // const feedPost: any = {
            //     msg: currentFeedPostMsg,
            //     type: "message",
            //     // attachment: [
            //     //     feedPostImage
            //     // ],
            //     student_id: parseInt(data.student_id, 10),
            //     class: data.student_class,
            //     is_deleted: false,
            //     is_profane: false,
            //     is_active: true,
            // };

            // if (feedPostImage) {
            //     feedPost.type = "image";
            //     feedPost.attachment = [feedPostImage];
            // }
            // await insertPostToTesla(teslaMongoDb, feedPost);

            const studyGroupImageDNP = await getDNP("study_group_post_image");
            // const studyGroupImageUrl = _.get(studyGroupImageDNP, "[0].value", "https://d10lpgp6xz60nq.cloudfront.net/images/2022/02/04/12-53-40-114-PM_WhatsApp%20Image%202022-02-04%20at%206.22.33%20PM.webp");
            const studyGroupImageUrl = _.get(studyGroupImageDNP, "[0].value", null);

            const studyGroupMsgDNP = await getDNP("study_group_post_message");
            const msgList = studyGroupMsgDNP.map(eachMsg => eachMsg.value);
            msgList.unshift("Oo mere yaar, English se ho jaayega pyaar, bas try toh Karo ye English Practice ek baar - https://doubtnut.app.link/DcEnAJGEsnb");


            const studyGroups = await getStudyGroupsBySid(data.student_id);
            console.log(studyGroupImageUrl);
            for (let j = 0; j < studyGroups.length; j++) {
                const currentMsg = getRandomMsg(msgList);

                // prefix - type - group_type
                // sg - private - 1
                // pg - public - 2
                // tg - paid - 3
                const studyGroupTextMsg = getStudyGroupMsg(2, 1, studyGroups[j].group_id, studyGroupImageUrl, data.student_id, data.student_img_url, data.student_name, currentMsg);
                publishRaw("micro.study.group", studyGroupTextMsg, 0);

                if (studyGroupImageUrl) {
                    const studyGroupImgMsg = getStudyGroupMsg(2, 2, studyGroups[j].group_id, studyGroupImageUrl, data.student_id, data.student_img_url, data.student_name, currentMsg);
                    publishRaw("micro.study.group", studyGroupImgMsg, 0);
                }
            }
        }
    } catch (e) {
        console.error("Error: ", e);
    }
}

export const opts = [{
    topic: "api-server.practice-english.feed.post",
    fromBeginning: true,
    autoCommitAfterNumberOfMessages: 500,
    autoCommitIntervalInMs: 60000,
}];
