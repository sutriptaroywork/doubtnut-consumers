import * as _ from "lodash";
import moment from "moment";
import { InputMeta } from "../../interfaces";
import { mysql, redis } from "../../modules";

async function insertUserQuestionAskCountByDate(studentId, date) {
    const sql = "insert into student_doubtnut_paywall_question_asked_count_mapping (student_id, date, count) VALUES (?,?,1) on DUPLICATE KEY UPDATE count = count + 1";
    return mysql.writeCon.query(sql, [studentId, date]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function getUserQuestionAskCount(studentId) {
    console.log(studentId);
    const sql = "select *, sum(count) as total_question_asked_count from student_doubtnut_paywall_question_asked_count_mapping where student_id = ?";
    return mysql.singleQueryTransaction(sql, [studentId]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function getUserQuestionAskCountFromRedis(studentId) {
    return redis.getAsync(`doubtnut_paywall_question_ask_count:${studentId}`);
}

async function setUserQuestionAskCountRedis(studentId, data) {
    return redis.setAsync(`doubtnut_paywall_question_ask_count:${studentId}`, JSON.stringify(data), "Ex", 60 * 60 * 24 * 30); // 1 month
}

export async function onMsg(msg: { data: any; meta: InputMeta }[]) {
    for (let i = 0; i < msg.length; i++) {
        const { meta, data } = msg[i];
        console.log("meta", meta);
        console.log("data", data);
        const ts = new Date(meta.ts);

        if (data.type == "increase_count") {
            await insertUserQuestionAskCountByDate(data.student_id, data.date);
        }
        const studentQuestionAskCountMysql = await getUserQuestionAskCount(data.student_id);
        console.log("studentQuestionAskCountMysql", studentQuestionAskCountMysql);
        await setUserQuestionAskCountRedis(data.student_id, {
            student_id: data.student_id,
            question_ask_total_count: studentQuestionAskCountMysql.length ? studentQuestionAskCountMysql[0].total_question_asked_count : 0,
        });
    }
}

export const opts = [{
    topic: "api-server.doubtnut-paywall.question.count",
    fromBeginning: true,
    numberOfConcurrentPartitions: 1,
    autoCommitAfterNumberOfMessages: 1,
    autoCommitIntervalInMs: 60000,
}];
