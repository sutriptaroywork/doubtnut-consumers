import _ from "lodash";
import moment from "moment";
import { mysql, redis, redis_hp, publishRaw } from "../../modules";
import { InputMeta } from "../../interfaces";

// const luaScript = fs.readFileSync("./src/consumers/trending-chapter/lLen.lua");

const HASH_EXPIRY_ONE_DAY = 60 * 60 * 24;
const LF_SIDS = [-142, -55];
// seconds
const LF_ET = 180;
const SF_ET = 10;

async function getVideoViewStatById(view_id): Promise<any> {
    const sql = "SELECT * from video_view_stats WHERE view_id = ?";
    return mysql.con.query(sql, [view_id]).then(x => x[0][0]);
}

async function getLocationByStudentId(studentId): Promise<any> {
    const sql = "SELECT * from student_location WHERE student_id = ?";
    return mysql.con.query(sql, [studentId]).then(x => x[0][0]);
}

async function getChapterByQuestionId(questionId): Promise<any> {
    const sql = "SELECT * from questions_meta WHERE question_id = ?";
    return mysql.con.query(sql, [questionId]).then(x => x[0][0]);
}

async function getLFChapterByQuestionId(questionId): Promise<any> {
    const sql = "select topic from course_resources cr where resource_reference = ? limit 1";
    return mysql.con.query(sql, [questionId]).then(x => x[0][0]);
}

async function insertDump(db, data) {
    return db.collection("vvs_location_chapter").insertOne(data);
}

async function insertDataToMongo({ db, vvsRow, studentLocation, cityData, chapterData, engageTime, videoTime }) {
    const latitude = _.get(studentLocation, "latitude", 0.0);
    const longitude = _.get(studentLocation, "longitude", 0.0);
    const pincode = _.get(studentLocation, "pincode", "");
    const state = _.get(studentLocation, "state", "");
    const country = _.get(studentLocation, "country", "");
    // to add
    const tehsil = "";

    const dumpData = { ...vvsRow, engage_time: engageTime, video_time: videoTime, cityData, chapterData, latitude, longitude, pincode, state, country, tehsil };
    await insertDump(db, dumpData);
}

async function insertRecommendationData({ vvsRow, questionChapter, engageTime, videoTime }) {
    const studentId = vvsRow.student_id;
    const questionId = vvsRow.question_id;
    let chapterName = _.get(questionChapter, "chapter", null);

    let videoType = "SF";
    let userType = "SF";

    let questionData = await redis.hgetAsync(`QUESTION:${questionId}`, "ANSWER");

    if (_.isEmpty(questionData)) {
        return;
    }

    questionData = JSON.parse(questionData);
    if (LF_SIDS.includes(questionData[0].student_id)) {
        videoType = "LF";
    }
    const subject = questionData[0].subject;
    const tagUserByET = (videoType === "LF" && (parseInt(engageTime, 10) > LF_ET)) || (videoType === "SF" && (parseInt(engageTime, 10) > SF_ET));

    if (!tagUserByET) {
        return;
    }

    if (videoType === "LF") {
        const LFChapter = await getLFChapterByQuestionId(`${questionId}`);
        chapterName = _.get(LFChapter, "topic", null);
    }

    if (_.isEmpty(chapterName)) {
        return;
    }

    const today = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
    const yesterday = moment().subtract(1, "days").add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
    const dayBefYesterday = moment().subtract(2, "days").add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");


    const [sfDataToday, sfDataYesterday, sfDataDayBefYesterday, lfDataToday, lfDataYesterday, lfDataDayBefYesterday] = await Promise.all([
        redis_hp.llenAsync(`u:rec:${studentId}:SF:${today}`),
        redis_hp.llenAsync(`u:rec:${studentId}:SF:${yesterday}`),
        redis_hp.llenAsync(`u:rec:${studentId}:SF:${dayBefYesterday}`),
        redis_hp.llenAsync(`u:rec:${studentId}:LF:${today}`),
        redis_hp.llenAsync(`u:rec:${studentId}:LF:${yesterday}`),
        redis_hp.llenAsync(`u:rec:${studentId}:LF:${dayBefYesterday}`),
    ]);
    let totalSf = sfDataToday + sfDataYesterday + sfDataDayBefYesterday;
    let totalLf = lfDataToday + lfDataYesterday + lfDataDayBefYesterday;

    // let totalSf = await redis_hp.eval(luaScript, 3, `u:rec:${studentId}:SF:${today}`, `u:rec:${studentId}:SF:${yesterday}`, `u:rec:${studentId}:SF:${dayBefYesterday}`);
    // let totalLf = await redis_hp.eval(luaScript, 3, `u:rec:${studentId}:LF:${today}`, `u:rec:${studentId}:LF:${yesterday}`, `u:rec:${studentId}:LF:${dayBefYesterday}`);

    if (videoType === "SF") {
        totalSf += 1;
    } else if (videoType === "LF") {
        totalLf += 1;
    }

    if (totalSf && !totalLf) {
        userType = "SF";
    } else if (totalSf && totalLf) {
        userType = "SF_LF";
    } else if (!totalSf && totalLf) {
        userType = "LF";
    }

    await redis_hp.multi()
        .hset(`u:rec:${studentId}`, videoType, chapterName)
        .hset(`u:rec:${studentId}`, "userType", userType)
        .expire(`u:rec:${studentId}`, HASH_EXPIRY_ONE_DAY * 4)
        .lpush(`u:rec:${studentId}:${videoType}:${today}`, chapterName)
        .ltrim(`u:rec:${studentId}:${videoType}:${today}`, 0, 5)
        .expire(`u:rec:${studentId}:${videoType}:${today}`, HASH_EXPIRY_ONE_DAY * 4)
        .execAsync();

    const consumerData = {
        data: {
            userType,
            videoType,
            chapterName,
            studentId,
            questionId,
            subject,
        },
    };
    publishRaw("consumer.homepage.recommendation", consumerData, +studentId % 20);
}


export async function onMsg(msg: { data: { viewId: string; engageTime: string; videoTime: string; isback: string }; meta: InputMeta }[]) {
    for (let i = 0; i < msg.length; i++) {
        const { data } = msg[i];
        let { videoTime, engageTime }: any = data;
        // const teslaMongoDb = await getMongoDB("teslaFeed", "doubtnut");

        const vvsRow = await getVideoViewStatById(data.viewId);
        if (!vvsRow) {
            console.log("no row");
            continue;
        }
        if (vvsRow.engage_time >= engageTime) {
            engageTime = vvsRow.engage_time;
        }
        if (vvsRow.video_time >= videoTime) {
            videoTime = vvsRow.video_time;
        }

        // const studentLocation = await getLocationByStudentId(vvsRow.student_id);
        const questionChapter = await getChapterByQuestionId(vvsRow.question_id);

        // const cityData = _.get(studentLocation, "city", null);
        // const chapterData = _.get(questionChapter, "chapter", null);
        // const checkData = studentLocation && questionChapter && cityData && chapterData;

        await insertRecommendationData({ vvsRow, questionChapter, engageTime, videoTime });

        // if (!checkData) {
        //     console.log("no location or chapter data");
        //     continue;
        // }
        // engageTime = parseInt(engageTime, 10) / 1000;
        // const redisKey = `trend:${cityData.toLowerCase().replace(/( |\n|\r|\r\n)/g, "")}`;
        // await Promise.all([
        //     redis.multi()
        //         .zincrby(redisKey, engageTime, chapterData)
        //         .expire(redisKey, 60 * 60 * 24 * 4)
        //         .execAsync(),

        // insertDataToMongo({ db: teslaMongoDb, vvsRow, studentLocation, cityData, chapterData, engageTime, videoTime }),
        // ]);
    }
}

export const opts = [{
    topic: "api-server.vvs.update",
    fromBeginning: false,
    numberOfConcurrentPartitions: 2,
}];
