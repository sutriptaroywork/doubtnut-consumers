import _ from "lodash";
import moment from "moment";
import { mysql, redis, redis_hp } from "../../modules";

const HASH_EXPIRY_ONE_DAY = 60 * 60 * 24;

async function getClassLocaleCategory(questionId) {
    const sql = `select lc.class, lc.locale, CASE
            WHEN lc.course_exam = "IIT" THEN "IIT"
            WHEN lc.course_exam = "NEET" THEN "NEET"
            ELSE 'BOARDS'
        END as exam_category from liveclass_course_resources lcr 
        left join liveclass_course lc on lcr.liveclass_course_id = lc.id
        where lcr.resource_reference = ? limit 1`;
    return mysql.con.query(sql, [questionId]).then(x => x[0][0]);
}

async function getChapterNameFromAlias(chapterName) {
    const sql = "SELECT chapter_alias from chapter_alias_all_lang WHERE chapter = ?";
    return mysql.con.query(sql, [chapterName]).then(x => x[0][0]);
}

async function getCourseTypeFromCcm(userCcm) {
    const sql = "select course from class_course_mapping ccm where id in (?)";
    return mysql.con.query(sql, [userCcm]).then(x => x[0][0]);
}

async function getNextChaptersData(qidClass, qidLocale, qidCategory, subject) {
    const sql = `SELECT
        id, master_chapter, chapter_order
        from
            recommendation_chapter
        where
            class = ?
            and locale = ?
            and category = ?
            and subject = ?
        order by
            chapter_order`;
    return mysql.con.query(sql, [qidClass, qidLocale, qidCategory, subject]).then(x => x[0]);
}

async function getNextChapterQidsData(chapterId, limit) {
    const sql = `SELECT rcv.resource_reference, rc.subject, rc.chapter_order, rcv.video_order  from recommendation_chapter_video rcv
        left join recommendation_chapter rc on rcv.chapter_id = rc.id
        WHERE rcv.chapter_id = ? order by rcv.video_order limit ?`;
    return mysql.con.query(sql, [chapterId, limit]).then(x => x[0]);
}

async function getNextChapter(studentId, userType, videoType, questionId, subject, chapterName) {
    let qidClass = "";
    let qidLocale = "";
    let qidCategory = "";

    if (videoType === "LF") {
        const qidLFType = await getClassLocaleCategory(`${questionId}`);
        qidClass = qidLFType.class;
        qidLocale = qidLFType.locale;
        qidCategory = qidLFType.exam_category;
    }

    if (videoType === "SF") {
        const chapterAlias = await getChapterNameFromAlias(chapterName);
        chapterName = _.get(chapterAlias, "chapter_alias", null);

        let [userData, userCcm] = await Promise.all([
            redis.hgetAsync(`USER:PROFILE:${studentId}`, "USER"),
            redis.hgetAsync(`USER:PROFILE:${studentId}`, "ccmId"),
        ]);

        userData = JSON.parse(userData);
        userCcm = JSON.parse(userCcm);

        if (_.isEmpty(userData) || _.isEmpty(userCcm)) {
            console.log("empty user Data");
            console.log(userData, userCcm);
            return null;
        }

        qidClass = userData[0].student_class;
        qidLocale = userData[0].locale === "hi" ? "HINDI" : "ENGLISH";
        const courseType = await getCourseTypeFromCcm(userCcm);

        if (courseType.course.includes("IIT")) {
            qidCategory = "IIT";
        } else if (courseType.course.includes("NEET")) {
            qidCategory = "NEET";
        } else {
            qidCategory = "BOARDS";
        }
    }
    if (!(qidClass && qidLocale && qidCategory && chapterName && subject)) {
        console.log("no metadata");
        console.log(qidClass, qidLocale, qidCategory, chapterName, subject);
        return null;
    }
    if (["LF", "SF"].includes(userType)) {
        let allChapters: any = await getNextChaptersData(qidClass, qidLocale, qidCategory, subject);
        allChapters = [...allChapters, ...allChapters];
        const allChaptersName: string[] = allChapters.map(chapter => chapter.master_chapter);
        const chaptersIndex = allChaptersName.indexOf(chapterName);
        if (chaptersIndex === -1) {
            console.log("chapter not in table", chapterName);
            return null;
        }
        const next3chapters = allChapters.slice(chaptersIndex, chaptersIndex + 3);
        const next3chaptersIds = next3chapters.map(chapter => chapter.id);
        return next3chaptersIds;
    } else if (userType === "SF_LF") {
        const today = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");

        const [sfChapter, lfChapter] = await Promise.all([
            redis_hp.lrangeAsync(`u:rec:${studentId}:SF:${today}`, 0, 1),
            redis_hp.lrangeAsync(`u:rec:${studentId}:LF:${today}`, 0, 1),
        ]);

        let allChapters: any = await getNextChaptersData(qidClass, qidLocale, qidCategory, subject);
        allChapters = [...allChapters, ...allChapters];

        const allChaptersName: string[] = allChapters.map(chapter => chapter.master_chapter);
        const sfChaptersIndex = allChaptersName.indexOf(sfChapter[0]);
        const lfChaptersIndex = allChaptersName.indexOf(lfChapter[0]);

        if (sfChaptersIndex === -1 || lfChaptersIndex === -1) {
            return null;
        }
        const nextChaptersSF = allChapters.slice(sfChaptersIndex, sfChaptersIndex + 1);
        const next2chaptersLF = allChapters.slice(lfChaptersIndex, lfChaptersIndex + 2);

        const next3chapters = [...next2chaptersLF, ...nextChaptersSF];
        const next3chaptersIds = next3chapters.map(chapter => chapter.id);

        return next3chaptersIds;
    }

    return null;
}

async function getNextChapterQids(chapterIds) {
    const videosPromise = [];
    for (let i = 0; i < chapterIds.length; i++) {
        const limit = (i === chapterIds.length - 1) ? 4 : 3;
        videosPromise.push(getNextChapterQidsData(chapterIds[i], limit));
    }
    const videosData = await Promise.all(videosPromise);
    const returnData = [];
    for (let i = 0; i < videosData.length; i++) {
        returnData.push(...videosData[i]);
    }
    return returnData;
}

export async function onMsg(msg: { data: any }[]) {
    for (let i = 0; i < msg.length; i++) {
        const { userType, videoType, chapterName, studentId, questionId, subject } = msg[i].data;

        const nextChapterIds = await getNextChapter(studentId, userType, videoType, questionId, subject, chapterName,);

        if (_.isEmpty(nextChapterIds)) {
            console.log("no chapter data");
            continue;
        }

        const nextChapterQids = await getNextChapterQids(nextChapterIds);

        if (_.isEmpty(nextChapterQids)) {
            console.log("no qids data", nextChapterIds);
            continue;
        }

        // const carouselData = await generateCarouselData(nextChapterQids);
        const carouselQids = nextChapterQids.map(qid => ({
            id: qid.resource_reference,
            subject: qid.subject,
        }));

        if (!_.isEmpty(carouselQids)) {
            await redis.multi()
                .set(`u:rec:${studentId}:carousel`, JSON.stringify(carouselQids))
                .expire(`u:rec:${studentId}:carousel`, HASH_EXPIRY_ONE_DAY * 3)
                .execAsync();
        }
    }
}

export const opts = [{
    topic: "consumer.homepage.recommendation",
    fromBeginning: false,
    numberOfConcurrentPartitions: 5,
}];
