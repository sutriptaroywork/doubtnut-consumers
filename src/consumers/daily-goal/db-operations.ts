import { mysql } from "../../modules";

async function insertDailyDoubt(data) {
    const sql = "INSERT INTO daily_doubt SET ?";
    return mysql.writeCon.query(sql, [data]);
}

async function getFormulaSheets(topicName, classVal): Promise<any> {
    const sql = "SELECT id, level1, level2, location, img_url FROM pdf_download WHERE package = 'FORMULA SHEET' AND status = 1 AND level2 = ? AND class = ?";
    return mysql.con.query(sql, [topicName, classVal]).then(x => x[0]);
}

async function getChapterAliasByChapter(chapter): Promise<any> {
    const sql = "SELECT q.question_id, c.* FROM questions q RIGHT JOIN chapter_alias_all_lang c ON q.chapter = c.chapter WHERE q.chapter = ? LIMIT 5";
    return mysql.con.query(sql, [chapter]).then(x => x[0]);
}

async function getPreviousTopicByDate(studentId, topicName, currentDate): Promise<any> {
    const sql = "SELECT id FROM daily_doubt WHERE sid = ? AND topic = ? AND date(date) != ? ORDER BY date DESC LIMIT 1";
    return mysql.con.query(sql, [studentId, topicName, currentDate]).then(x => x[0]);
}

async function getQuestionList(topicId): Promise<any> {
    const sql = "SELECT type, data_list as resource_id from daily_doubt_resources WHERE topic_reference = ?";
    return mysql.con.query(sql, [topicId]).then(x => x[0]);
}

async function getTodaysTopics(studentId): Promise<any> {
    const sql = "SELECT * FROM daily_doubt WHERE sid = ? AND date(date) = CURDATE()";
    return mysql.con.query(sql, [studentId]).then(x => x[0]);
}

async function getByQuestionId(question_id): Promise<any> {
    // const sql = "select * from questions where question_id = ?";
    const sql = "SELECT b.*, a.*, c.id as 'text_solution_id',c.sub_obj,c.opt_1,c.opt_2,c.opt_3,c.opt_4,c.answer as text_answer,c.solutions as text_solutions FROM (Select * from questions where question_id=?) as a left join answers as b on a.question_id = b.question_id left join text_solutions as c on c.question_id=a.question_id order by b.answer_id desc limit 1";
    return mysql.con.query(sql, [question_id]).then(x => x[0]);
}

async function getTopicDetails(studentId, topic): Promise<any> {
    const sql = "SELECT * FROM daily_doubt where sid = ? AND topic = ? AND date(date) = date(NOW())";
    return mysql.con.query(sql, [studentId, topic]).then(x => x[0]);
}

async function updateQuestionId(topicId, questionIdList) {
    const sql = "UPDATE daily_doubt SET qid_list = ? where id = ?";
    return mysql.writeCon.query(sql, [questionIdList, topicId]);
}

export const dbOperations = {
    insertDailyDoubt,
    getFormulaSheets,
    getChapterAliasByChapter,
    getPreviousTopicByDate,
    getQuestionList,
    getTodaysTopics,
    getByQuestionId,
    getTopicDetails,
    updateQuestionId,
};
