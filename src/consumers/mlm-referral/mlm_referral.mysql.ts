import { mysql } from "../../modules";

async function getCouponClaimsFromPaymentReferralEntryByIdAndCoupon(couponCode: string, id: number) {
    // Last payment_referral_entries id before Referral count reset is ...722
    const sql = "SELECT id, payment_info_id, coupon_code FROM payment_referral_entries where coupon_code = ? and id <= ? and id > 722";
    return mysql.singleQueryTransaction(sql, [couponCode, id]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function getCouponClaimsFromPaymentInfoBeforeStealthLive(couponCode: string) {
    // First payment_info_id entry in payment_referral_entries is 3577163
    const sql = "SELECT id from payment_info where coupon_code = ? and status = 'SUCCESS' and id < 3577163";
    return mysql.singleQueryTransaction(sql, [couponCode]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function getPaymentReferralEntryByPaymentInfoId(paymentInfoId: number){
    const sql = "SELECT id, payment_info_id, coupon_code FROM payment_referral_entries where payment_info_id = ?";
    return mysql.singleQueryTransaction(sql, [paymentInfoId]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function getPaymentEntryById(paymentInfoId: number) {
    const sql = "SELECT pi.id, pi.coupon_code, pi.student_id, s.mobile, pr.id as payment_referral_id from payment_info pi JOIN students s ON s.student_id = pi.student_id JOIN payment_referral_entries pr on pr.payment_info_id = pi.id WHERE pi.id = ? and pi.status = 'SUCCESS'";
    return mysql.singleQueryTransaction(sql, [paymentInfoId]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function getStudentDetailsByStudentReferralCoupon(couponCode: string) {
    const sql = "SELECT a.*, b.mobile FROM (SELECT * FROM student_referral_course_coupons WHERE coupon_code = ? and is_active = 1) AS a JOIN students AS b ON a.student_id=b.student_id";
    return mysql.singleQueryTransaction(sql, [couponCode]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function getDnPropertyByBucket(bucketName: string) {
    const sql = "SELECT name, value from dn_property WHERE bucket = ? and is_active = 1";
    return mysql.con.query(sql, [bucketName]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function createEntryInReferralTree(obj: any) {
    const sql = "INSERT INTO referral_mlm SET ?";
    return mysql.writeCon.query(sql, [obj]);
}

async function updateEntryInReferralTreeById(obj: any, id: number) {
    const sql = "UPDATE referral_mlm SET ? where id = ?";
    return mysql.writeCon.query(sql, [obj, id]);
}

async function getReferralTreeTableEntryByStudentId(studentId: string) {
    const sql = "SELECT t.*, s.mobile from referral_mlm t join students s on s.student_id = t.student_id where t.student_id = ? order by id desc";
    return mysql.singleQueryTransaction(sql, [studentId]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function getReferralTreeTableEntryById(id: number) {
    const sql = "SELECT t.*, s.mobile from referral_mlm t join students s on s.student_id = t.student_id where t.id = ? and t.is_active = 1 order by t.id desc";
    return mysql.singleQueryTransaction(sql, [id]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function createDisbursmentEntry(obj: any) {
    const sql = "INSERT INTO student_referral_disbursement SET ?";
    return mysql.writeCon.query(sql, [obj]);
}

async function checkDisbursmentEntry(studentId: string, paymentInfoId: number) {
    const sql = "SELECT id, payment_info_id, amount, invitor_student_id from student_referral_disbursement where payment_info_id = ? and invitor_student_id = ?";
    return mysql.singleQueryTransaction(sql, [paymentInfoId, studentId]).then(value => JSON.parse(JSON.stringify(value[0])));
}

async function checkUserForCEOReferralProgramByStudentId(studentId: string) {
    /**
     * Referral CEO Program TG id is 1710
     */
    const tgSql = "SELECT tg.sql FROM target_group tg WHERE tg.id = 1710";
    const resultTgSql = await mysql.con.query(tgSql).then(value => JSON.parse(JSON.stringify(value[0])));

    /**
     * First case assuming there exists a ps table in the tg query on which we check for user -79ms
     * Second case someone changed the query and there no longer exists a ps table in tg query running the scan through whole list for student_id - 1.2s
     */
    // Join in query
    let result;
    console.log(resultTgSql);
    try {
        const userSql = `${resultTgSql[0].sql.replace(/;/g, "")} AND ps.student_id = ?`;
        result = await mysql.singleQueryTransaction(userSql, [studentId]).then(value => JSON.parse(JSON.stringify(value[0])));
    } catch (e) {
        const userSql = `SELECT * FROM (${resultTgSql[0].sql.replace(/;/g, "")}) AS a WHERE a.student_id = ?`;
        result = await mysql.singleQueryTransaction(userSql, [studentId]).then(value => JSON.parse(JSON.stringify(value[0])));
    }
    console.log(result);
    return result;
}

async function getDisbursalEntriesCountForParent(studentId) {
    const sql = "SELECT * from student_referral_disbursement where invitor_student_id = ? and entry_for like 'parent%'";
    return mysql.singleQueryTransaction(sql, [studentId]).then(value => JSON.parse(JSON.stringify(value[0])));
}

export const referralMysql = {
    getCouponClaimsFromPaymentReferralEntryByIdAndCoupon,
    getCouponClaimsFromPaymentInfoBeforeStealthLive,
    getPaymentReferralEntryByPaymentInfoId,
    getPaymentEntryById,
    getStudentDetailsByStudentReferralCoupon,
    getDnPropertyByBucket,
    createEntryInReferralTree,
    updateEntryInReferralTreeById,
    getReferralTreeTableEntryByStudentId,
    getReferralTreeTableEntryById,
    createDisbursmentEntry,
    checkDisbursmentEntry,
    checkUserForCEOReferralProgramByStudentId,
    getDisbursalEntriesCountForParent,
};
