import moment from "moment";
import axios from "axios";
import { mysql, redis  } from "../../modules";
import { InputMeta } from "../../interfaces";

async function insertCouponWinners(data) {
    const sql = "INSERT INTO student_renewal_target_group SET ?";
    return mysql.writeCon.query(sql, [data]);
}

async function checkCouponWinner(studentId): Promise<any> {
    const sql = 'select * from student_renewal_target_group where student_id=? and coupon like "PAIDCHAMPIONSHIP%"';
    return mysql.con.query(sql, [studentId]).then(x => x[0]);
}

async function updateCouponWinner(id, coupon) {
    const sql = "update student_renewal_target_group set coupon=? where id=?";
    return mysql.writeCon.query(sql, [coupon, id]);
}

async function getResourcesCountFromCourseAssortment(assortmentID, batchID, startDate, endDate): Promise<any> {
    const sql = 'SELECT count(*) as count from (SELECT assortment_id, course_resource_id,resource_type,name FROM course_resource_mapping where assortment_id=? and resource_type = \'assortment\') as a left join (SELECT assortment_id, course_resource_id,resource_type,name FROM course_resource_mapping) as b on a.course_resource_id=b.assortment_id left join course_resource_mapping as c on b.course_resource_id = c.assortment_id and c.resource_type=\'assortment\' left join course_details as d on c.course_resource_id = d.assortment_id left join course_resource_mapping e on c.course_resource_id=e.assortment_id and e.resource_type = \'resource\' left join course_resources cr on cr.id=e.course_resource_id where d.assortment_type="resource_video" and cr.resource_type in (1,8) and e.batch_id=? and e.live_at between ? and ? group by d.assortment_type';
    return mysql.con.query(sql, [assortmentID, batchID, startDate, endDate]).then(x => x[0]);
}

async function setResourcesCountFromAssortment(assortmentID, batchID, endDate, data) {
    return redis.setAsync(`course_resource_video_count:${assortmentID}_${batchID}_${moment(endDate).format("DD")}`, JSON.stringify(data), "Ex", 60 * 60 * 2); // 2 hours
}

async function getResourcesCountFromAssortment(assortmentID, batchID, endDate) {
    return redis.getAsync(`course_resource_video_count:${assortmentID}_${batchID}_${moment(endDate).format("DD")}`);
}

async function getUserAttendance(studentID, assortmentID) {
    return redis.getAsync(`user_assortment_attendance:${assortmentID}_${studentID}`);
}

async function setUserAttendance(studentID, assortmentID, monthEndDiff) {
    return redis.setAsync(`user_assortment_attendance:${assortmentID}_${studentID}`, 1, "EX", monthEndDiff);
}

async function incrementUserAttendance(studentID, assortmentID) {
    return redis.incr(`user_assortment_attendance:${assortmentID}_${studentID}`);
}

async function getResourcesCountFromCourseAssortmentContainer(assortmentID, batchID, startDate, endDate) {
    let data = await getResourcesCountFromAssortment(assortmentID, batchID, endDate);
    if (data) {
        return JSON.parse(data);
    }
    data = await getResourcesCountFromCourseAssortment(assortmentID, batchID, startDate, endDate);
    setResourcesCountFromAssortment(assortmentID, batchID, endDate, data);
    return data;
}

async function getUserAttendanceContainer(studentID, assortmentID, monthEndDiff) {
    let userAttendance = await getUserAttendance(studentID, assortmentID);
    if (!userAttendance) {
        setUserAttendance(studentID, assortmentID, monthEndDiff);
        return 1;
    } else {
        userAttendance = await incrementUserAttendance(studentID, assortmentID);
    }
    return JSON.parse(userAttendance);
}

async function updatePercolateIndex(couponList) {
    const params = JSON.stringify({coupon_codes:couponList});
    const { data } = await axios({
        method: "post",
        url: "https://gateway.doubtnut.com/coupons/api/v1/update-coupon-details",
        headers: {
            "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NzI0NTE1LCJpYXQiOjE2MzEyOTA0NjQsImV4cCI6MTYzMTg5NTI2NH0.PR25hvTTXeYe2M1tLUG7PCWhbTOprNbrzGdrD9u7HQ4",
            "Content-Type": "application/json",
        },
        data: params,
    });
}

export async function onMsg(msg: { data: { studentId: string; assortmentId: string; batchId: string }; meta: InputMeta }[]) {
    for (let i = 0; i < msg.length; i++) {
        const { meta, data } = msg[i];
        const { studentId, assortmentId, batchId } = data;
        const monthStart = moment().add(5, "hours").add(30, "minutes").startOf("month").format("YYYY-MM-DD HH:mm:ss");
        const today =  moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD HH:mm:ss");
        const monthEnd = moment().add(5, "hours").add(30, "minutes").endOf("month");
        const monthEndDiff = monthEnd.diff(moment().add(5, "hours").add(30, "minutes"));
        // const start = moment(paidStudents[0].start_date).diff(monthStart) > 0 ? paidStudents[0].start_date : monthStart.format('YYYY-MM-DD HH:mm:ss');
        const totalResources = await getResourcesCountFromCourseAssortmentContainer(assortmentId, batchId, monthStart, today);
        const totalClassesHappened = totalResources[0].count;
        const userAttendance = await getUserAttendanceContainer(studentId, assortmentId, monthEndDiff);
        const attendancePercentage = (parseInt(userAttendance, 10) * 100) / totalClassesHappened;
        if (attendancePercentage >= 50) {
            const coupon = attendancePercentage >= 90 ? "PAIDCHAMPIONSHIP50" : `${attendancePercentage >= 75 ? "PAIDCHAMPIONSHIP25" : "PAIDCHAMPIONSHIP10"}`;
            const isAlreadyWinner = await checkCouponWinner(studentId);
            if (!isAlreadyWinner.length) {
                const insertObj = {
                    student_id: studentId,
                    coupon,
                    is_active: 1,
                };
                await insertCouponWinners(insertObj);
                updatePercolateIndex([coupon]);
            } else if (isAlreadyWinner.length && isAlreadyWinner[0].coupon !== "") {
                await updateCouponWinner(isAlreadyWinner[0].id, coupon);
                updatePercolateIndex([coupon]);
            }
        }
    }
}

export const opts = [{
    topic: "api-server.championship.coupon",
    fromBeginning: true,
}];
