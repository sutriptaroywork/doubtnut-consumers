import moment from "moment";
import { mysql } from "../../modules";

async function updateDataAfterUninstall(studentId) {
    const sql = "update students set gcm_reg_id = null, is_uninstalled = 1 where student_id = ? ";
    return mysql.writeCon.query(sql, [studentId]);
}

async function insertDataAfterUninstall(studentId) {
    const sql = `insert into retarget_student_churn(student_id, can_be_targeted, loss_type, uninstall_timestamp, created_at) values(?,1,1,'${moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD HH:mm:ss")}', '${moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD HH:mm:ss")}')`;
    console.log(sql, studentId);
    return mysql.writeCon.query(sql, [studentId]);
}

export async function onMsg(msg: { data: { userId: string } }) {
    try {
        console.log("this is data@@", msg[0].data.data.userDim.userId);
        const studentId = msg[0].data.data.userDim.userId;
        if (studentId) {
            await updateDataAfterUninstall(studentId);
            await insertDataAfterUninstall(studentId);
            console.log("this is done");
        }

    } catch (e) {
        console.error("Error:: ", e);
    }
}

export const opts = [{
    topic: "hook-server.events.uninstall", // Topic Property
    fromBeginning: true, // Topic Property
}];
