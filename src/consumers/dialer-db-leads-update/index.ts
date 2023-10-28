import { mysql } from "../../modules";

async function checkLeadQuery(studentId): Promise<any> {
    const sql = "SELECT * FROM leads_cstm WHERE student_id_c=?";
    return mysql.con.query(sql, [studentId]).then(x => x[0]);
}

async function updateCustomLeads(student_class, student_id) {
    const sql = "UPDATE leads_cstm SET student_class_c = ? WHERE student_id_c =?";
    return mysql.writeCon.query(sql, [student_class, student_id]);
}

export async function onMsg(msg: { data: { student_id: number; student_class: string }}[]) {
    console.log(msg);
    for (let i = 0; i < msg.length; i++) {
        try {
        const { data } = msg[i];
        const { student_id, student_class } = data;

            if (student_id){
                // SELECT * FROM leads_cstm WHERE student_id_c
                const checkleadQuery = await checkLeadQuery(student_id);
                if (checkleadQuery.length > 0){
                    // UPDATE leads_cstm SET student_class_c
                    const results = await updateCustomLeads(student_class, student_id);
                } else {
                console.log(`Record not found for Student ID ${student_id}`);
                }
            }

        } catch (err) {
            console.log(err);
        }
    }
}

export const opts = [{
    topic: "api-server.dialer.lead.update",
    fromBeginning: false,
    numberOfConcurrentPartitions: 1,
}];
