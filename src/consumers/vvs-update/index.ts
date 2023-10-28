import moment from "moment";
import { mysql } from "../../modules";
import { InputMeta } from "../../interfaces";

async function updateVVS(video_time, engage_time, updated_at, view_id) {
    const sql = "UPDATE video_view_stats SET ? WHERE view_id = ?";
    return mysql.writeCon.query(sql, [{ video_time, engage_time, updated_at }, view_id]);
}

async function getVideoViewStatByReferId(view_id): Promise<any> {
    const sql = "SELECT view_id from video_view_stats WHERE is_back = 1 AND refer_id = ? order by view_id desc limit 1";
    return mysql.con.query(sql, [view_id]).then(x => x[0][0]);
}

async function getVideoViewStatById(view_id): Promise<any> {
    const sql = "SELECT * from video_view_stats WHERE view_id = ?";
    return mysql.con.query(sql, [view_id]).then(x => x[0][0]);
}

async function insertVVS(data) {
    const sql = "INSERT INTO video_view_stats SET ?";
    return mysql.writeCon.query(sql, [data]);
}

export async function onMsg(msg: { data: { viewId: string; engageTime: string; videoTime: string; isback: string }; meta: InputMeta }[]) {
    for (let i = 0; i < msg.length; i++) {
        const { meta, data } = msg[i];
        let { videoTime, engageTime } = data;
        const ts = moment(meta.ts).add(5, "h").add("30", "minute").toISOString().replace("T", " ").substr(0, 19);

        const vvsRow = await getVideoViewStatById(data.viewId);
        if (!vvsRow) {
            throw new Error(`No VVS row ${data.viewId}`);
        }
        if (vvsRow.engage_time >= engageTime) {
            engageTime = vvsRow.engage_time;
        }
        if (vvsRow.video_time >= videoTime) {
            videoTime = vvsRow.video_time;
        }
        if (data.isback === "1") {
            // check if view exist with is_back
            const isBackRow: { view_id: number } = await getVideoViewStatByReferId(data.viewId);
            if (isBackRow) {
                await updateVVS(videoTime, engageTime, ts, isBackRow.view_id);
            } else {
                const viewData = {
                    student_id: vvsRow.student_id,
                    question_id: vvsRow.question_id,
                    answer_id: vvsRow.answer_id,
                    answer_video: vvsRow.answer_video,
                    engage_time: engageTime,
                    video_time: videoTime,
                    parent_id: vvsRow.parent_id,
                    is_back: 1,
                    session_id: vvsRow.session_id,
                    tab_id: vvsRow.tab_id,
                    ip_address: vvsRow.ip_address,
                    source: vvsRow.source,
                    refer_id: data.viewId,
                    created_at: ts,
                    updated_at: ts,
                };
                await insertVVS(viewData);
            }
        }
        if (engageTime > vvsRow.engage_time || videoTime > vvsRow.video_time) {
            await updateVVS(videoTime, engageTime, ts, data.viewId);
        }
    }
}

export const opts = [{
    topic: "api-server.vvs.update",
    fromBeginning: true,
    numberOfConcurrentPartitions: 2,
}];
