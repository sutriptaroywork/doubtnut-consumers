/* eslint-disable no-underscore-dangle */

import _ from "lodash";
import axios from "axios";
import moment from "moment";
import { mysql } from "../../modules";

async function updateInGraph(row) {
    row.is_deleted = row.__deleted;
    row.updated_at = moment(row.updated_at).unix();

    const { data } = await axios({
        method: "post",
        url: "https://micro.internal.doubtnut.com/liveclass-cdc/course-resources",
        headers: {
            "Content-Type": "application/json",
        },
        data: {
            row: _.omit(row, ["__deleted"]),
        },
    });
}
export async function onMsg(msg: any) {
    const db = {
        mysql: {
            read: mysql.con,
            write: mysql.writeCon,
        },
    };
    // msg is array of
    /* {
        assortment_id: 16844,
        batch_id: 2
    }*/
    for (let m = 0; m < msg.length; m++) {
        try {
            if (moment().diff(moment(msg[m].updated_at), "days") < 3 || msg[m].__deleted || moment().diff(moment(msg[m].created_at), "days") < 3) {

                const courseResourceParamsNeeded = [
                    "id", "resource_reference",
                    "resource_type", "subject",
                    "topic", "expert_name",
                    "expert_image", "q_order",
                    "class", "meta_info",
                    "tags", "name",
                    "display", "description",
                    "chapter", "chapter_order",
                    "exam", "board",
                    "ccm_id", "book",
                    "faculty_id", "stream_start_time",
                    "image_url", "locale",
                    "vendor_id", "duration",
                    "rating", "old_resource_id",
                    "stream_end_time", "stream_push_url",
                    "stream_vod_url", "stream_status",
                    "old_detail_id", "lecture_type",
                    "is_active", "updated_at",
                    "__deleted",
                ];
                const courseResourceParams = _.pick(msg[m], courseResourceParamsNeeded);
                updateInGraph(courseResourceParams);
            }

        } catch (err) {
            console.log(err);
        } finally {
            console.log(`the script successfully ran at ${new Date()}`);
        }
    }

}

export const opts = [{
    topic: "warehouse.mysql.course-resources",
    fromBeginning: true,
}];
