import axios from "axios";
import _ from "lodash";
import moment from "moment";
import { mysql, redis } from "../../modules";


function getCourseResourceById(resourceId) {
    const sql = "select resource_reference, resource_type from course_resources cr where id = ?";
    return mysql.con.query(sql, [resourceId]).then(x => x[0]);
}
async function updateInGraph(row) {
    // eslint-disable-next-line no-underscore-dangle
    row.is_deleted = row.__deleted;
    row.updated_at = moment(row.updated_at).unix();
    row.live_at = moment(row.live_at).unix();
    const { data } = await axios({
        method: "post",
        url: "https://micro.internal.doubtnut.com/liveclass-cdc/course-resource-mapping",
        headers: {
            "Content-Type": "application/json",
        },
        data: {
            row: _.omit(row, ["__deleted"]),
        },
    });
}

const VID_RESOURCE_TYPES = [1, 4, 8];

export async function onMsg(msg: any) {

    for (let m = 0; m < msg.length; m++) {
        try {
            try {
                if (moment().diff(moment(msg[m].updated_at), "days") < 3 || msg[m].is_deleted || moment().diff(moment(msg[m].created_at), "days") < 3) {
                    const courseResourceMappingParamsNeeded = [
                        "id",
                        "assortment_id",
                        "course_resource_id",
                        "resource_type",
                        "schedule_type",
                        "live_at",
                        "is_replay",
                        "batch_id",
                        "updated_at",
                        "__deleted",
                    ];
                    const courseResourceMappingParams = _.pick(msg[m], courseResourceMappingParamsNeeded);
                    updateInGraph(courseResourceMappingParams);
                }
            } catch (err) {
                console.log(err);
            }
            const { resource_type: resourceType } = msg[m];
            if (resourceType !== "resource") {
                console.log("type is assortment");
                return;
            } else {
                console.log("type is resource");
                const { course_resource_id: resourceId } = msg[m];

                const courseResource = await getCourseResourceById(resourceId);
                const courseResourceType = _.get(courseResource, "[0].resource_type", -1);
                const courseResourceReference = _.get(courseResource, "[0].resource_reference", -1);

                console.log("resource type", courseResourceType);
                console.log("resource reference", courseResourceReference);

                if (VID_RESOURCE_TYPES.includes(parseInt(courseResourceType))) {
                    console.log("deleting cache");
                    await redis.delAsync(`all_course_assortments_by_qId:${courseResourceReference}`);
                    await redis.delAsync(`course_assortment_resource_${courseResourceReference}`);
                    await redis.delAsync(`course_assortment_resource_${courseResourceReference}_undefined`);
                    await Promise.all([6, 7, 8, 9, 10, 11, 12, 13, 14].map(cl => {
                        redis.delAsync(`course_assortment_resource_${courseResourceReference}_${cl}`);
                    }));
                    console.log("deleted");
                }
            }

        } catch (err) {
            console.log(err);
        } finally {
            console.log(`the script successfully ran at ${new Date()}`);
        }
    }

}
export const opts = [{
    topic: "warehouse.mysql.course-resource-mapping",
    fromBeginning: false,
}];
