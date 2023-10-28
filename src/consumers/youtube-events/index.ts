import _ from "lodash";
import { mysql } from "../../modules";


function getCourseDetailByAssortmentId(assortmentId) {
    const sql = "select display_name, display_description from course_details cd where assortment_id = ? and assortment_type = 'resource_video'";
    return mysql.con.query(sql, [assortmentId]).then(x => x[0]);
}

function getCourseResourceByResourceId(resourceId) {
    const sql = "select * from course_resources cr where id = ?";
    return mysql.con.query(sql, [resourceId]).then(x => x[0]);
}

function insertYoutubeEvent(youtubeEventObj) {
    const sql = "insert ignore into youtube_event set ?";
    return mysql.writeCon.query(sql, [youtubeEventObj]).then(x => x[0]);
}

const VIDEO_TYPES = [1];
export async function onMsg(msg: any) {
    for (let m = 0; m < msg.length; m++) {
        // {
        //     "id": 7057615,
        //     "assortment_id": 1234582,
        //     "course_resource_id": 606351,
        //     "resource_type": "resource",
        //     "name": "Problems on Combination of Capacitors",
        //     "schedule_type": "scheduled",
        //     "live_at": "2022-06-08T13:45:00Z",
        //     "created_at": "2022-06-08T04:37:38Z",
        //     "is_trial": 797940,
        //     "is_replay": 0,
        //     "old_resource_id": 2429851,
        //     "resource_name": null,
        //     "batch_id": 1,
        //     "updated_at": "2022-06-08T04:37:38Z",
        //     "__deleted": "false"
        //  }
        try {
            const { resource_type: resourceType, __deleted: isDeleted, assortment_id: assortmentId, course_resource_id: resourceId, live_at: liveAt } = msg[m];
            if ((resourceType != "resource") || (isDeleted == "true")) {
                console.log("Not resource");
                continue;
            }
            if (!liveAt) {
                console.log("live at is null");
                continue;
            }
            const courseDetailEntry: any = await getCourseDetailByAssortmentId(assortmentId);
            const courseResourceEntry: any = await getCourseResourceByResourceId(resourceId);

            if (!courseDetailEntry.length || !courseResourceEntry.length) {
                console.log("No cd or cr entry");
                continue;
            }
            if (!VIDEO_TYPES.includes(courseResourceEntry[0].resource_type)) {
                console.log("Not video resource type");
                continue;
            }
            const eventData = {
                assortment_id: assortmentId,
                resource_reference: courseResourceEntry[0].resource_reference,
                resource_type: courseResourceEntry[0].resource_type,
                live_at: liveAt,
                title: courseResourceEntry[0].display,
                description: courseResourceEntry[0].description,
            };
            console.log("eventData", eventData);
            await insertYoutubeEvent(eventData);
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
