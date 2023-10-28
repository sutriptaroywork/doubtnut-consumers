
import _ from "lodash";
import { mysql } from "../../modules";

import {
    getCourseFromResourceId,
    getChildAssortment,
    getType,
    getAssortmentType,
    checkAssortmentWithBatchInPackages,
    createSubjectPackage,
    createChapterPackage,
} from "../../helpers/package-creation";

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
            const { assortment_id, batch_id } = msg[m];

            const courseDetails = await getCourseFromResourceId(db.mysql.read, assortment_id);

            // continue if no course found
            if (!_.get(courseDetails, "[0].assortment_id", null)) {
                console.log("no course found for resource", assortment_id);
                continue;
            }
            const checkCoursePackage = await checkAssortmentWithBatchInPackages(db.mysql.read, courseDetails[0].assortment_id, batch_id);

            // continue if course package not exists
            if (!_.get(checkCoursePackage, "[0].id", null)) {
                console.log("course package not exists");
                continue;
            }

            // get subject assortments
            const subjectAssortments = await getChildAssortment(db.mysql.read, courseDetails[0].assortment_id);

            // continue if no subject assortment exists
            if (!_.get(subjectAssortments, "[0].assortment_id", null)) {
                console.log("subjects not exists for course", courseDetails[0].assortment_id);
                continue;
            }
            for (let i = 0; i < subjectAssortments.length; i++) {
                if (subjectAssortments[i].assortment_type !== "subject") {
                    console.log("assortment not a subject", subjectAssortments[i].course_resource_id);
                    continue;
                }
                const subjectAid = subjectAssortments[i].course_resource_id;

                const checkSubjectPackage = await checkAssortmentWithBatchInPackages(db.mysql.read, subjectAid, batch_id);
                if (_.get(checkSubjectPackage, "[0].id", null)) {
                    console.log("subject package exists", subjectAid);
                } else {
                    console.log("creating subject package ", subjectAid);
                    const type = await getType(db.mysql.read, subjectAid);
                    const resourceType = await getAssortmentType(db.mysql.read, subjectAid);

                    if (type.length && resourceType.length) {
                        await createSubjectPackage({ db, type, resourceType, assortment_id: subjectAid, batch_id });
                    }
                }
                // get chapter assortments
                const chapterAssortments = await getChildAssortment(db.mysql.read, subjectAssortments[i].course_resource_id);
                for (let j = 0; j < chapterAssortments.length; j++) {
                    if (chapterAssortments[j].assortment_type !== "chapter") {
                        console.log("assortment not a subject", chapterAssortments[j].course_resource_id);
                        continue;
                    }
                    const chapterAid = chapterAssortments[j].course_resource_id;
                    const checkChapterPackage = await checkAssortmentWithBatchInPackages(db.mysql.read, chapterAid, batch_id);
                    if (_.get(checkChapterPackage, "[0].id", null)) {
                        console.log("chapter package exists", chapterAid);
                        continue;
                    } else {
                        console.log("creating chapter package ", chapterAid);

                        const type = await getType(db.mysql.read, chapterAid);
                        const resourceType = await getAssortmentType(db.mysql.read, chapterAid);

                        if (type.length && resourceType.length) {
                            await createChapterPackage({ db, type, resourceType, batch_id });
                        }
                    }
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
    topic: "package.creation.new.batch",
    fromBeginning: true,
}];
