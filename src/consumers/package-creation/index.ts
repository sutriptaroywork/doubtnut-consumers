/* eslint-disable eqeqeq */
/* eslint-disable no-underscore-dangle */
import _ from "lodash";
import moment from "moment";
import axios from "axios";
import { mysql, publishRaw } from "../../modules";

import {
    getClassDurationCount,
    getClassPdf,
    getType,
    getCourseResource,
    toTitleCase,
    getAssortmentType,
    getDuration,
    insertPackage,
    insertVariants,
    checkAssortmentInPackages,
    checkAssortmentWithBatchInPackages,
    createSubjectPackage,
    createChapterPackage,
} from "../../helpers/package-creation";
// cd
// assortment_id,class,ccm_id,display_name,display_description,category,display_image_rectangle,display_image_square,deeplink,max_retail_price,final_price,meta_info,max_limit,is_active,check_okay,start_date,end_date,expiry_date,priority,dn_spotlight,promo_applicable,minimum_selling_price,parent,is_free,assortment_type,display_icon_image,faculty_avatars,demo_video_thumbnail,demo_video_qid,rating,subtitle,sub_assortment_type,year_exam,category_type,is_active_sales,is_show_web,updated_at

const RESOURCE_TYPES = ["resource_video", "resource_pdf", "resource_test"];
const RESOURCE_TYPES_ASSORTMENT = ["class", "subject", "chapter"];
const BASE_PRICE_MAP = {
    resource_video: 29,
    resource_pdf: 15,
    resource_test: 15,
};

const DISPLAY_PRICE_MAP = {
    resource_video: 29,
    resource_pdf: 5,
    resource_test: 5,
};

const MIN_LIMIT_MAP = {
    resource_video: 19,
    resource_pdf: 5,
    resource_test: 5,
};


async function updateInGraph(row) {
    row.is_deleted = row.__deleted;
    row.updated_at = moment(row.updated_at).unix();
    row.start_date = moment(row.start_date).unix();
    row.end_date = moment(row.end_date).unix();
    row.expiry_date = moment(row.expiry_date).unix();
    const { data } = await axios({
        method: "post",
        url: "https://micro.internal.doubtnut.com/liveclass-cdc/course-details",
        headers: {
            "Content-Type": "application/json",
        },
        data: {
            row: _.omit(row, ["__deleted"]),
        },
    });
    console.log(data);
}

export async function onMsg(msg: any) {
    const db = {
        mysql: {
            read: mysql.con,
            write: mysql.writeCon,
        },
    };
    // msg is array of course_details row inserted, eg given below.
    /* {
    assortment_id: 16844,
    created_at: '2020-10-10T10:02:02Z',
    created_by: 'AS',
    class: 11,
    ccm_id: null,
    display_name: 'VIDEO | BIOLOGY | Biological Classification | 4. Kingdom Monera - Archaebacteria',
    display_description: 'VIDEO | BIOLOGY | Biological Classification | 4. Kingdom Monera - Archaebacteria',
    category: 'NEET',
    display_image_rectangle: 'https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/EFAE2CA6-F55E-9373-6B54-03BA46B4FC25.webp',
    display_image_square: null,
    deeplink: null,
    max_retail_price: 100,
    final_price: 80,
    meta_info: 'ENGLISH',
    max_limit: 120,
    is_active: 1,
    check_okay: 1,
    start_date: '2020-10-10T10:02:02Z',
    end_date: '2021-10-10T10:02:02Z',
    expiry_date: '2021-10-10T10:02:02Z',
    updated_at: '2021-09-26T06:21:50Z',
    updated_by: 'AS',
    priority: 1,
    dn_spotlight: 1,
    promo_applicable: 0,
    minimum_selling_price: 0,
    parent: null,
    is_free: 0,
    assortment_type: 'resource_video',
    display_icon_image: null,
    faculty_avatars: null,
    demo_video_thumbnail: null,
    demo_video_qid: null,
    rating: null,
    subtitle: null,
    sub_assortment_type: null,
    year_exam: null,
    category_type: null,
    is_active_sales: 1,
    __deleted: 'false'
  } */
    for (let m = 0; m < msg.length; m++) {
        try {
            try {
                if (moment().diff(moment(msg[m].updated_at), "days") < 3 || msg[m].__deleted || moment().diff(moment(msg[m].created_at), "days") < 3) {

                    const courseDetailsParamsNeeded = [
                        "assortment_id",
                        "class",
                        "ccm_id",
                        "display_name",
                        "display_description",
                        "category",
                        "display_image_rectangle",
                        "display_image_square",
                        "deeplink",
                        "max_retail_price",
                        "final_price",
                        "meta_info",
                        "max_limit",
                        "is_active",
                        "check_okay",
                        "start_date",
                        "end_date",
                        "expiry_date",
                        "priority",
                        "dn_spotlight",
                        "promo_applicable",
                        "minimum_selling_price",
                        "parent",
                        "is_free",
                        "assortment_type",
                        "display_icon_image",
                        "faculty_avatars",
                        "demo_video_thumbnail",
                        "demo_video_qid",
                        "rating",
                        "subtitle",
                        "sub_assortment_type",
                        "year_exam",
                        "category_type",
                        "is_active_sales",
                        "is_show_web",
                        "updated_at",
                        "__deleted",
                    ];

                    const courseDetailsParams = _.pick(msg[m], courseDetailsParamsNeeded);
                    updateInGraph(courseDetailsParams);
                }
            } catch (e) {
                // do nothing
            }
            const { assortment_id } = msg[m];
            const resourceCreated = msg[m];
            console.log(assortment_id);

            if (resourceCreated.is_free) {
                console.log("free");
                continue;
            }
            // const existPackage = await checkAssortmentInPackages(db.mysql.read, resourceCreated.assortment_id);

            // continue if package exists
            // if (_.get(existPackage[0], "id", null)) {
            //     console.log("exists");
            //     continue;
            // }

            const type_resource = resourceCreated.assortment_type;

            console.log(RESOURCE_TYPES.includes(type_resource));
            console.log(RESOURCE_TYPES_ASSORTMENT.includes(type_resource));

            if (RESOURCE_TYPES.includes(type_resource)) {
                console.log("it is resource");

                const assortmentType = await getAssortmentType(db.mysql.read, resourceCreated.assortment_id);
                const check = await checkAssortmentInPackages(db.mysql.read, resourceCreated.assortment_id);
                if (assortmentType && assortmentType.length > 0 && !check.length) {
                    console.log("creating resource package");
                    if (assortmentType[0].schedule_type === "recorded") {
                        const courseResource = await getCourseResource(db.mysql.read, assortmentType[0].course_resource_id);
                        const categoryArray = resourceCreated.category.split("|");
                        const nameArray = resourceCreated.display_name.split("|");
                        const duration = await getDuration(db.mysql.read, courseResource[0].resource_reference);


                        let name = "";
                        let description = "";
                        if (type_resource === "resource_video") {
                            name = `Single Video | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
                            let minutes = 10;
                            if (duration && duration.length > 0 && duration[0].duration !== null && duration[0].duration > 0) {
                                minutes = Math.floor(duration[0].duration / 60);
                            }
                            description = `${toTitleCase(courseResource[0].description.replace(/\|/g, " | "))} | ${minutes} mins+`;
                        } else if (type_resource === "resource_pdf") {
                            name = `${nameArray[0]} | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;

                        } else if (type_resource === "resource_test") {
                            name = `${nameArray[0]} | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
                        }

                        const obj = {
                            assortment_id: resourceCreated.assortment_id,
                            name,
                            description,
                            is_active: 1,
                            type: "subscription",
                            min_limit: MIN_LIMIT_MAP[type_resource],
                            duration_in_days: 365,
                            batch_id: assortmentType[0].batch_id,
                        };
                        const insert = await insertPackage(db.mysql.write, obj);
                        const varObj = {
                            package_id: insert.insertId,
                            base_price: BASE_PRICE_MAP[type_resource],
                            display_price: DISPLAY_PRICE_MAP[type_resource],
                            is_default: 1,
                            is_show: 1,
                            is_active: 1,
                        };
                        console.log(obj);
                        await insertVariants(db.mysql.write, varObj);
                    } else if (assortmentType[0].schedule_type === "scheduled") {
                        const courseResource = await getCourseResource(db.mysql.read, assortmentType[0].course_resource_id);
                        console.log(assortmentType);
                        const categoryArray = resourceCreated.category.split("|");
                        const nameArray = resourceCreated.display_name.split("|");

                        let name = "";
                        let description = toTitleCase(courseResource[0].description.replace(/\|/g, " | "));
                        if (type_resource === "resource_video") {
                            name = `Single Video | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
                            description = `${toTitleCase(courseResource[0].description.replace(/\|/g, " | "))} | ` + ` Class live on ${moment(assortmentType[0].live_at).format("dddd, MMMM Do YYYY, h:mm:ss a")}`;
                            if (courseResource && courseResource[0] && courseResource[0].expert_name !== null && courseResource[0].expert_name !== "") {
                                name = `${name} by ${toTitleCase(courseResource[0].expert_name)}`;
                            }
                        } else if (type_resource === "resource_pdf") {
                            name = `${nameArray[0]} | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;

                        } else if (type_resource === "resource_test") {
                            name = `${nameArray[0]} | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
                        }

                        const obj = {
                            assortment_id: resourceCreated.assortment_id,
                            name,
                            description,
                            is_active: 1,
                            type: "subscription",
                            min_limit: MIN_LIMIT_MAP[type_resource],
                            duration_in_days: 365,
                            batch_id: assortmentType[0].batch_id,

                        };
                        const insert = await insertPackage(db.mysql.write, obj);
                        const varObj = {
                            package_id: insert.insertId,
                            base_price: BASE_PRICE_MAP[type_resource],
                            display_price: DISPLAY_PRICE_MAP[type_resource],
                            is_default: 1,
                            is_show: 1,
                            is_active: 1,
                        };
                        await insertVariants(db.mysql.write, varObj);
                    }

                    if (assortmentType[0].batch_id > 1) {
                        const consumerData = {
                            assortment_id: resourceCreated.assortment_id,
                            batch_id: assortmentType[0].batch_id,
                        };
                        publishRaw("package.creation.new.batch", consumerData, 0);
                    }
                }
            }

            // ask this
            // const allFreeCourse = await getAllFreeCourse(db.mysql.read, assortment_id);
            // console.log(allFreeCourse);

            // for (let i = 0; i < allFreeCourse.length; i++) {
            if (RESOURCE_TYPES_ASSORTMENT.includes(type_resource)) {
                console.log("it is assortment");

                // const courseMapping = await getCourseMapping(db.mysql.read, assortment_id);
                // if (courseMapping && courseMapping.length > 0) {
                //     for (let j = 0; j < courseMapping.length; j++) {
                const type = await getType(db.mysql.read, resourceCreated.assortment_id);
                const resourceType = await getAssortmentType(db.mysql.read, resourceCreated.assortment_id);

                const check = await checkAssortmentWithBatchInPackages(db.mysql.read, resourceCreated.assortment_id, resourceType[0].batch_id);
                if (type && type.length && !check.length) {
                    console.log("creating assortment package");
                    if (type[0].assortment_type == "class") {
                        const categoryArray = type[0].category.split("|");
                        const name = `${toTitleCase(type[0].display_name)} for ${categoryArray[0].toUpperCase()}`;
                        const classPdf = await getClassPdf(db.mysql.read, type[0].assortment_id);
                        const obj = {
                            assortment_id: type[0].assortment_id,
                            name,
                            description: `Master complete class for ${categoryArray[0].toUpperCase()}`,
                            is_active: 1,
                            type: "subscription",
                            min_limit: 59,
                            duration_in_days: 365,
                            batch_id: resourceType[0].batch_id,

                        };
                        if (resourceType[0].schedule_type == "recorded") {
                            const duration = await getClassDurationCount(db.mysql.read, type[0].assortment_id);
                            if (duration && duration.length > 0 && duration[0].duration != 0 && duration[0].count != 0) {
                                obj.description = `${obj.description} | ${duration[0].count} classes - RECORDED - ${Math.floor(duration[0].duration)}+ minutes`;
                            }
                        } else {
                            obj.description = `${obj.description} | ` + "Regular Live Classes Every Week | Recording Available";
                        }
                        if (classPdf && classPdf.length > 0) {
                            obj.description = `${obj.description} - ${classPdf.length} PDF`;
                        }

                        console.log(obj);
                        const insert = await insertPackage(db.mysql.write, obj);
                        const varObj = {
                            package_id: insert.insertId,
                            base_price: 6000,
                            display_price: 5000,
                            is_default: 1,
                            is_show: 1,
                            is_active: 1,
                        };
                        await insertVariants(db.mysql.write, varObj);
                        // await updateAssortment(type[0].assortment_id);
                    } else if (type[0].assortment_type == "subject") {
                        await createSubjectPackage({ db, type, resourceType, assortment_id, batch_id: resourceType[0].batch_id });
                        // await updateAssortment(type[0].assortment_id);
                    } else if (type[0].assortment_type == "chapter") {
                        await createChapterPackage({ db, type, resourceType, batch_id: resourceType[0].batch_id });
                        // await updateAssortment(type[0].assortment_id);
                    }
                }
                //     }
                // }
            }

        } catch (err) {
            console.log(err);
        } finally {
            console.log(`the script successfully ran at ${new Date()}`);
        }
    }

}

export const opts = [{
    topic: "warehouse.mysql.course-details",
    fromBeginning: false,
}];
