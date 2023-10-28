/* eslint-disable eqeqeq */
function getCourseFromResourceId(db, assortmentId) {
    const sql = `select cd.assortment_id  from (
        select assortment_id  from course_resource_mapping crm where crm.course_resource_id in (
            select assortment_id  from course_resource_mapping crm where course_resource_id in (
                select assortment_id  from course_resource_mapping crm where course_resource_id in (
                    select assortment_id  from course_resource_mapping crm where assortment_id  = ? and resource_type = 'resource'
                ) and resource_type = 'assortment'
            ) and resource_type = 'assortment'
        ) and crm.resource_type = 'assortment'
    ) as a
    left join course_details cd on a.assortment_id = cd.assortment_id 
    where cd.assortment_type = 'course' group by cd.assortment_id`;
    return db.query(sql, [assortmentId]).then(x => x[0]);
}

function getChildAssortment(db, assortmentId) {
    const sql = `select crm.assortment_id, crm.course_resource_id, cd.assortment_type  from course_resource_mapping crm
    left join course_details cd on crm.course_resource_id = cd.assortment_id
    where crm.assortment_id = ? and crm.resource_type = 'assortment' GROUP by crm.assortment_id, crm.course_resource_id`;
    return db.query(sql, [assortmentId]).then(x => x[0]);
}

function getClassDurationCount(database, assortment_id) {
    // let sql = `SELECT count(*) as count, sum(t2.duration)/60 as duration from (SELECT distinct a.assortment_id, a.display_name,c.resource_reference from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id  left join course_resource_mapping as d on b.course_resource_id = d.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (1,4,8)) as c on d.course_resource_id = c.id where d.resource_type="resource" and c.id is not null ORDER BY b.resource_type  DESC) as t1 left join answers as t2 on t1.resource_reference = t2.question_id`;
    const sql = `SELECT count(t1.resource_reference) as count, sum(t2.duration)/60 as duration  from (SELECT * from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping WHERE assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC)))) and resource_type in (1,4,8)) as t1 left join answers as t2 on t1.resource_reference = t2.question_id`;
    return database.query(sql).then(x => x[0]);;
}

function getClassPdf(database, assortment_id) {
    // let sql =`SELECT a.assortment_id, a.display_name,d.course_resource_id,d.resource_type from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id  left join course_resource_mapping as d on b.course_resource_id = d.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (2)) as c on d.course_resource_id = c.id where d.resource_type="resource" and c.id is not null ORDER BY b.resource_type  DESC`;
    const sql = `SELECT t1.resource_reference from (SELECT * from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping WHERE assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC)))) and resource_type in (2)) as t1`;
    return database.query(sql).then(x => x[0]);;
}

function getChapterDurationCount(database, assortment_id) {
    // let sql = `Select count(*) as count, sum(t2.duration)/60 as duration from (SELECT distinct a.assortment_id, c.resource_reference from (SELECT *  FROM course_details WHERE assortment_id = ${assortment_id}) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id, case when player_type = "youtube" then meta_info else resource_reference end as resource_reference from course_resources where resource_type in (1,4,8)) as c on b.course_resource_id = c.id) as t1 left join answers as t2 on t1.resource_reference = t2.question_id`;
    const sql = `SELECT count(t1.resource_reference) as count,sum(t2.duration)/60 as duration from (SELECT case when player_type = "youtube" then meta_info else resource_reference end as resource_reference from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC)) and resource_type in (1,4,8) ORDER BY id  ASC) as t1 left JOIN answers as t2 on t1.resource_reference = t2.question_id`;
    return database.query(sql).then(x => x[0]);;
}

function getSubjectDurationCount(database, assortment_id) {
    // let sql = `SELECT count(*) as count, sum(t2.duration)/60 as duration from (SELECT distinct a.assortment_id, a.display_name,c.resource_reference from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id, case when player_type = "youtube" then meta_info else resource_reference end as resource_reference from course_resources where resource_type in (1,4,8)) as c on b.course_resource_id = c.id where b.resource_type="resource" and c.id is not null ORDER BY b.resource_type  DESC) as t1 left join answers as t2 on t1.resource_reference=t2.question_id`;
    const sql = `SELECT count(t1.resource_reference) as count, sum(t2.duration)/60 as duration from (SELECT case when player_type = "youtube" then meta_info else resource_reference end as resource_reference from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC))) and resource_type in (1,4,8) ORDER BY id  ASC) as t1 left JOIN answers as t2 on t1.resource_reference = t2.question_id`;
    return database.query(sql).then(x => x[0]);;
}

function getChapterPdf(database, assortment_id) {
    // let sql =`SELECT distinct a.assortment_id,c.id,c.resource_type, c.resource_reference from (SELECT *  FROM course_details WHERE assortment_id = ${assortment_id} ) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (2)) as c on b.course_resource_id = c.id where b.resource_type="resource" and c.id is not null`;
    const sql = `SELECT id from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC)) and resource_type in (2)`;
    return database.query(sql).then(x => x[0]);;
}

function getSubjectPdf(database, assortment_id) {
    // let sql =` SELECT a.assortment_id, a.display_name,b.course_resource_id,b.resource_type from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id  left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (2)) as c on b.course_resource_id = c.id where b.resource_type="resource" and c.id is not null ORDER BY b.resource_type  DESC`;
    const sql = `SELECT id from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC))) and resource_type in (2) ORDER BY id  ASC`;
    return database.query(sql).then(x => x[0]);;
}

function getSubjectTests(database, assortment_id) {
    // let sql =`SELECT a.assortment_id, a.display_name,b.course_resource_id,b.resource_type from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id  left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (9)) as c on b.course_resource_id = c.id where b.resource_type="resource" and c.id is not null ORDER BY b.resource_type  DESC`;
    const sql = `SELECT count(id) from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC))) and resource_type in (9) ORDER BY id  ASC`;
    return database.query(sql).then(x => x[0]);;
}

function getType(database, res_id) {
    const sql = `select distinct assortment_id, assortment_type,category, display_name from course_details where assortment_id = ${res_id}`;
    return database.query(sql).then(x => x[0]);;
}

function getCourseMapping(database, assortment_id) {
    const sql = `select * from course_resource_mapping where assortment_id=${assortment_id}`;
    console.log(sql);
    return database.query(sql).then(x => x[0]);;
}

function getAllFreeCourse(database, aid) {
    const sql = "select distinct assortment_id from course_details where is_free=0 and assortment_id = ? and assortment_type  not in (\"resource_video\", \"resource_pdf\", \"resource_test\",\"resource_quiz\")";
    return database.query(sql, [aid]).then(x => x[0]);;
}

function getChapterTests(database, assortment_id) {
    const sql = `SELECT distinct a.assortment_id,c.id,c.resource_type, c.resource_reference from (SELECT *  FROM course_details WHERE assortment_id = ${assortment_id} ) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (9)) as c on b.course_resource_id = c.id where b.resource_type="resource" and c.id is not null`;
    return database.query(sql).then(x => x[0]);;
}
function getCourseResource(database, course_resource_id) {
    const sql = `select case when player_type = "youtube" then meta_info else resource_reference end as resource_reference, description, expert_name from course_resources where id=${course_resource_id}  limit 1`;
    return database.query(sql).then(x => x[0]);;
}

function toTitleCase(str) {
    return str.replace(
        /\w\S*/g,
        txt => txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase(),
    );
}

function getAssortmentType(database, assortment_id) {
    const sql = `select * from course_resource_mapping where assortment_id=${assortment_id} order by id desc limit 1`;
    console.log(sql);
    return database.query(sql).then(x => x[0]);;
}

function getDuration(database, question_id) {
    const sql = `select * from answers where question_id=${question_id} order by answer_id DESC`;
    return database.query(sql).then(x => x[0]);;
}

function getSubjectParentPackages(db, assortmentId, batchId) {
    const sql = 'select p.id, p.duration_in_days, cd.assortment_type, cd.meta_info from package p left join course_details cd on p.assortment_id = cd.assortment_id  where p.is_active = 1 and (p.duration_in_days <= 365 or p.is_multi_year = 0) and p.assortment_id in (select assortment_id from course_resource_mapping where resource_type = "assortment" and course_resource_id = ?) and p.batch_id = ? group by p.assortment_id, p.duration_in_days';
    return db.query(sql, [assortmentId, batchId]).then(x => x[0]);
}

function getVariantByPackageId(db, packageId) {
    const sql = "select * from variants where package_id = ? and is_default = 1 order by display_price";

    return db.query(sql, [packageId]).then(x => x[0]);
}

// function getResource(database, aId) {
//     const sql = "SELECT distinct a.assortment_id, a.category, a.display_name, a.assortment_type as type_resource from (SELECT *  FROM course_details WHERE assortment_id = ? and assortment_type in (\"resource_video\",  \"resource_pdf\", \"resource_test\") and is_free = 0) as a left join package as b on a.assortment_id = b.assortment_id where b.id is null order by a.assortment_id asc";
//     return database.query(sql, [aId]).then(x => x[0]);
// }

function insertPackage(database, obj) {
    const sql = "insert into package set ?";
    return database.query(sql, [obj]).then(x => x[0]);
}

function insertVariants(database, obj) {
    const sql = "insert into variants set ?";
    return database.query(sql, [obj]).then(x => x[0]);
}

function checkAssortmentInPackages(database, aId) {
    const sql = `select * from package where assortment_id=${aId}`;
    return database.query(sql).then(x => x[0]);;
}

function checkAssortmentWithBatchInPackages(database, aId, batchId) {
    const sql = "select * from package where assortment_id = ? and batch_id = ?";
    return database.query(sql, [aId, batchId]).then(x => x[0]);
}

function getFraction(num, fraction) {
    return Math.floor(fraction * parseInt(num, 10));
}

async function createSubjectPackage({ db, type, resourceType, assortment_id, batch_id }){
    const categoryArray = type[0].category.split("|");
    const name = `${toTitleCase(type[0].display_name)} for ${categoryArray[0].toUpperCase()}`;
    const subjectTests = await getSubjectTests(db.mysql.read, type[0].assortment_id);
    const subjectPdf = await getSubjectPdf(db.mysql.read, type[0].assortment_id);

    const subjectParent = await getSubjectParentPackages(db.mysql.read, assortment_id, batch_id);

    if (subjectParent && subjectParent.length > 0) {
        for (let j = 0; j < subjectParent.length; j++) {
            const parentPackage = subjectParent[j];
            const parentVariant = await getVariantByPackageId(db.mysql.read, parentPackage.id);

            console.log(parentPackage, parentVariant);
            if (!(parentVariant && parentVariant.length)){
                continue;
            }
            let displayPrice = getFraction(parentVariant[0].display_price, 0.4);
            let basePrice = getFraction(parentVariant[0].base_price, 0.4);

            if (parentPackage.meta_info == "HINDI") {
                displayPrice = getFraction(parentVariant[0].display_price, 0.5);
                basePrice = getFraction(parentVariant[0].base_price, 0.5);
            }

            const minLimit = getFraction(displayPrice, 0.5);

            const obj = {
                assortment_id: type[0].assortment_id,
                name,
                description: `Master complete subject for ${categoryArray[0].toUpperCase()}`,
                is_active: 1,
                type: "subscription",
                min_limit: minLimit,
                duration_in_days: parentPackage.duration_in_days,
                batch_id,

            };
            if (resourceType[0].schedule_type == "recorded") {
                const duration = await getSubjectDurationCount(db.mysql.read, type[0].assortment_id);
                if (duration && duration.length > 0 && duration[0].duration != 0 && duration[0].count != 0) {
                    // obj.description = obj.description + " | " + Math.floor(duration[0].duration) + "+ minutes of video on demand | "+duration[0].count+ " videos on demand"
                    obj.description = `${obj.description} | ${duration[0].count} classes - RECORDED - ${Math.floor(duration[0].duration)}+ minutes`;
                }
            } else {
                obj.description = `${obj.description} | ` + "Regular Live Classes Every Week | Recording Available";
            }
            if (subjectTests && subjectTests.length > 0) {
                obj.description = `${obj.description} - ${subjectTests.length} Tests`;
            }
            if (subjectPdf && subjectPdf.length > 0) {
                obj.description = `${obj.description} - ${subjectPdf.length} PDF`;
            }

            console.log(obj);
            const insert = await insertPackage(db.mysql.write, obj);

            const varObj = {
                package_id: insert.insertId,
                base_price: basePrice,
                display_price: displayPrice,
                is_default: 1,
                is_show: 1,
                is_active: 1,
            };
            await insertVariants(db.mysql.write, varObj);
        }
    }
}
async function createChapterPackage({ db, type, resourceType, batch_id }) {
    const categoryArray = type[0].category.split("|");
    const name = `${toTitleCase(type[0].display_name)} for ${categoryArray[0].toUpperCase()}`;
    const chapterTests = await getChapterTests(db.mysql.read, type[0].assortment_id);
    const chapterPdf = await getChapterPdf(db.mysql.read, type[0].assortment_id);
    const obj = {
        assortment_id: type[0].assortment_id,
        name,
        description: `All lectures in this series related to this chapter for ${categoryArray[0].toUpperCase()}`,
        is_active: 1,
        type: "subscription",
        min_limit: 59,
        duration_in_days: 365,
        batch_id,
    };
    if (resourceType[0].schedule_type == "recorded") {
        const duration = await getChapterDurationCount(db.mysql.read, type[0].assortment_id);
        if (duration && duration.length > 0 && duration[0].duration != 0 && duration[0].count != 0) {
            // obj.description = obj.description + " | " + Math.floor(duration[0].duration) + "+ minutes of video on demand | "+duration[0].count+ " videos on demand"
            obj.description = `${obj.description} | ${duration[0].count} classes - RECORDED - ${Math.floor(duration[0].duration)}+ minutes`;
        }
    } else {
        obj.description = `${obj.description} | ` + "Regular Live Classes Every Week | Recording Available";
    }
    if (chapterTests && chapterTests.length > 0) {
        obj.description = `${obj.description} - ${chapterTests.length} Tests`;
    }
    if (chapterPdf && chapterPdf.length > 0) {
        obj.description = `${obj.description} - ${chapterPdf.length} PDF`;
    }
    console.log(obj);
    const insert = await insertPackage(db.mysql.write, obj);
    const varObj = {
        package_id: insert.insertId,
        base_price: 229,
        display_price: 159,
        is_default: 1,
        is_show: 1,
        is_active: 1,
    };
    await insertVariants(db.mysql.write, varObj);
}
export {
    getCourseFromResourceId,
    getChildAssortment,
    getClassDurationCount,
    getClassPdf,
    getChapterDurationCount,
    getSubjectDurationCount,
    getChapterPdf,
    getSubjectPdf,
    getSubjectTests,
    getType,
    getCourseMapping,
    getAllFreeCourse,
    getChapterTests,
    getCourseResource,
    toTitleCase,
    getAssortmentType,
    getDuration,
    getSubjectParentPackages,
    getVariantByPackageId,
    insertPackage,
    insertVariants,
    checkAssortmentInPackages,
    checkAssortmentWithBatchInPackages,
    getFraction,
    createSubjectPackage,
    createChapterPackage,
};
