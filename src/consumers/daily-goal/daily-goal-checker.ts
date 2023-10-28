import _ from "lodash";

import { utility } from "../../modules/utility";
import { dbOperations } from "./db-operations";
import { redisOperations } from "./redis-operations";
import { getDataForDailyGoal } from "./elastic-data";

/* eslint no-underscore-dangle: ["error", { "allow": ["_extras", "_source"] }] */

interface ResourcePath {
    resource_path: string;
}

interface Suggesstion {
    _source: object;
    srcId: string;
    _extras: ResourcePath;
}

interface DoubtfeedData {
    liveClass?: object;
    video?: object;
    pdf?: object;
    sugg?: Suggesstion [];
};

function existingResourceList(resourceList, resourceType) {
    const arrayResourceTypes = ["TOPIC_VIDEO", "PDF", "FORMULA_SHEET"];
    let resourceIds;
    if (arrayResourceTypes.indexOf(resourceType) > -1) {
        resourceIds = "";
    } else {
        resourceIds = 0;
    }
    if (resourceList.length > 0) {
        const prevLiveVideoQuestions = resourceList.filter(x => x.type === resourceType);
        if (prevLiveVideoQuestions.length > 0) {
            resourceIds = prevLiveVideoQuestions[0].resource_id;
        }
    }
    return resourceIds;
}

async function checkLcData(resourceList, liveVideoData) {
    let prevLiveQuestionsId = 0;
    prevLiveQuestionsId = existingResourceList(resourceList, "LIVE_VIDEO");

    liveVideoData = liveVideoData[0].list;

    const liveVideoDataWithoutPrev = liveVideoData.filter(x => x._source.id !== prevLiveQuestionsId.toString());
    if (liveVideoDataWithoutPrev.length !== 0) {
        liveVideoData = liveVideoDataWithoutPrev.slice(0, 1);
    } else {
        liveVideoData = liveVideoData.slice(0, 1);
    }

    return liveVideoData;
}

async function checkTopicVideoData(resourceList, topicVideoData) {
    let prevTopicVideoIdsArr = [];
    const prevTopicVideoIds = existingResourceList(resourceList, "TOPIC_VIDEO");
    prevTopicVideoIdsArr = prevTopicVideoIds.split(",");

    topicVideoData = topicVideoData[0].list;

    if (topicVideoData.length > 0) {
        const topicVideoDataWithoutPrev = topicVideoData.filter(x => prevTopicVideoIdsArr.indexOf(x._source.id) === -1);
        if (topicVideoDataWithoutPrev.length > 2) {
            topicVideoData = topicVideoDataWithoutPrev.slice(0, 5);
        } else {
            topicVideoData = topicVideoData.slice(0, 5);
        }
        if (topicVideoData.length <= 2) {
            topicVideoData = [];
        }
    }

    return topicVideoData;
}

async function checkTopicBoosterData(chapter, resourceList) {
    let questionId = 0;

    let prevTbId = 0;
    prevTbId = existingResourceList(resourceList, "TOPIC_MCQ");
    const chapterAliasResponse = await dbOperations.getChapterAliasByChapter(chapter);

    if (chapterAliasResponse.length > 0) {
        let chapterAlias = "";
        let quesId = 0;
        chapterAliasResponse.forEach(x => {
            if (x.question_id !== prevTbId) {
                chapterAlias = x.chapter_alias;
                quesId = x.question_id;
            }
        });
        if (chapterAlias !== "") {
            const key = `TOPIC_${chapterAlias}_5`;
            const isChapterAliasAllowed = await redisOperations.getByKey(key);
            if (isChapterAliasAllowed && questionId !== 0) {
                questionId = quesId;
            }
        }
    }
    return questionId;
}

async function checkPdfData(resourceList, pdfData) {
    let prevPdfIdsArr = [];
    const prevPdfIds = existingResourceList(resourceList, "PDF");
    prevPdfIdsArr = prevPdfIds.split(",");

    pdfData = pdfData[0].list;

    if (pdfData.length > 0) {
        const pdfDataWithoutPrev = pdfData.filter(x => prevPdfIdsArr.indexOf(x._source.resource_path) === -1);
        if (pdfDataWithoutPrev.length > 2) {
            pdfData = pdfDataWithoutPrev.slice(0, 5);
        } else {
            pdfData = pdfData.slice(0, 5);
        }

        if (pdfData.length <= 2) {
            pdfData = [];
        }
    }

    return pdfData;
}

async function checkFsData(chapter, resourceList, studentClass) {
    let prevfsIdsArr = [];
    const prevfsIds = existingResourceList(resourceList, "FORMULA_SHEET");
    prevfsIdsArr = prevfsIds.split(",");

    let fsData = await dbOperations.getFormulaSheets(chapter, studentClass);

    if (fsData.length > 0) {
        let fsList = [];
        const fsDataWithoutPrev = fsData.filter(x => prevfsIdsArr.indexOf(x.id) === -1);

        if (fsDataWithoutPrev.length > 2) {
            fsList = fsDataWithoutPrev;
        } else {
            fsList = fsData;
        }

        fsList = fsList.filter(x => x.location && !_.isEmpty(x.location) && !_.isNull(x.location));
        fsList = fsList.slice(0, 5);

        if (fsList.length !== 0) {
            fsData = fsList;
        }
    }

    return fsData;
}

async function checkDoubtFeedAvailable(chapter, studentId, studentClass, subject, locale) {
    let topicExist = false;

    const currDate = new Date();
    const offset = new Date().getTimezoneOffset();
    if (offset === 0) {
        currDate.setHours(currDate.getHours() + 5);
        currDate.setMinutes(currDate.getMinutes() + 30);
    }
    const dateToBePassed = utility.getDateFromMysqlDate(currDate);
    let previousTopic = await dbOperations.getPreviousTopicByDate(studentId, chapter, dateToBePassed);

    let previousResources = [];
    if (previousTopic.length > 0) {
        previousTopic = previousTopic[0];
        previousResources = await dbOperations.getQuestionList(previousTopic.id);
    }

    const dailyGoalObj = {
        type: "live",
        subject,
        chapter,
        class: studentClass,
        student_id: studentId,
        locale,
    };
    let doubtfeedData: DoubtfeedData = await getDataForDailyGoal(dailyGoalObj);
    doubtfeedData = doubtfeedData.liveClass;
    if (doubtfeedData && Object.keys(doubtfeedData).length !== 0 && doubtfeedData.sugg && doubtfeedData.sugg.length !== 0) {
        doubtfeedData.sugg.forEach(x => {
            x._source = {
                id: x.srcId,
            };
        });
        const elasticData = [{ list: doubtfeedData.sugg }];
        const liveVideoData = await checkLcData(previousResources, elasticData);
        if (liveVideoData.length > 0) {
            topicExist = true;
        }
    }

    if (!topicExist) {
        dailyGoalObj.type = "video";
        doubtfeedData = await getDataForDailyGoal(dailyGoalObj);
        doubtfeedData = doubtfeedData.video;
        if (doubtfeedData && Object.keys(doubtfeedData).length !== 0 && doubtfeedData.sugg && doubtfeedData.sugg.length !== 0) {
            doubtfeedData.sugg.forEach(x => {
                x._source = {
                    id: x.srcId,
                };
            });
            const elasticData = [{ list: doubtfeedData.sugg }];
            const topicVideoData = await checkTopicVideoData(previousResources, elasticData);
            if (topicVideoData.length > 0) {
                topicExist = true;
            }
        }
    }

    if (!topicExist) {
        const topicBoosterData = await checkTopicBoosterData(chapter, previousResources);
        if (topicBoosterData !== 0) {
            topicExist = true;
        }
    }

    if (!topicExist) {
        dailyGoalObj.type = "pdf";
        doubtfeedData = await getDataForDailyGoal(dailyGoalObj);
        doubtfeedData = doubtfeedData.pdf;
        if (doubtfeedData && Object.keys(doubtfeedData).length !== 0 && doubtfeedData.sugg && doubtfeedData.sugg.length !== 0) {
            doubtfeedData.sugg.forEach(x => {
                x._source = {
                    resource_path: x._extras.resource_path,
                };
            });
            const elasticData = [{ list: doubtfeedData.sugg }];
            const pdfData = await checkPdfData(previousResources, elasticData);
            if (pdfData.length > 0) {
                topicExist = true;
            }
        }
    }

    if (!topicExist) {
        const fsData = await checkFsData(chapter, previousResources, studentClass);
        if (fsData.length > 0) {
            topicExist = true;
        }
    }

    return topicExist;
}

async function addChapterData(studentId, questionId, parentId, locale) {
    const returnData = {
        sendNotification: false,
        topic: "",
    };
    const questionDetails = await dbOperations.getByQuestionId(questionId);
    if (questionDetails.length > 0) {
        let { subject } = questionDetails[0];
        const wrongChapterArray = [" ", "NULL", "Default", "DEFAULT", "default"];
        if (wrongChapterArray.indexOf(subject) > -1) {
            subject = "";
        }
        const topic = questionDetails[0].chapter;
        const classVal = questionDetails[0].class;
        if (topic !== "" && topic !== "DEFAULT") {
            const topicDetails = await dbOperations.getTopicDetails(studentId, topic);
            if (topicDetails.length > 0) {
                let qList = topicDetails[0].qid_list;
                if (qList.includes(",")) {
                    qList = qList.split(",");
                } else {
                    qList = [qList];
                }
                if (!qList.includes(parentId)) {
                    qList.unshift(parentId);
                    qList = qList.join();
                    dbOperations.updateQuestionId(topicDetails[0].id, qList);
                }
            } else {
                const checkTopicDataAvalability = await checkDoubtFeedAvailable(topic, studentId, classVal, subject, locale);
                if (checkTopicDataAvalability) {
                    const obj = {
                        sid: studentId,
                        qid_list: parentId,
                        topic,
                        subject,
                    };
                    dbOperations.insertDailyDoubt(obj);
                    returnData.sendNotification = true;
                    returnData.topic = topic;
                }
            }
        }
    }

    return returnData;
}

async function storeDoubtFeedTopic(studentId, questionId, parentId, locale) {
    const todaysTopics = await dbOperations.getTodaysTopics(studentId);
    let mainFlag = 0;
    if (todaysTopics.length > 0) {
        let flag = 0;
        todaysTopics.forEach(x => {
            let qList = x.qid_list;
            if (qList.includes(",")) {
                qList = qList.split(",");
            } else {
                qList = [qList];
            }
            if (qList.includes(parentId)) {
                flag = 1;
            }
        });

        if (flag === 0) {
            mainFlag = 1;
        }
    } else {
        mainFlag = 1;
    }

    if (mainFlag === 1) {
        return addChapterData(studentId, questionId, parentId, locale);
    }
    return {
        sendNotification: false,
        topic: "",
    };
}

export const dailyGoalChecker = {
    storeDoubtFeedTopic,
};
