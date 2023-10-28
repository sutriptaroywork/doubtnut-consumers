import _ from "lodash";
import { mysql } from "../modules";
let snidNew = {};


async function getSNIDData() {
    const sql = "SELECT crm.campaign, csm.student_id, crm.s_n_id FROM campaign_sid_mapping csm join campaign_redirection_mapping crm on csm.campaign=crm.campaign where crm.is_active=1";
    // console.log(sql);
    return mysql.con.query(sql).then(x => x[0]);
}

async function createSNIDMapping(){
    // console.log("inside creating snid mapping");
    const snidMapping: any = {null: [], all: []};
    const snidData: any = await getSNIDData();
    if (snidData && snidData.length){
        for (let i = 0; i < snidData.length; i++){
            if (snidData[i].student_id){
                if (!snidData[i].s_n_id || !snidData[i].s_n_id.length){
                    snidMapping.null.push(snidData[i].student_id);
                } else if (snidData[i].s_n_id === "all"){
                    snidMapping.all.push(snidData[i].student_id);
                } else {
                    const newsnidArr = snidData[i].s_n_id.split(",");
                    for (let j = 0; j < newsnidArr.length; j++){
                        if (!snidMapping[newsnidArr[j]]){
                            snidMapping[newsnidArr[j]] = [];
                        }
                        snidMapping[newsnidArr[j]].push(snidData[i].student_id);
                    }
                }
            }
        }
    }
    snidNew = _.cloneDeep(snidMapping);
}

function getSNID(){
    return snidNew;
}

export const common = {
    createSNIDMapping,
    getSNID,
};
