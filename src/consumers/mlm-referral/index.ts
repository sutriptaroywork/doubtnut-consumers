import * as _ from "lodash";
import moment from "moment";
import { InputMeta } from "../../interfaces";
import { referralMysql } from "./mlm_referral.mysql";

interface ReferralData {
    coupon_owner_student_id: string;
    coupon_applier_student_id: string;
    coupon_owner_mobile: number;
    coupon_applier_mobile: number;
    payment_info_id: number;
    claim_no: number;
    coupon_code: string;
}

interface ReferralTreeObj {
    student_id: string;
    parent_id: number;
    is_first: number;
}

interface DisbursmentObj {
    invitor_student_id: string;
    mobile: number;
    invitee_student_id: string;
    amount: number;
    order_id: string;
    payment_info_id: number;
    entry_for: string;
}

const dnPropertyBuckets = {
    /**
     * @param coupon_owner_referral_disbursment : "mlm_referral_invitor_claim_amount_mapping",
     * This bucket maps the Disbursal amount for Coupon Owner's student, mapped with claim no for referral code
     * Eg: {
     *  claim_no: {
            1: {
                referral_amount: 1000,
            },
            2: {
                referral_amount: 1000,
            },
            3: {
                referral_amount: 1500,
            },
            4: {
                referral_amount: 1500,
            },
        },
    * }
    * name : value mapping as claim_no: referral_amount, mapping
    */
    coupon_owner_referral_disbursment : "mlm_referral_invitor_claim_amount_mapping",

    /**
     * @param parent_referral_disbursment : "mlm_referral_parent_no_amount_mapping",
     * This bucket maps the Disbursal amount with the Coupon Owner's Parent No in Heirarchy  for referral code
     * Parent no 1 represent immediate parent of the Coupon Owner, Parent no 2 is the super Parent of the Coupon Owner (Tree Structure)
     * Eg: {
     *  Parent: {
            1: {
                referral_amount: 500,
            },
            2: {
                referral_amount: 250,
            },
        },
    * }
    * name : value mapping as parent_no: referral_amount, mapping
    */
    parent_referral_disbursment : "mlm_referral_parent_no_amount_mapping",
};

const maximumDisbursmentAmount = 2000;
const after10DisbusrmentAmount = 1000;

async function setReferralTreeEntries(couponOwnerReferralTreeEntry: any, couponApplierReferralTreeEntry: any, data: ReferralData) {
    try {
        // There already exist Coupon Applier student entry in table and it also has a parent (multiple parents are not supported)
        if (couponApplierReferralTreeEntry.length > 0 && (couponApplierReferralTreeEntry[0].parent_id !== null || couponApplierReferralTreeEntry[0].is_first !== 1)) {
            return true;
        }

        // Add Coupon Owner's student_id in the table with parent as null
        if (!couponOwnerReferralTreeEntry.length) {
            const couponOwnerReferralTreeObj: ReferralTreeObj = {
                student_id: data.coupon_owner_student_id,
                parent_id: null,
                is_first: 1,
            };
            await referralMysql.createEntryInReferralTree(couponOwnerReferralTreeObj);
            couponOwnerReferralTreeEntry.push( ...await referralMysql.getReferralTreeTableEntryByStudentId(data.coupon_owner_student_id));
        }

        if (data.coupon_owner_student_id === data.coupon_applier_student_id) {
            return true;
        }

        // Update the Coupon Applier entry with parent(Coupon Owner's mlm_referral id), if it's(Coupon Applier) parent_id is null and is_first = 1 (root parent)
        if (couponApplierReferralTreeEntry.length && couponApplierReferralTreeEntry[0].parent_id === null) {
            await referralMysql.updateEntryInReferralTreeById({
                parent_id: couponOwnerReferralTreeEntry[0].id,
                is_first: 0,
            }, couponApplierReferralTreeEntry[0].id);
        }

        // Add Coupon Applier student_id in the table with parent_id as Coupon Owner's mlm_referral id
        if (!couponApplierReferralTreeEntry.length) {
            const couponApplierReferralTreeObj: ReferralTreeObj = {
                student_id: data.coupon_applier_student_id,
                parent_id: couponOwnerReferralTreeEntry[0].id,
                is_first: 0,
            };
            await referralMysql.createEntryInReferralTree(couponApplierReferralTreeObj);
        }
        console.log("1", couponOwnerReferralTreeEntry);
        return true;
    } catch (e) {
        console.error(e);
        throw new Error(JSON.stringify(e));
    }
}

async function createCouponOwnerStudentDisbursmentEntry(checkCouponOwnerStudentsTg: any, claimDisbursmentMapping: any, data: ReferralData) {
    try {
        const disbursmentEntry = await referralMysql.checkDisbursmentEntry(data.coupon_owner_student_id, data.payment_info_id);
        if (disbursmentEntry.length) {
            return [];
        }
        claimDisbursmentMapping = _.groupBy(claimDisbursmentMapping, "name");
        let couponOwnerReferralAmount = 0;
        if (checkCouponOwnerStudentsTg.length > 0 && !_.isEmpty(claimDisbursmentMapping[data.claim_no])) {
            let claimAmount = parseInt(claimDisbursmentMapping[data.claim_no][0].value, 10);
            claimAmount = isNaN(claimAmount) ? 0 : claimAmount;
            couponOwnerReferralAmount = Math.min(claimAmount, maximumDisbursmentAmount);
        }
        // 11th onwards maximum Disursal Amount
        else if (checkCouponOwnerStudentsTg.length > 0 && data.claim_no >= 11) {
            couponOwnerReferralAmount = after10DisbusrmentAmount;
        }
        // old structure
        else if (parseInt(data.coupon_owner_student_id, 10) % 2 === 0 && !checkCouponOwnerStudentsTg.length) {
            couponOwnerReferralAmount = 150;
        }

        const couponOwnerDisbursmentObj: DisbursmentObj = {
            invitor_student_id: data.coupon_owner_student_id,
            mobile: data.coupon_owner_mobile,
            invitee_student_id: data.coupon_applier_student_id,
            amount: couponOwnerReferralAmount,
            order_id: (moment(new Date()).format("YYYYMMDDHHmmssSSS")).toString() + Math.floor(Math.random() * 100),
            payment_info_id: data.payment_info_id,
            entry_for: "invitor",
        };
        await referralMysql.createDisbursmentEntry(couponOwnerDisbursmentObj);
        return [];
    } catch (e) {
        console.error(e);
        throw new Error(JSON.stringify(e));
    }
}

async function createCouponOwnerParentsDisbursmentEntries(checkCouponOwnerStudentsTg: any, claimDisbursmentMapping: any, parentDisbursmentMapping: any, childEntry: any, data: ReferralData) {
    try {
        // Parent Levels upto which disbursment has to be given
        const parentsToFetchLength = parentDisbursmentMapping.length;

        parentDisbursmentMapping = _.groupBy(parentDisbursmentMapping, "name");
        claimDisbursmentMapping = _.groupBy(claimDisbursmentMapping, "name");

        if (checkCouponOwnerStudentsTg.length > 0 && (!_.isEmpty(claimDisbursmentMapping[data.claim_no.toString()]))) {

            // Loop till whatever comes first parent level to disburse or root parent(parent_id is null and is_first = 1)
            for (let i = 0; i < parentsToFetchLength && !_.isEmpty(childEntry); i++) {
                const parentEntry = await referralMysql.getReferralTreeTableEntryById(childEntry[0].parent_id);

                console.log("entry", parentEntry);
                if (!_.isEmpty(parentEntry)) {
                    const [disbursmentEntry, parentEntryCount] = await Promise.all([referralMysql.checkDisbursmentEntry(parentEntry[0].student_id, data.payment_info_id),
                    referralMysql.getDisbursalEntriesCountForParent(parentEntry[0].student_id),
                ]);
                    /**
                     * Check if there's a disbursment entry in table corresponding to the transaction
                     * Total Parent Disbursments should be 5 only for a particular student_id
                    */
                    if (_.isEmpty(disbursmentEntry) && parentEntryCount.length < 6) {
                        const parentDisbursmentObj: DisbursmentObj = {
                            invitor_student_id: parentEntry[0].student_id,
                            mobile: parentEntry[0].mobile,
                            invitee_student_id: childEntry[0].student_id,
                            amount: Math.min(parseInt(parentDisbursmentMapping[`PARENT${i + 1}`][0].value, 10), maximumDisbursmentAmount),
                            order_id: (moment(new Date()).format("YYYYMMDDHHmmssSSS")).toString() + Math.floor(Math.random() * 100),
                            payment_info_id: data.payment_info_id,
                            entry_for: `parent${i + 1}`,
                        };
                        await referralMysql.createDisbursmentEntry(parentDisbursmentObj);
                    }
                }
                childEntry = parentEntry;
            }
        }
    } catch (e) {
        console.error(e);
        throw new Error(JSON.stringify(e));
    }
}

export async function onMsg(msg: { data: any; meta: InputMeta }[]) {
    for (let i = 0; i < msg.length; i++) {
        try {
            const { meta, data } = msg[i];
            console.log("meta", meta);
            console.log("data", data);
            const ts = new Date(meta.ts);

            const paymentEntry = await referralMysql.getPaymentEntryById(data.payment_info_id);
            console.log("paymentEntry", paymentEntry);
            const couponOwnerDetails = await referralMysql.getStudentDetailsByStudentReferralCoupon(paymentEntry[0].coupon_code);

            const referralDataObj: ReferralData = {
                coupon_owner_student_id: couponOwnerDetails[0].student_id,
                coupon_applier_student_id: paymentEntry[0].student_id,
                coupon_owner_mobile: couponOwnerDetails[0].mobile,
                coupon_applier_mobile: paymentEntry[0].mobile,
                payment_info_id: paymentEntry[0].id,
                claim_no: 0,
                coupon_code: paymentEntry[0].coupon_code,
            };


            // suggestion is_disbursed:1
            // naming convetion
            const [checkCouponOwnerStudentsTg, claimDisbursmentMapping, parentDisbursmentMapping, totalCouponClaimsAfterResetLive, couponOwnerReferralTreeEntry, couponApplierReferralTreeEntry] = await Promise.all([
                // checks if the Coupon Ownwer belongs to the tg for mlm referral/ ceo referral
                referralMysql.checkUserForCEOReferralProgramByStudentId(referralDataObj.coupon_owner_student_id),
                referralMysql.getDnPropertyByBucket(dnPropertyBuckets.coupon_owner_referral_disbursment),
                referralMysql.getDnPropertyByBucket(dnPropertyBuckets.parent_referral_disbursment),
                referralMysql.getCouponClaimsFromPaymentReferralEntryByIdAndCoupon(referralDataObj.coupon_code, paymentEntry[0].payment_referral_id),
                referralMysql.getReferralTreeTableEntryByStudentId(referralDataObj.coupon_owner_student_id),
                referralMysql.getReferralTreeTableEntryByStudentId(referralDataObj.coupon_applier_student_id),
                // referralMysql.getCouponClaimsFromPaymentInfoBeforeStealthLive(referralDataObj.coupon_code),
            ]);

            referralDataObj.claim_no = totalCouponClaimsAfterResetLive.length;

            // Set Referral Tree Entries
            if (checkCouponOwnerStudentsTg.length > 0) {
                await setReferralTreeEntries(couponOwnerReferralTreeEntry, couponApplierReferralTreeEntry, referralDataObj);
            }

            // ***************** Disbursal Amount Calculation Starts *****************

            // Coupon Owner(invitor) Disbursment Entry
            await createCouponOwnerStudentDisbursmentEntry(checkCouponOwnerStudentsTg, claimDisbursmentMapping, referralDataObj);

            // Coupon Owner Student's Parent Disbursment Entries
            console.log("Parent Referral Entry", couponOwnerReferralTreeEntry, checkCouponOwnerStudentsTg);
            if (checkCouponOwnerStudentsTg.length > 0 && couponOwnerReferralTreeEntry.length && couponOwnerReferralTreeEntry[0].parent_id !== null) {
                console.log("Parent Referral Entry");
                await createCouponOwnerParentsDisbursmentEntries(checkCouponOwnerStudentsTg, claimDisbursmentMapping, parentDisbursmentMapping, couponOwnerReferralTreeEntry, referralDataObj);
            }
            // ***************** Disbursal Amount Calculation Ends *****************
        } catch (e) {
            console.error(e);
            if (_.includes(e, "PROTOCOL_CONNECTION_LOST")) {
                i--;
            }
        }
    }
}

export const opts = [{
    topic: "api-server.mlm.referral",
    fromBeginning: true,
    numberOfConcurrentPartitions: 1,
    autoCommitAfterNumberOfMessages: 1,
    autoCommitIntervalInMs: 60000,
}];
