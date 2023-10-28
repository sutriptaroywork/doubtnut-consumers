function getDateFromMysqlDate(datepassed) {
    const dateValue = new Date(datepassed);
    let dateToBeReturned = `${dateValue.getFullYear()}`;
    dateToBeReturned += dateValue.getMonth().toString().length === 1 ? `-0${dateValue.getMonth() + 1}` : `-${dateValue.getMonth() + 1}`;
    dateToBeReturned += dateValue.getDate().toString().length === 1 ? `-0${dateValue.getDate()}` : `-${dateValue.getDate()}`;
    return dateToBeReturned;
}

export const utility = {
    getDateFromMysqlDate,
};
