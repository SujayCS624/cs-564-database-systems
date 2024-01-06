#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	int recLen = 0;
	Status status;
	AttrDesc attrDesc[attrCnt];
	// Iterate over all attributes provided in attrList
	for(int i = 0; i < attrCnt; i++) {
		if(attrList[i].attrValue == NULL) {
			return ATTRNOTFOUND;
		}
		// Get the info of the attribute from the attribute catalog
		status = attrCat->getInfo(relation, attrList[i].attrName, attrDesc[i]);
		if (status != OK)
		{
			return status;
		}
		// Compute the length of the record to be inserted
		recLen += attrDesc[i].attrLen;
	}
	
	RID outRid;
	Record rec;
	// Create new object of the InsertFileScan class
	InsertFileScan* iScan = new InsertFileScan(relation, status);
	if(status != OK) {
		return status;
	}
	char ans[recLen+1];

	// Iterate over all atrributes of the record to be inserted and reorder them based on their offsets
	for(int i = 0; i < attrCnt; i++) {
		char* attr = (char*)attrList[i].attrValue;
		if(attrDesc[i].attrType == 1){
			int searchVal = atoi((char*)attrList[i].attrValue);
			attr = (char*)&searchVal;
		}
		else if(attrDesc[i].attrType == 2) {
			float searchVal2 = atof((char*)attrList[i].attrValue);
			attr = (char*)&searchVal2;
		}
		memcpy(ans+attrDesc[i].attrOffset, attr, attrDesc[i].attrLen);
	}
	rec.length = recLen;
	rec.data = ans;
	// Insert the record into the relation
	status = iScan->insertRecord(rec, outRid);
	if(status != OK) {
		return status;
	}
	return OK;

}

