#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
    cout << "Doing QU_Select " << endl;

	Status status;
	AttrDesc projNames2[projCnt];

	// Get info about the predicate's attribute using the attribute catalog
	AttrDesc attrDesc;
	if(attr != NULL) {
		status = attrCat->getInfo(attr->relName,
                                     attr->attrName,
                                     attrDesc);
		if (status != OK)
		{
			return status;
		}
	}
	// Iterate over all the desired attributes of the relation 
	int recLen = 0;
	for(int i = 0; i < projCnt; i++) {
		// Get the info of the attribute from the attribute catalog
		status = attrCat->getInfo(projNames[i].relName,
                                     projNames[i].attrName,
                                     projNames2[i]);
		if (status != OK)
		{
			return status;
		}
		// Compute the length of the record to be selected
		recLen += projNames2[i].attrLen;
	}
    // QU_Select sets up things and then calls ScanSelect to do the actual work
	return ScanSelect(result, projCnt, projNames2, &attrDesc, op, attrValue, recLen);

}


const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	// Convert the filter value to be of the right type
	int searchVal;
	float searchVal2;
	if(attrDesc->attrType == 1) {
		searchVal = atoi(filter);
		filter = (char*)&searchVal;
	}
	else if(attrDesc->attrType == 2) {
		searchVal2 = atof(filter);
		filter = (char*)&searchVal2;
	}
		Status status;
		// Create a new object of HeapFileScan class
		HeapFileScan* scan = new HeapFileScan(projNames[0].relName, status);
		// If no predicate, then select all tuples in the relation
		if(filter == NULL) {
			status = scan->startScan(0, 0, STRING, filter, EQ);
			if(status != OK) {
				return status;
			}		
		}
		// If predicate is provided, initialize startScan with the appropriate arguments
		else {
			int len = attrDesc->attrLen;
			status = scan->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype) attrDesc->attrType, filter, op);
			if(status != OK) {
				return status;
			}
		}
		RID rec2Rid;
		Record rec;
		// Create a new object of InsertFileScan class
		InsertFileScan* iScan = new InsertFileScan(result, status);
		if(status != OK) {
			return status;
		}
		while(scan->scanNext(rec2Rid) == OK) {
			status = scan->getRecord(rec);
			if(status != OK) {
				return status;
			}

			char outputData[reclen];
    		Record outputRec;
    		outputRec.data = (void *) outputData;
    		outputRec.length = reclen;

			int outputOffset = 0;
            for (int i = 0; i < projCnt; i++)
            {
                memcpy(outputData + outputOffset, (char *)rec.data + projNames[i].attrOffset, projNames[i].attrLen);                    
                outputOffset += projNames[i].attrLen;
            }
			RID outRid;
			// Insert selected record into temp result relation 
			status = iScan->insertRecord(outputRec, outRid);
			if(status != OK) {
				return status;
			}

		}
	scan->endScan();
    delete scan;
	delete iScan;
	return OK;


}
