#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
        status = db.createFile(fileName);

        if(status != OK) {
            return status;
        }

        // Open the file
        status = db.openFile(fileName, file);
        

        if(status != OK) {
            return status;
        }

        // Allocate the header page
        status = bufMgr->allocPage(file, hdrPageNo, newPage);

        if(status != OK) {
            return status;
        }

        hdrPage = (FileHdrPage*) newPage;
        // Initialize values in the header page
        strcpy(hdrPage->fileName, fileName.c_str());
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        // Allocate the first data page
        status = bufMgr->allocPage(file, newPageNo, newPage);

        hdrPage->pageCnt++;

        if(status != OK) {
            return status;
        }

        // Initialize values for the new data page
        newPage->init(newPageNo);

        // Update values in the header page
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;

        // Unpin header page
        status = bufMgr->unPinPage(file, hdrPageNo, true);

        if(status != OK) {
            return status;
        }

        // Unpin first data page
        status = bufMgr->unPinPage(file, newPageNo, true);

        if(status != OK) {
            return status;
        }

        // Close the file
        status = db.closeFile(file);

        return status;	
		
    }

    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // Get the header page number
        status = filePtr->getFirstPage(headerPageNo);
        
        if(status != OK){
            returnStatus = status;
            return;
        }

        // Read in the header page
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);

        if(status != OK){
            returnStatus = status;
            return;
        }

        // Initialize private data members of header page
        headerPage = (FileHdrPage*) pagePtr;

        hdrDirtyFlag = true;

        // Get the first page number
        curPageNo = headerPage->firstPage;

        // Read in the first data page as current page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);

        if(status != OK){
            returnStatus = status;
            return;
        }

        // Initialize Heapfile data members
        curDirtyFlag = true;

        curRec = NULLRID;
        returnStatus = status;
        return;
		
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cout << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cout << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
        cout << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    // Check if current page contains the record
    if(curPageNo == rid.pageNo) {
        // Get the record from the rid
        status = curPage->getRecord(rid, rec);
        if(status!=OK){
            return status;
        }
        // Updating Heapfile data members
        curDirtyFlag = true;
        curRec = rid;
    }
    // If current page does not contain the record
    else {
        // Unpin the current page
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        if(status!=OK){
            return status;
        }

        // Read in the page containing the record
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if(status!=OK){
            return status;
        }

        // Get the record from the rid
        status = curPage->getRecord(rid, rec);
        if(status!=OK){
            return status;
        }

        // Updating Heapfile data members
        curPageNo = rid.pageNo;

        curDirtyFlag = true;

        curRec = rid;

    }

   
   return status;
   
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;
    // Check if current page is NULL
    if(curPage == NULL) {
        // Populate the current page with the first data page
        nextPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        if(status != OK) {
            return status;
        }
        curPageNo = nextPageNo;
    }
    // Loop over all pages of the file
    while(1) {

        int isEmptyPage = 0;
        // If we encounter a new page for the first time
        if(curRec.pageNo != curPageNo){
            // Get the first record in the page
            status = curPage->firstRecord(tmpRid);
            // If there are no records in the page and it is the last page, return FILEEOF
            if(status == NORECORDS && curPageNo == headerPage->lastPage){
                return FILEEOF;
            }
            // If there are no records in the page but it isn't the last page, set empty page flag to true
            else if(status == NORECORDS) {
                isEmptyPage = 1;
            }
            
            if(status != OK && status != NORECORDS) {
                return status;
            }

            // If we get a valid record
            if(status != NORECORDS){
                // Get the record
                status = curPage->getRecord(tmpRid, rec);

                if(status != OK && status != NORECORDS) {
                    return status;
                }
                // Check if record satisfies the predicate
                curRec = tmpRid;
                if(matchRec(rec) == true) {
                    outRid = curRec;
                    return OK;
                }    
            }

        }

        // If records exist in the page
        if(!isEmptyPage){
            // Loop over all the records in a page
            while(1) {
                status = curPage->nextRecord(curRec, nextRid);

                if(status == ENDOFPAGE) {
                    break;
                }

                if(status != OK) {
                    return status;
                }

                status = curPage->getRecord(nextRid, rec);

                if(status != OK) {
                    return status;
                }

                curRec = nextRid;
                if(matchRec(rec) == true) {
                    outRid = curRec;
                    return OK;
                } 
            }
        }

        // If it is the last page and we went through all records, return FILEEOF
        if(curPageNo == headerPage->lastPage){
            return FILEEOF;
        }

        // Get the next page number
        status = curPage->getNextPage(nextPageNo);

        if(status != OK) {
            return status;
        }
        // Unpin the current page
        status = bufMgr->unPinPage(filePtr,curPageNo,true);
        if(status != OK) {
            return status;
        }
        // Read the next page
        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        curPageNo = nextPageNo;
        if(status != OK) {
            return status;
        }
    }

    return status;
    
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // If the current page is NULL, get the last page of the file
    if(curPage == NULL) {
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if(status != OK) {
            return status;
        }
    }

    // Try to insert the record into the current page
    status = curPage->insertRecord(rec, outRid);

    // If insert is successful, update HeapFile data members
    if(status == OK) {
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
        curRec = outRid;
        return status;
    }
    // If insert was unsuccessful
    else {
        // Allocate a new page and insert the record into it
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);

        if(status != OK) {
            return status;
        }
        
        newPage->init(newPageNo);

        headerPage->lastPage = newPageNo;

        headerPage->pageCnt++;

        status = curPage->setNextPage(newPageNo);

        if(status != OK) {
            return status;
        }

        status = bufMgr->unPinPage(filePtr, curPageNo, true);

        if(status != OK) {
            return status;
        }

        curPage = newPage;

        curPageNo = newPageNo;

        status = curPage->insertRecord(rec, outRid);

        if(status == OK) {
            headerPage->recCnt++;
            hdrDirtyFlag = true;
            curDirtyFlag = true;
            curRec = outRid;
        }
        return status;
    }
}


