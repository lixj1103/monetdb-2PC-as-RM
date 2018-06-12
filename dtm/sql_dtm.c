/*-------------------------------------------------------------------------
 *
 * sql_dtm.c
 *	  Provides routines for performing distributed transaction
 *
 * Copyright (c) 2005-2009, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#include "monetdb_config.h"
#include "sql_dtm.h"
#include "sql_scenario.h"
#include "trace_log.h"
#include <sql_storage.h>

/* bit 1 is for statement wants DTX transaction
 *
 * bits 2-3 for iso level  00 read-committed
 *						   01 read-uncommitted
 *						   10 repeatable-read
 *						   11 serializable
 * bit 4 is for read-only
 */
#define GP_OPT_NEED_TWO_PHASE                           0x0001

#define GP_OPT_READ_COMMITTED    						0x0002
#define GP_OPT_READ_UNCOMMITTED  						0x0004
#define GP_OPT_REPEATABLE_READ   						0x0006
#define GP_OPT_SERIALIZABLE 	  						0x0008

#define GP_OPT_READ_ONLY         						0x0010

#define GP_OPT_EXPLICT_BEGIN      						0x0020

#define DTM_DEBUG5 LOG

/* isMppTxOptions_StatementWantsDtxTransaction:
 * Return the NeedTwoPhase flag.
 */
bool
static isMonetdbMppTxOptions_NeedTwoPhase(int txnOptions)
{
	return ((txnOptions & GP_OPT_NEED_TWO_PHASE) != 0);
}

/* isMppTxOptions_ExplicitBegin:
 * Return the ExplicitBegin flag.
 */
bool
static isMonetdbMppTxOptions_ExplicitBegin(int txnOptions)
{
	return ((txnOptions & GP_OPT_EXPLICT_BEGIN) != 0);
}

/**
 * Does DistributedTransactionContext indicate that this is acting as a QD?
 */
static bool
isQDContext(void)
{
    switch (DistributedTransactionContext)
	{
		case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
		case DTX_CONTEXT_QD_RETRY_PHASE_2:
			return true;
		default:
			return false;
	}
}

/**
 * Does DistributedTransactionContext indicate that this is acting as a QE?
 */
static bool
isQEContext()
{
	switch(DistributedTransactionContext)
	{
		case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
		case DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT:
		case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
		case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
		case DTX_CONTEXT_QE_READER:
			return true;
		default:
			return false;
	}
}

/**
 * All assignments of the global DistributedTransactionContext should go through this function
 *   (so we can add logging here to see all assignments)
 *
 * @param context the new value for DistributedTransactionContext
 */
static void
setMonetdbDistributedTransactionContext(DtxContext context)
{	
	
	mlog( LOG_DTM_DEBUG ,JKD_TRANSACTION, 0, "Setting DistributedTransactionContext to '%s'", DtxContextToString(context));
	DistributedTransactionContext = context;
}

static bool
requireMonetdbDistributedTransactionContext(DtxContext requiredCurrentContext)
{
	if (DistributedTransactionContext != requiredCurrentContext )
	{
		//elog(FATAL, "Expected segment distributed transaction context to be '%s', found '%s'",
		//	 DtxContextToString(requiredCurrentContext),
		//	 DtxContextToString(DistributedTransactionContext));
		// FATAL will cause hang
		  mlog( LOG_ERROR_M ,JKD_TRANSACTION, 0 , "Expected segment distributed transaction context to be '%s', found '%s'",
			DtxContextToString(requiredCurrentContext),
			DtxContextToString(DistributedTransactionContext));
			
			return false;
	}
	return true;
}

/**
 * Called on the QE when a query to process has been received.
 *
 * This will set up all distributed transaction information and set the state appropriately.
 */
str
setupMonetdbQEDtxContext (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	DistributedSnapshot *distributedSnapshot;
	int txnOptions;
	bool needTwoPhase;
	bool explicitBegin;
	bool haveDistributedSnapshot;
	bool isEntryDbSingleton = false;
	bool isReaderQE = false;
	bool isWriterQE = true;
	bool isSharedLocalSnapshotSlotPresent;
	str msg = MAL_SUCCEED;

	assert (dtxContextInfo != NULL);
  
	/*
	 * DTX Context Info (even when empty) only comes in QE requests.
	 */
	distributedSnapshot = &TempDtxContextInfo.distributedSnapshot;
	txnOptions = TempDtxContextInfo.distributedTxnOptions;

	needTwoPhase = isMonetdbMppTxOptions_NeedTwoPhase(txnOptions);
  explicitBegin = isMonetdbMppTxOptions_ExplicitBegin(txnOptions);

	haveDistributedSnapshot =
		(TempDtxContextInfo.distributedXid!= InvalidDistributedTransactionId);
	
	
	//TODO:wangccdl isSharedLocalSnapshotSlotPresent
	//isSharedLocalSnapshotSlotPresent = (SharedLocalSnapshotSlot != NULL);

	/*if (DEBUG5 >= log_min_messages || Debug_print_full_dtm)
	{
		elog(DTM_DEBUG5,
			 "setupMonetdbQEDtxContext inputs (part 1): Gp_role = %s, Gp_is_writer = %s, "
			 "txnOptions = 0x%x, needTwoPhase = %s, explicitBegin = %s, isoLevel = %s, readOnly = %s, haveDistributedSnapshot = %s.",
			 role_to_string(Gp_role), (Gp_is_writer ? "true" : "false"), txnOptions,
			 (needTwoPhase ? "true" : "false"), (explicitBegin ? "true" : "false"),
			 IsoLevelAsUpperString(mppTxOptions_IsoLevel(txnOptions)), (isMppTxOptions_ReadOnly(txnOptions) ? "true" : "false"),
			 (haveDistributedSnapshot ? "true" : "false"));
	    	elog(DTM_DEBUG5,
			 "setupMonetdbQEDtxContext inputs (part 2): distributedXid = %u, isSharedLocalSnapshotSlotPresent = %s.",
			 dtxContextInfo->distributedXid,
		     (isSharedLocalSnapshotSlotPresent ? "true" : "false"));

		if (haveDistributedSnapshot)
		{
			elog(DTM_DEBUG5,
				 "setupMonetdbQEDtxContext inputs (part 2a): distributedXid = %u, "
				 "distributedSnapshotData (xmin = %u, xmax = %u, xcnt = %u), distributedCommandId = %d",
				 dtxContextInfo->distributedXid,
				 distributedSnapshot->header.xmin, distributedSnapshot->header.xmax,
				 distributedSnapshot->header.count,
				 dtxContextInfo->curcid);
		}
		if (isSharedLocalSnapshotSlotPresent)
		{
			elog(DTM_DEBUG5,
				 "setupMonetdbQEDtxContext inputs (part 2b):  shared local snapshot xid = %u "
				 "(xmin: %u xmax: %u xcnt: %u) curcid: %d, QDxid = %u/%u, QDcid = %u",
				 SharedLocalSnapshotSlot->xid,
				 SharedLocalSnapshotSlot->snapshot.xmin,
				 SharedLocalSnapshotSlot->snapshot.xmax,
	 			 SharedLocalSnapshotSlot->snapshot.xcnt,
				 SharedLocalSnapshotSlot->snapshot.curcid,
				 SharedLocalSnapshotSlot->QDxid, SharedLocalSnapshotSlot->segmateSync,
				 SharedLocalSnapshotSlot->QDcid);
		}
	}*/

	/*switch (Gp_role)
	{
		case GP_ROLE_EXECUTE:
			if (Gp_segment == -1 && !Gp_is_writer)
			{
				isEntryDbSingleton = true;
			}
			else
			{
				//
				 * NOTE: this is a bit hackish. It appears as though
				 * StartTransaction() gets called during connection setup before
				 * we even have time to setup our shared snapshot slot.
				 //
				if (SharedLocalSnapshotSlot == NULL)
				{
					if (explicitBegin || haveDistributedSnapshot)
					{
						//TODO:wangccdl
						elog(ERROR, "setupMonetdbQEDtxContext not expecting distributed begin or snapshot when no Snapshot slot exists");
					}
				}
				else
				{
					if (Gp_is_writer)
					{
						isWriterQE = true;
					}
					else
					{
						isReaderQE = true;
					}
				}
			}
			break;

		default:
			assert(DistributedTransactionContext == DTX_CONTEXT_LOCAL_ONLY);
			//TODO:wangccdl
			//elog(DTM_DEBUG5,
			//	 "setupMonetdbQEDtxContext leaving context = 'Local Only' for Gp_role = %s", role_to_string(Gp_role) );
			return msg;
	}*/
	
	//TODO:wangccdl
	//elog(DTM_DEBUG5,
	//	 "setupMonetdbQEDtxContext intermediate result: isEntryDbSingleton = %s, isWriterQE = %s, isReaderQE = %s.",
	//	 (isEntryDbSingleton ? "true" : "false"),
	//	 (isWriterQE ? "true" : "false"), (isReaderQE ? "true" : "false"));

	/*
	 * Copy to our QE global variable.
	 */
	DtxContextInfo_Copy(&QEDtxContextInfo, &TempDtxContextInfo);
  mlog( LOG_DTM_DEBUG ,JKD_TRANSACTION,0 , "setupMonetdbQEDtxContext id='%s'", QEDtxContextInfo.distributedId );
	switch (DistributedTransactionContext)
	{
		case DTX_CONTEXT_LOCAL_ONLY:
			if (isEntryDbSingleton && haveDistributedSnapshot)
			{
				/*
				 * Later, in GetSnapshotData, we will adopt the QD's
				 * transaction and snapshot information.
				 */

				setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_ENTRY_DB_SINGLETON );
			}
			else if (isReaderQE && haveDistributedSnapshot)
			{
				/*
				 * Later, in GetSnapshotData, we will adopt the QE Writer's
				 * transaction and snapshot information.
				 */

				setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_READER );
			}
			else if (isWriterQE && (explicitBegin || needTwoPhase))
			{
				if (!haveDistributedSnapshot)
				{
					//TODO:wangccdl
					//elog(DTM_DEBUG5,
					//	 "setupMonetdbQEDtxContext Segment Writer is involved in a distributed transaction without a distributed snapshot...");
				}

				//TODO:wangccdl
				//if (IsTransactionOrTransactionBlock())
				//{
					//TODO:wangccdl
					//elog(ERROR, "Starting an explicit distributed transaction in segment -- cannot already be in a transaction");
				//}

				if (explicitBegin)
				{
					/*
					 * We set the DistributedTransactionContext BEFORE we create the
					 * transactions to influence the behavior of
					 * StartTransaction.
					 */
					setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER );

					// we don't need to call begin here
					msg = doQEDistributedExplicitBegin(cntxt, mb, stk, pci, txnOptions);
				}
				else
				{
					assert(needTwoPhase);
					setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER );		
				}
			}
			else if (haveDistributedSnapshot)
			{
				//if (IsTransactionOrTransactionBlock())
				//{
					//TODO:wangccdl
					//elog(ERROR,
					//	 "Going to start a local implicit transaction in segment using a distribute "
					//	 "snapshot -- cannot already be in a transaction");
				//}

				/*
				 * Before executing the query, postgres.c make a standard call to
				 * StartTransactionCommand which will begin a local transaction with
				 * StartTransaction.  This is fine.
				 *
				 * However, when the snapshot is created later, the state below will
				 * tell GetSnapshotData to make the local snapshot from the
				 * distributed snapshot.
				 */
				//setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT );
				/* lixj:
				   explicitBegin == false &&  needTwoPhase == false
				   This is happening only when jsql is auto_commit and the sql is a select(without write),
				   for this condition ,TM willnot send commit/prepare/commit_prepare cmd to RM, so we need to deal it in RM, 
				   otherwise,this transaction will never finished,select sqls in this session can not get new changed results;
				   Our solution is set auto_commit on monetdb ,and auto_commit off when sql_trans_end
				 */
				msg = doQEDistributedSetAutoCommit( cntxt, mb, stk, pci, txnOptions );
			}
			else
			{
				assert (!haveDistributedSnapshot);

				/*
				 * A local implicit transaction without reference to
				 * a distributed snapshot.  Stay in NONE state.
				 */
				assert(DistributedTransactionContext == DTX_CONTEXT_LOCAL_ONLY);
			}
			break;

		case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
/*
		elog(NOTICE, "We should have left this transition state '%s' at the end of the previous command...",
			 DtxContextToString(DistributedTransactionContext));
*/
			//TODO:wangccdl
			//assert (IsTransactionOrTransactionBlock());

			if (explicitBegin)
			{
				//TODO:wangccdl
				//elog(ERROR, "Cannot have an explicit BEGIN statement...");
			}
			break;

		case DTX_CONTEXT_QE_AUTO_COMMIT_IMPLICIT:
			//TODO:wangccdl
			//elog(ERROR, "We should have left this transition state '%s' at the end of the previous command",
			//	 DtxContextToString(DistributedTransactionContext));
			break;

		case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
			//assert (IsTransactionOrTransactionBlock());
			break;

		case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
		case DTX_CONTEXT_QE_READER:
			/*
			 * We are playing games with the xact.c code, so we shouldn't test
			 * with the IsTransactionOrTransactionBlock() routine.
			 */
			break;

		case DTX_CONTEXT_QE_PREPARED:
		case DTX_CONTEXT_QE_FINISH_PREPARED:
			//TODO:wangccdl
			//elog(ERROR, "We should not be trying to execute a query in state '%s'",
			//	 DtxContextToString(DistributedTransactionContext));
			break;

		default:
			//TODO:wangccdl
			//elog(PANIC, "Unexpected segment distribute transaction context value: %d",
			//	 (int) DistributedTransactionContext);
			break;
	}

	//TODO:wangccdl
	//elog(DTM_DEBUG5, "setupMonetdbQEDtxContext final result: DistributedTransactionContext = '%s'.",
	//	 DtxContextToString(DistributedTransactionContext));

	if (haveDistributedSnapshot)
	{
		//TODO:wangccdl
		/*elog((Debug_print_snapshot_dtm ? LOG : DEBUG5), "[Distributed Snapshot #%u] *Set QE* currcid = %d (gxid = %u, '%s')",
			 dtxContextInfo->distributedSnapshot.header.distribSnapshotId,
			 dtxContextInfo->curcid,
			 getMonetdbDistributedTransactionId(),
			 DtxContextToString(DistributedTransactionContext));*/
	}
	return msg;
}

DistributedTransactionId
getMonetdbDistributedTransactionId(void)
{
	if ( isQEContext())
	{
		return QEDtxContextInfo.distributedXid;
	}
	else
	{
		return InvalidDistributedTransactionId;
	}
}

bool
getMonetdbDistributedTransactionIdentifier(char *id)
{
	if ( isQEContext())
	{
		if (QEDtxContextInfo.distributedXid != InvalidDistributedTransactionId)
		{
			//TODO:wangccdl
			//if (strlen(QEDtxContextInfo.distributedId) >= TMGIDSIZE)
			//	elog(PANIC, "Distribute transaction identifier too long (%d)",
			//		 (int)strlen(QEDtxContextInfo.distributedId));
			memcpy(id, QEDtxContextInfo.distributedId, TMGIDSIZE);
			return true;
		}
	}

	memset(id, 0, TMGIDSIZE);
	return false;
}

void
finishMonetdbDistributedTransactionContext (char* debugCaller, bool aborted)
{
	DistributedTransactionId gxid;

	gxid = getMonetdbDistributedTransactionId();
	//TODO:wangccdl
	//elog(DTM_DEBUG5,
	//	 "finishDistributedTransactionContext called to change DistributedTransactionContext from %s to %s (caller = %s, gxid = %u)",
	//	 DtxContextToString(DistributedTransactionContext),
	//	 DtxContextToString(DTX_CONTEXT_LOCAL_ONLY),
	//	 debugCaller,
	//	 gxid);

	setMonetdbDistributedTransactionContext( DTX_CONTEXT_LOCAL_ONLY );
	DtxContextInfo_Reset(&QEDtxContextInfo);

}

/**
 * On the QD, run the Prepare operation.
 */
static str
performMonetdbDtxProtocolPrepare(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid )
{
        mvc *sql = NULL;
        str msg;
        char buf[BUFSIZ];
        int ret = 0;

        if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) {
                return msg;
        }
        if ((msg = checkSQLContext(cntxt)) != NULL) {
                return msg;
        }

	//elog(DTM_DEBUG5, "performMonetdbDtxProtocolCommand going to call PrepareTransactionBlock for distributed transaction (id = '%s')", gid );
  mlog(  LOG_DTM_DEBUG, JKD_TRANSACTION,0 , "going to call mvc_prepare for distributed transaction (id = '%s')",gid );
  char dbfarm[PATHLENGTH];
  char *p;
  char *dbpath=GDKgetenv("gdk_dbpath");
  char clogName[PATHLENGTH];
	p = strrchr(dbpath , DIR_SEP);
	assert(p != NULL);
	strncpy(dbfarm, dbpath, p - dbpath);
	dbfarm[p - dbpath] = 0;
	
	sprintf( clogName, "%s/pm_clog/%s", dbfarm , p+1 );
  
  ret = mvc_prepare( sql, 0, NULL, clogName, gid );
  if (ret < 0) {
  //	finishMonetdbDistributedTransactionContext("performDtxProtocolAbortPrepared -- Commit Prepared (error case)", true);
    mlog(LOG_ERROR_M,JKD_TRANSACTION,0, "mvc_prepare of distributed transaction failed (id = '%s')", gid );
    throw(SQL, "sql.trans", "2D000!PREPARE: failed");
  }
  else
  {
	  mlog(LOG_DTM_DEBUG,JKD_TRANSACTION,0, "Prepare of distributed transaction succeeded (id = '%s')", gid );
  }
	setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_PREPARED );
	return MAL_SUCCEED;
}


/**
 * On the QD, run the Commit Prepared operation.
 */
static str
performMonetdbDtxProtocolCommitPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound)
{
  mvc *sql = NULL;
  str msg;
  char buf[BUFSIZ];
  int ret = 0;

  if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) {
        return msg;
  }
  if ((msg = checkSQLContext(cntxt)) != NULL) {
        return msg;
  }
  mlog(  LOG_DTM_DEBUG,JKD_TRANSACTION, 0 , "going to call mvc_commit_prepare for distributed transaction %s ",gid );

	// Since this call may fail, lets setup a handler.
	if (sql->session->auto_commit == 1) {
		throw(SQL, "sql.trans", "2DM30!COMMIT PREPARE: not allowed in auto commit mode");
	}
	char dbfarm[PATHLENGTH];
	char clogDir[PATHLENGTH];
  char *p;
  char *dbpath=GDKgetenv("gdk_dbpath");
  char clogName[PATHLENGTH];
	p = strrchr(dbpath , DIR_SEP);
	assert(p != NULL);
	strncpy(dbfarm, dbpath, p - dbpath);
	dbfarm[p - dbpath] = 0;
	
	/*if ((p = getFarmPath(dbfarm, sizeof(dbfarm), NULL)) != NULL)
		return(p);*/
	sprintf( clogName, "%s/pm_clog/%s", dbfarm , p+1 );
	
	ret = mvc_commit_prepare(sql, 0, NULL, clogName, gid );
	if (ret < 0) {
		finishMonetdbDistributedTransactionContext("performDtxProtocolCommitPrepared -- Commit Prepared (error case)", false);
  	mlog(LOG_ERROR_M,JKD_TRANSACTION,0, "mvc_commit_prepare of distributed transaction failed (id = '%s')", gid );
 		throw(SQL, "sql.trans", "2D000!COMMIT: failed");
	}
  else
  {
	   finishMonetdbDistributedTransactionContext("performDtxProtocolCommitPrepared -- Commit Prepared", false);
  }
	return MAL_SUCCEED;
}
/**
 * On the QD tempalte1 DB, run the Recovery Commit Prepared operation.
 */
static str
performMonetdbDtxProtocolRecoveryCommitPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound)
{
        mvc *sql = NULL;
        str msg;
        char buf[BUFSIZ];
        int ret = 0;
        char clogDir[PATHLENGTH]={0};

        if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) {
                return msg;
        }
        if ((msg = checkSQLContext(cntxt)) != NULL) {
                return msg;
        }
  
	elog(DTM_DEBUG5,
		 "performMonetdbDtxProtocolRecoveryCommitPrepared going to call FinishPreparedTransaction for distributed transaction %s", gid );
  
  mlog(  LOG_INFO, JKD_TRANSACTION,0 ,"going to call mvc_recovery_commit_prepare for distributed transaction (id = '%s')",gid );
  
	// Since this call may fail, lets setup a handler.
	if (sql->session->auto_commit == 1) {
		throw(SQL, "sql.trans", "2DM30!COMMIT PREPARE: not allowed in auto commit mode");
	}
	
	char dbfarm[PATHLENGTH];
  char *p;
  char *dbpath=GDKgetenv("gdk_dbpath");
	p = strrchr(dbpath , DIR_SEP);
	assert(p != NULL);
	strncpy(dbfarm, dbpath, p - dbpath);
	dbfarm[p - dbpath] = 0;
	
	/*if ((p = getFarmPath(dbfarm, sizeof(dbfarm), NULL)) != NULL)
		return(p);*/
	sprintf( clogDir, "%s/pm_clog", dbfarm );
	
	ret = mvc_recovery_commit_prepare( sql, 0, NULL, clogDir, gid );
	if (ret < 0) {
		finishMonetdbDistributedTransactionContext("performDtxProtocolCommitPrepared -- Recovery Commit Prepared (error case)", false);
		mlog(LOG_ERROR_M,JKD_TRANSACTION,0, "mvc_recovery_commit_prepare of distributed transaction failed (id = '%s')", gid );
		throw(SQL, "sql.trans", "2D000!COMMIT: failed");
	}
  
	finishMonetdbDistributedTransactionContext("performDtxProtocolCommitPrepared -- Recovery Commit Prepared", false);
	return MAL_SUCCEED;
}
/**
 * On the QD, run the Abort Prepared operation.
 */
static str
performMonetdbDtxProtocolAbortPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound)
{
	mvc *sql = NULL;
	str msg;
	char buf[BUFSIZ];
	int ret = 0;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) { 
		return msg;
	}
	if ((msg = checkSQLContext(cntxt)) != NULL) { 
		return msg;
	}
  mlog(  LOG_DTM_DEBUG, JKD_TRANSACTION,0 ,"going to call mvc_abort_prepare for distributed transaction (id = '%s')",gid );

	if (sql->session->auto_commit == 1) {
		finishMonetdbDistributedTransactionContext("performDtxProtocolAbortPrepared -- Abort Prepared (error case)", true);
		throw(SQL, "sql.trans", "2DM30!ABORT PREPARE: not allowed in auto commit mode");
	}
	ret = mvc_abort_prepare(sql, 0, NULL);
	if (ret < 0) {
		finishMonetdbDistributedTransactionContext("performDtxProtocolAbortPrepared -- Abort Prepared (error case)", true);
		throw(SQL, "sql.trans", "2D000!ABORT PREPARE: failed");
	}

	finishMonetdbDistributedTransactionContext("performDtxProtocolAbortPrepared -- Abort Prepared", true);
	return msg;
}
/**
 * On the QD tempalte1 DB, run the Recovery Abort Prepared operation.
 */
static str
performMonetdbDtxProtocolRecoveryAbortPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound)
{
	mvc *sql = NULL;
	str msg;
	char buf[BUFSIZ];
	int ret = 0;
	char clogDir[PATHLENGTH]={0};

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) { 
		return msg;
	}
	if ((msg = checkSQLContext(cntxt)) != NULL) { 
		return msg;
	}
  mlog(  LOG_INFO, JKD_TRANSACTION,0 ,"going to call mvc_recovery_abort_prepare for distributed transaction (id = '%s')",gid );

	if (sql->session->auto_commit == 1) {
		finishMonetdbDistributedTransactionContext("performMonetdbDtxProtocolRecoveryAbortPrepared -- Abort Prepared (error case)", true);
		throw(SQL, "sql.trans", "2DM30!ABORT PREPARE: not allowed in auto commit mode");
	}
	char dbfarm[PATHLENGTH];
  char *p;
  char *dbpath=GDKgetenv("gdk_dbpath");
	p = strrchr(dbpath , DIR_SEP);
	assert(p != NULL); 
	strncpy(dbfarm, dbpath, p - dbpath);
	dbfarm[p - dbpath] = 0;
	
	/*if ((p = getFarmPath(dbfarm, sizeof(dbfarm), NULL)) != NULL)
		return(p);*/
	sprintf( clogDir, "%s/pm_clog", dbfarm );
	ret = mvc_recovery_abort_prepare(sql, 0, NULL, clogDir, gid);
	if (ret < 0) {
		finishMonetdbDistributedTransactionContext("performMonetdbDtxProtocolRecoveryAbortPrepared -- Abort Prepared (error case)", true);
		throw(SQL, "sql.trans", "2D000!ABORT PREPARE: failed");
	}/**/

	finishMonetdbDistributedTransactionContext("performMonetdbDtxProtocolRecoveryAbortPrepared -- Abort Prepared", true);
	return msg;
}

/**
 * On the QD, run the Retry Commit Prepared operation.
 */
static str
performMonetdbDtxProtocolRetryCommitPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound)
{
	 mvc *sql = NULL;
   str msg;
   char buf[BUFSIZ];
   int ret = 0;
   char clogDir[PATHLENGTH]={0};

	 if( IndoubtTransRetryFlag == 0 )
	 {
	 		return performMonetdbDtxProtocolCommitPrepared(cntxt, mb, stk, pci, gid, raiseErrorIfNotFound);     
	 }

   if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) {
           return msg;
   }
   if ((msg = checkSQLContext(cntxt)) != NULL) {
           return msg;
   }

	 elog(DTM_DEBUG5,
		 "performMonetdbDtxProtocolRetryCommitPrepared going to call FinishPreparedTransaction for distributed transaction %s", gid );
   mlog(  LOG_INFO, JKD_TRANSACTION,0 ,"going to call mvc_retry_commit_prepare for distributed transaction (id = '%s')",gid );
 
	// Since this call may fail, lets setup a handler.
	if (sql->session->auto_commit == 1) {
		throw(SQL, "sql.trans", "2DM30!RETRY COMMIT PREPARE: not allowed in auto commit mode");
	}
	
	char dbfarm[PATHLENGTH];
	char clogName[PATHLENGTH];
  char *p;
  char *dbpath=GDKgetenv("gdk_dbpath");
	p = strrchr(dbpath , DIR_SEP);
	assert(p != NULL);
	strncpy(dbfarm, dbpath, p - dbpath);
	dbfarm[p - dbpath] = 0;  

	sprintf( clogName, "%s/pm_clog/%s", dbfarm , p+1 );
	
	//ret = mvc_commit( sql, 0, NULL );//mvc *m, int chain, const char *name
	ret = mvc_retry_commit_prepare( sql, 0, NULL, clogName, gid );
	if (ret < 0) {
		finishMonetdbDistributedTransactionContext("performMonetdbDtxProtocolRetryCommitPrepared -- Retry Commit Prepared (error case)", false);
		throw(SQL, "sql.trans", "2D000!COMMIT: failed");
	}

 // sql->session->tr->schema_updates=0;
 // sql->session->tr->wstime += 5 ;
	finishMonetdbDistributedTransactionContext("performMonetdbDtxProtocolRetryCommitPrepared -- Retry Commit Prepared", false);
	IndoubtTransRetryFlag = 0;
	
	return MAL_SUCCEED;
}
/**
 * On the QD, run the Retry Abort Prepared operation.
 */
static str
performMonetdbDtxProtocolRetryAbortPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound)
{
	sleep(20);
	mvc *sql = NULL;
	str msg;
	char buf[BUFSIZ];
	int ret = 0;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) { 
		return msg;
	}
	if ((msg = checkSQLContext(cntxt)) != NULL) { 
		return msg;
	}
  mlog(  LOG_INFO,JKD_TRANSACTION, 0 ,"going to call mvc_retry_abort_prepare for distributed transaction (id = '%s')",gid );
 

	if (sql->session->auto_commit == 1) {
		finishMonetdbDistributedTransactionContext("performMonetdbDtxProtocolRetryAbortPrepared -- Abort Prepared (error case)", true);
		throw(SQL, "sql.trans", "2DM30!ABORT PREPARE: not allowed in auto commit mode");
	}
	ret = mvc_retry_abort_prepare(sql, 0, NULL);
	if (ret < 0) {
		finishMonetdbDistributedTransactionContext("performMonetdbDtxProtocolRetryAbortPrepared -- Abort Prepared (error case)", true);
		throw(SQL, "sql.trans", "2D000!ABORT PREPARE: failed");
	}/**/

	finishMonetdbDistributedTransactionContext("performMonetdbDtxProtocolRetryAbortPrepared -- Abort Prepared", true);
	return msg;
}



/**
 * On the QE, handle a DtxProtocolCommand
 */
str
performMonetdbDtxProtocolCommand( Client cntxt,
                           MalBlkPtr mb,
                           MalStkPtr stk,
                           InstrPtr pci,
                           const char *gid,
                           DtxProtocolCommand dtxProtocolCommand)
{
        mvc *sql = NULL;
        str msg = MAL_SUCCEED;
        char buf[BUFSIZ];
        bool ret=true;

        if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) {
                return msg;
        }
        if ((msg = checkSQLContext(cntxt)) != NULL) {
                return msg;
        }
  mlog(LOG_DTM_DEBUG, JKD_TRANSACTION,0 , "return for distributed transaction %s, cmd= '%s' ", gid, DtxProtocolCommandToString(dtxProtocolCommand)  );
	switch (dtxProtocolCommand)
	{
		case DTX_PROTOCOL_COMMAND_STAY_AT_OR_BECOME_IMPLIED_WRITER:
			switch(DistributedTransactionContext)
			{
			case DTX_CONTEXT_LOCAL_ONLY:
				// convert to implicit_writer! 
				msg = setupMonetdbQEDtxContext(cntxt, mb, stk, pci);
				break;
			case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
				// already the state we like 
				break;
			default:
				if ( isQEContext() || isQDContext())
				{
					throw(SQL, "sql.trans", "25000!Unexpected segment distributed transaction context: '%s'", 
						DtxContextToString(DistributedTransactionContext));
				}
				else
				{
					throw(SQL, "sql.trans", "25000!Unexpected segment distributed transaction context value: %d",
						(int) DistributedTransactionContext);
				}
				break;
			}
			break;

		case DTX_PROTOCOL_COMMAND_ABORT_NO_PREPARED:
			msg = monetdbAbortOutOfAnyTransaction(cntxt, mb, stk, pci,gid);
			break;

		case DTX_PROTOCOL_COMMAND_PREPARE:
			// The QD has directed us to read-only commit or prepare an implicit or explicit
			// distributed transaction.
			switch (DistributedTransactionContext)
			{
				case DTX_CONTEXT_LOCAL_ONLY:
					// Spontaneously aborted while we were back at the QD?
					//wangccdl
					//throw(SQL, "sql.trans", "25000!Distributed transaction %s not found", gid);
					throw(SQL, "sql.trans", "25000!Distributed transaction xxxx not found");
					break;

				case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
				case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
					msg = performMonetdbDtxProtocolPrepare(cntxt, mb, stk, pci ,gid);
					break;

				case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
				case DTX_CONTEXT_QD_RETRY_PHASE_2:
				case DTX_CONTEXT_QE_PREPARED:
				case DTX_CONTEXT_QE_FINISH_PREPARED:
				case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
				case DTX_CONTEXT_QE_READER:
					throw(SQL, "sql.trans", "25000!Unexpected segment distribute transaction context: '%s'",
						 DtxContextToString(DistributedTransactionContext));

				default:
					throw(SQL, "sql.trans", "25000!Unexpected segment distribute transaction context value: %d",
						 (int) DistributedTransactionContext);
					break;
			}
			break;

		case DTX_PROTOCOL_COMMAND_ABORT_SOME_PREPARED:
			switch (DistributedTransactionContext)
			{
				case DTX_CONTEXT_LOCAL_ONLY:
					// Spontaneously aborted while we were back at the QD?
					//TODO:wangccdl
					//throw(SQL, "sql.trans", "25000!Distributed transaction %s not found"%s
					throw(SQL, "sql.trans", "25000!Distributed transaction xxxx not found");
					break;

				case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
				case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
					msg = monetdbAbortOutOfAnyTransaction(cntxt, mb, stk, pci,gid);
					break;

				case DTX_CONTEXT_QE_PREPARED:
					setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_FINISH_PREPARED );
					msg = performMonetdbDtxProtocolAbortPrepared(cntxt, mb, stk, pci, gid, true);
					break;

				case DTX_CONTEXT_QD_DISTRIBUTED_CAPABLE:
				case DTX_CONTEXT_QD_RETRY_PHASE_2:
				case DTX_CONTEXT_QE_ENTRY_DB_SINGLETON:
				case DTX_CONTEXT_QE_READER:
					throw(SQL, "sql.trans", "25000!Unexpected segment distribute transaction context: '%s'",
						 DtxContextToString(DistributedTransactionContext));

				default:
					throw(SQL, "sql.trans", "25000!Unexpected segment distribute transaction context value: %d",
						 (int) DistributedTransactionContext);
					break;
			}
			break;

		case DTX_PROTOCOL_COMMAND_COMMIT_PREPARED:
			ret=requireMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_PREPARED );
			setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_FINISH_PREPARED );
			msg = performMonetdbDtxProtocolCommitPrepared(cntxt, mb, stk, pci, gid, true);
			break;

		case DTX_PROTOCOL_COMMAND_ABORT_PREPARED:
			ret=requireMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_PREPARED );
			setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_FINISH_PREPARED );
			msg = performMonetdbDtxProtocolAbortPrepared(cntxt, mb, stk, pci, gid, true);
			break;

		case DTX_PROTOCOL_COMMAND_RETRY_COMMIT_PREPARED:
			ret=requireMonetdbDistributedTransactionContext( DTX_CONTEXT_LOCAL_ONLY );
			//TODO: Should uncomment in phase 2
			//msg = performMonetdbDtxProtocolCommitPrepared(cntxt, mb, stk, pci, gid, true);
			msg = performMonetdbDtxProtocolRetryCommitPrepared(cntxt, mb, stk, pci, gid, false);
			break;

		case DTX_PROTOCOL_COMMAND_RETRY_ABORT_PREPARED:
			ret=requireMonetdbDistributedTransactionContext( DTX_CONTEXT_LOCAL_ONLY );
			//TODO: Should uncomment in phase 2
			msg = performMonetdbDtxProtocolRetryAbortPrepared(cntxt, mb, stk, pci, gid, false);
			break;

		case DTX_PROTOCOL_COMMAND_RECOVERY_COMMIT_PREPARED:
			ret=requireMonetdbDistributedTransactionContext( DTX_CONTEXT_LOCAL_ONLY );
			msg = performMonetdbDtxProtocolRecoveryCommitPrepared(cntxt, mb, stk, pci, gid, false);
			break;

		case DTX_PROTOCOL_COMMAND_RECOVERY_ABORT_PREPARED:
			if( requireMonetdbDistributedTransactionContext( DTX_CONTEXT_LOCAL_ONLY ) || requireMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER)	)	
			{
				msg = performMonetdbDtxProtocolRecoveryAbortPrepared(cntxt, mb, stk, pci, gid, false);
				
			}
			else
			{
				mlog( LOG_ERROR_M,JKD_TRANSACTION, 0, "Unexpected segment distributed transaction context: '%s'", 
						DtxContextToString(DistributedTransactionContext));
						
				throw(SQL, "sql.dtm", "Unexpected segment distributed transaction context: '%s'", 
						DtxContextToString(DistributedTransactionContext));
			}	
			break;

		case DTX_PROTOCOL_COMMAND_SUBTRANSACTION_BEGIN_INTERNAL:
			switch(DistributedTransactionContext)
			{
				case DTX_CONTEXT_LOCAL_ONLY:
					// QE is not aware of DTX yet.
					// A typical case is SELECT foo(), where foo() opens internal subtransaction
					msg = setupMonetdbQEDtxContext(cntxt, mb, stk, pci);
					break;
				case DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER:
				// We already marked this QE to be writer, and transaction is open. 
				case DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER:
				case DTX_CONTEXT_QE_READER:
					break;
				default:
					// Lets flag this situation out, with explicit crash
					// assert (DistributedTransactionContext != DistributedTransactionContext);
					//	" SUBTRANSACTION_BEGIN_INTERNAL distributed transaction context invalid: %d",
					//	 (int) DistributedTransactionContext);
					break;
			}

			//TODO:wangccdl
			//BeginInternalSubTransaction(NULL);
			//assert(contextInfo->nestingLevel+1 == GetCurrentTransactionNestLevel());
			break;

		case DTX_PROTOCOL_COMMAND_SUBTRANSACTION_RELEASE_INTERNAL:
			//TODO:wangccdl
			//assert(contextInfo->nestingLevel == GetCurrentTransactionNestLevel());
			//ReleaseCurrentSubTransaction();
			break;
	
		case DTX_PROTOCOL_COMMAND_SUBTRANSACTION_ROLLBACK_INTERNAL:
			//  Rollback performs work on master and then dispatches, 
			// hence has nestingLevel its expecting post operation
			//TODO:wangccdl
			//if ((contextInfo->nestingLevel + 1) > GetCurrentTransactionNestLevel())
			//{
				//TODO:wangccdl
				//throw(SQL, "sql.trans", "25000!transaction %s at level %d already processed (current level %d)",
				//		gid, contextInfo->nestingLevel, GetCurrentTransactionNestLevel())));
			//}
			
			//unsigned int i = GetCurrentTransactionNestLevel() - contextInfo->nestingLevel;
			//while (i > 0)
			//{
			//	RollbackAndReleaseCurrentSubTransaction();
			//	i--;
			//}

			//assert(contextInfo->nestingLevel == GetCurrentTransactionNestLevel());
			break;

		default:
			throw(SQL, "sql.trans", "25000!Unrecognized dtx protocol command: %d",
				 (int) dtxProtocolCommand);
			break;
	}

	mlog(LOG_DTM_DEBUG, JKD_TRANSACTION,0 , "return for distributed transaction %s", gid );
	return msg;
}

str doQEDistributedExplicitBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int txnOptions) {
	bstream *in = cntxt->fdin;
	stream *out = cntxt->fdout;
  mvc *sql = NULL;
	str msg;
	backend *be;
	int oldvtop, oldstop;
	int pstatus = 0;
	int err = 0, opt = 0;
	int skipsqlparser=0;
	sql_rel *r=NULL;

        be = (backend *) cntxt->sqlcontext;
        if (be == 0) {
		/* tell the client */
		mnstr_printf(out, "!SQL state descriptor missing, aborting\n");
		mnstr_flush(out);
		/* leave a message in the log */
		fprintf(stderr, "SQL state descriptor missing, cannot handle client!\n");
		/* stop here, instead of printing the exception below to the
 * 		 * client in an endless loop */
		cntxt->mode = FINISHCLIENT;
		throw(SQL, "doQEDistributedExplicitBegin", "State descriptor missing");
	}
  sql = be->mvc;
  mlog(  LOG_DTM_DEBUG, JKD_TRANSACTION,0 ,"(id = '%s')",CurrLoggerGid );
	/*int type = *getArgReference_int(stk, pci, 1);
	int chain = *getArgReference_int(stk, pci, 2);
	str name = *getArgReference_str(stk, pci, 3);
	char buf[BUFSIZ];
	int ret = 0;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if (name && strcmp(name, str_nil) == 0)
		name = NULL;*/
      //	if (sql->session->auto_commit == 0)
	//if ( sql->session->tr->trans_state != TRANS_END && sql->session->tr->trans_state != TRANS_DEFAULT)
              //throw(SQL, "sql.trans", "25001!START TRANSACTION: cannot start a transaction within a transaction");
       /* if ( sql->session->tr->trans_state == TRANS_START )
              return MAL_SUCCEED;*/
           SQLtrans(sql);
        pstatus = sql->session->status;
        sql->session->auto_commit = 0;
	/*if (sql->session->active) {
		//RECYCLEdrop(cntxt);
		//mvc_rollback(sql, 0, NULL);
	}
	sql->session->auto_commit = 0;
	sql->session->level = chain;
	(void) mvc_trans(sql);*/
     
	return MAL_SUCCEED;
}
/*
 *	doQEDistributedSetAutoCommit
 *
 *	This function is used to set sql->session->auto_commit = 1
 *	
 *	.
 */
str doQEDistributedSetAutoCommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int txnOptions) {
	bstream *in = cntxt->fdin;
	stream *out = cntxt->fdout;
  mvc *sql = NULL;
//	str msg;
	backend *be;
	//int oldvtop, oldstop;
	int pstatus = 0;
	int err = 0, opt = 0;
	//sql_rel *r=NULL;

        be = (backend *) cntxt->sqlcontext;
        if (be == 0) {
		/* tell the client */
		mnstr_printf(out, "!SQL state descriptor missing, aborting\n");
		mnstr_flush(out);
		/* leave a message in the log */
		fprintf(stderr, "SQL state descriptor missing, cannot handle client!\n");
		/* stop here, instead of printing the exception below to the
 * 		 * client in an endless loop */
		cntxt->mode = FINISHCLIENT;
		throw(SQL, "doQEDistributedSetAutoCommit", "State descriptor missing");
	}
  sql = be->mvc;
  mlog(  LOG_DTM_DEBUG, JKD_TRANSACTION,0 ,"SetAutoCommit  (id = '%s')",CurrLoggerGid );
       
           SQLtrans(sql);
        pstatus = sql->session->status;
        sql->session->auto_commit = 1;
	return MAL_SUCCEED;
}
/*
 *	monetdbAbortOutOfAnyTransaction
 *
 *	This routine is provided for error recovery purposes.  It aborts any
 *	active transaction or transaction block, leaving the system in a known
 *	idle state.
 */
str
monetdbAbortOutOfAnyTransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid)
{
 	mvc *sql = NULL;
	str msg;
	char buf[BUFSIZ];
	int ret = 0;

	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL) { 
		return msg;
	}
	if ((msg = checkSQLContext(cntxt)) != NULL) { 
		return msg;
	}
  mlog(  LOG_DTM_DEBUG, JKD_TRANSACTION,0 ,"going to call mvc_abort_prepare for distributed transaction (id = '%s')",gid );

	if (sql->session->auto_commit == 1) {
		finishMonetdbDistributedTransactionContext("monetdbAbortOutOfAnyTransaction -- Abort Prepared (error case)", true);
		throw(SQL, "sql.trans", "2DM30!ABORT PREPARE: not allowed in auto commit mode");
	}
	//ret = mvc_abort_prepare(sql, 0, NULL);
	ret = mvc_rollback(sql, 0, NULL);
	if (ret < 0) {
		finishMonetdbDistributedTransactionContext("monetdbAbortOutOfAnyTransaction -- Abort Prepared (error case)", true);
		throw(SQL, "sql.trans", "2D000!ABORT PREPARE: failed");
	}

	finishMonetdbDistributedTransactionContext("monetdbAbortOutOfAnyTransaction -- Abort Prepared", true);
	return msg;
}

/**
 * On the QD, run the Savepoint operation.
 */
str
doQEDistributedSavepoint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid,char *name )
{
	mvc *sql = NULL;
	str msg;
	int type = *getArgReference_int(stk, pci, 1);
	int chain = *getArgReference_int(stk, pci, 2);
	//str name = *getArgReference_str(stk, pci, 3);
	char buf[BUFSIZ];
	int ret = 0;
  
	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ( name == NULL || strcmp(name, str_nil) == 0)
	{	
	   mlog(LOG_ERROR_M,JKD_TRANSACTION,0 , "Savepoint (name = %s) is null distributed transaction (id = '%s')",name, gid );
	   return MAL_SUCCEED;
  }
    mlog(LOG_DTM_DEBUG,JKD_TRANSACTION,0, "going to create savepoint (name = %s)  for distributed transaction (id = '%s')",name, gid );
		if (sql->session->auto_commit == 1) {
			if (name)
				throw(SQL, "sql.trans", "3BM30!SAVEPOINT: not allowed in auto commit mode");
			else
				throw(SQL, "sql.trans", "2DM30!COMMIT: not allowed in auto commit mode");
		}
		ret = mvc_commit(sql, chain, name);
		if (ret < 0 && !name)
		{
			 finishMonetdbDistributedTransactionContext("performDtxProtocolAbortPrepared -- Commit Prepared (error case)", true);
			 throw(SQL, "sql.trans", "2D000!COMMIT: failed");
		}
		if (ret < 0 && name)
		{
			  finishMonetdbDistributedTransactionContext("performDtxProtocolAbortPrepared -- Commit Prepared (error case)", true);
			 throw(SQL, "sql.trans", "3B000!SAVEPOINT: (%s) failed", name);
		}

	mlog(LOG_DTM_DEBUG ,JKD_TRANSACTION,0 , "Savepoint of distributed transaction succeeded (id = '%s')", gid );

	//setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_PREPARED );
	return MAL_SUCCEED;
}

/**
 * On the QD, run ROLLBACK TO Savepoint operation.
 */ 
str
doQEDistributedRollback(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid,char *name )
{
	mvc *sql = NULL;
	str msg;
	int type = *getArgReference_int(stk, pci, 1);
	int chain = *getArgReference_int(stk, pci, 2);
	//str name = *getArgReference_str(stk, pci, 3);
	char buf[BUFSIZ];
	int ret = 0;
  
	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ( name == NULL || strcmp(name, str_nil) == 0)
	{	
	   mlog(LOG_ERROR_M, JKD_TRANSACTION, 0, "going to rollback to savepoint ( name = %s ) for distributed transaction (id = '%s')  error !!!",name, gid );
	   return MAL_SUCCEED;
  }
   mlog(LOG_DTM_DEBUG,JKD_TRANSACTION, 0,"going to rollback to savepoint (%s)  for distributed transaction (id = '%s')",name, gid );
		if (sql->session->auto_commit == 1) {
			if (name)
				throw(SQL, "sql.trans", "3BM30!SAVEPOINT: not allowed in auto commit mode");
			else
				throw(SQL, "sql.trans", "2DM30!COMMIT: not allowed in auto commit mode");
		}
		ret = mvc_rollback(sql, chain, name);
		if (ret < 0 && !name)
		{
			 finishMonetdbDistributedTransactionContext("doQEDistributedERollback -- Commit Prepared (error case)", true);
			 throw(SQL, "sql.trans", "2D000!COMMIT: failed");
		}
		if (ret < 0 && name)
		{
			  finishMonetdbDistributedTransactionContext("doQEDistributedERollback -- Commit Prepared (error case)", true);
			 throw(SQL, "sql.trans", "3B000!SAVEPOINT: (%s) failed", name);
		}

	elog(DTM_DEBUG5, "Rollback of distributed transaction succeeded (id = '%s')", gid );

	//setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_PREPARED );
	return MAL_SUCCEED;
}

/**
 * On the QD, run RELEASE Savepoint operation.
 */ 
str
doQEDistributedRelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid,char *name )
{
	mvc *sql = NULL;
	str msg;
	int type = *getArgReference_int(stk, pci, 1);
	int chain = *getArgReference_int(stk, pci, 2);
	//str name = *getArgReference_str(stk, pci, 3);
	char buf[BUFSIZ];
	int ret = 0;
  
	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;
	if ( name == NULL || strcmp(name, str_nil) == 0)
	{	
	   mlog(LOG_DTM_DEBUG,JKD_TRANSACTION,0, "Release savepoint name is NULL for distributed transaction (id = '%s')", gid );
	  // return msg;
  }
  mlog(LOG_DTM_DEBUG,JKD_TRANSACTION,0, "going to release savepoint ( name = %s) for distributed transaction (id = '%s')", name, gid );
  
		if (sql->session->auto_commit == 1) {
				throw(SQL, "sql.trans", "3BM30!RELEASE SAVEPOINT: not allowed in auto commit mode");
		}
		ret = mvc_release(sql, name);
		if (ret < 0) {
			finishMonetdbDistributedTransactionContext("doQEDistributedRelease -- release savepoint (error case)", true);
			snprintf(buf, BUFSIZ, "3B000!RELEASE SAVEPOINT: (%s) failed", name);
			throw(SQL, "sql.trans", "%s", buf);
		}
		if (ret < 0 && name)
		{
			  finishMonetdbDistributedTransactionContext("doQEDistributedRelease -- release savepoint (error case)", true);
			 throw(SQL, "sql.trans", "3B000!SAVEPOINT: (%s) failed", name);
		}

	mlog( LOG_DTM_DEBUG,JKD_TRANSACTION, 0 , "Rollback of distributed transaction succeeded (id = '%s')", gid );

	//setMonetdbDistributedTransactionContext( DTX_CONTEXT_QE_PREPARED );
	return MAL_SUCCEED;
}
/**
 * On the QD, run checkpoint operation.
 * excute checkpoint cmd  ,set need_flush=1
 */ 
str
doQEDistributedCheckpoint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid,char *name )
{
	mlog(LOG_DTM_DEBUG,JKD_TRANSACTION, 0,"going to do checkpoint  ,distributed transaction (id = '%s')", gid );

	mvc *sql = NULL;
	str msg;
	int type = *getArgReference_int(stk, pci, 1);
	int chain = *getArgReference_int(stk, pci, 2);
	//str name = *getArgReference_str(stk, pci, 3);
	char buf[BUFSIZ];
	int ret = 0;
  
	if ((msg = getSQLContext(cntxt, mb, &sql, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

  store_apply_deltas();
	return MAL_SUCCEED;
}
