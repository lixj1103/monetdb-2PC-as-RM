/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef SQL_DTM_H
#define SQL_DTM_H

#include "jkd.h"
#include "monetdb_config.h"
#include "cdb/cdbdtxcontextinfo.h"
#include "sql.h"
#include "trace_log.h"

#define DTM_SUCCEED 0
#define DTM_FAIL -1 
/* sql_dtm implementation 
 * used to handle distributed transaction commands
 */

static str performMonetdbDtxProtocolCommitPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound);
static str performMonetdbDtxProtocolAbortPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound);

extern DistributedTransactionId getMonetdbDistributedTransactionId(void);
extern bool getMonetdbDistributedTransactionIdentifier(char *id);
extern void finishMonetdbDistributedTransactionContext (char *debugCaller, bool aborted);
extern str setupMonetdbQEDtxContext (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str monetdbAbortOutOfAnyTransaction(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid);

extern str doQEDistributedExplicitBegin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int txnOptions);
extern str doQEDistributedSetAutoCommit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, int txnOptions);
extern str doQEDistributedSavepoint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid,char * name );
extern str doQEDistributedRollback(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid,char *name );
extern str doQEDistributedRelease(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid,char *name );
extern str performMonetdbDtxProtocolCommand(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, DtxProtocolCommand dtxProtocolCommand);
extern str doQEDistributedCheckpoint(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, const char *gid,char *name );

static str performMonetdbDtxProtocolRecoveryCommitPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound);
static str performMonetdbDtxProtocolRecoveryAbortPrepared(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,const char *gid, bool raiseErrorIfNotFound);

#endif /* SQL_DTM_H */

