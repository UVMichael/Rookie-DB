package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        LockType currentLockType = lockContext.lockman.getLockType(transaction, lockContext.name);

        //case The current lock type can effectively substitute the requested type
        if (LockType.substitutable(effectiveLockType, requestType)) {
            //nothing needs to be done
            return;
        }

        //case The current lock type is IX and the requested lock is S
        if (currentLockType == LockType.IX && requestType == LockType.S) {
            //recusivly go through parents and ensure compatible with SIX
            checkParents(parentContext, LockType.SIX);
            //update current lock to SIX; there for granting S lock request and IX privledge
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        //case The current lock type is an intent lock
        if (currentLockType.isIntent()) {
            //check that the request is valid with all parent locks
            checkParents(parentContext, requestType);
            //escilate transaction
            lockContext.escalate(transaction);
            return;
        }

        //case None of the above: In this case, consider what values the explicit
        //     *   lock type can be, and think about how ancestor looks will need to be
        //     *   acquired or changed.
        //check that the request is valid with all parents locks
        checkParents(parentContext, requestType);
        if (currentLockType == LockType.NL) {
            //if NL Lock can simply acquire since no issues
            lockContext.acquire(transaction, requestType);
        } else {
            //else promote to greater type
            lockContext.promote(transaction, requestType);
        }

        return;
    }

    // TODO(proj4_part2) add any helper methods you want

    /**
    ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void checkParents(LockContext lockContext, LockType requestType) {
        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();

        LockType currentLockType = lockContext.lockman.getLockType(transaction, lockContext.name);
        LockType requestParent = LockType.parentLock(requestType);

        //check if conflict with current lock and request
        if (!LockType.canBeParentLock(currentLockType, requestType)) {
            //case we have read but want read write
            if (currentLockType == LockType.S && requestParent == LockType.IX) {
                //to maintian read on current lock parent of request must be SIX LOCk
                requestParent = LockType.SIX;
            }
            //recusivly update parents
            checkParents(parentContext, requestParent);
            //if no lock held on the current resource can simply acquire as there will be no conflicts
            if (currentLockType == LockType.NL) {
                lockContext.acquire(transaction, requestParent);
                return;
            }
            //otherwise we just promote lock up.
            lockContext.promote(transaction, requestParent);
            return;
        }
        return;
    }
}
