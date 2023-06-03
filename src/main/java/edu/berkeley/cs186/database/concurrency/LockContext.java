package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        //If a LockContext is readonly, acquire/release/promote/escalate should
        // throw an UnsupportedOperationException.
        if (this.readonly) {
            throw new UnsupportedOperationException("trying to acquire on a readonly context");
        }

        if (this.lockman.getLockType(transaction, this.getResourceName()) == lockType){
            throw new DuplicateLockRequestException("all ready has a lock of the same type");
        }

        //check what lock held on parent// if parent null is NL lock
        if(this.parent != null){
            //check if valid child for parent lock.
            if(!LockType.canBeParentLock(this.lockman.getLockType(transaction, parent.getResourceName()), lockType)){
                throw new InvalidLockException("making invalid parent child lock");
            }
        }

        //acquire the lock
        lockman.acquire(transaction, name, lockType);
        //update internal structures
        if (this.parent != null) {
            //increase number of children with teh newly acquired lock.
            this.parent.numChildLocks.put(transaction.getTransNum(),
                    parent.getNumChildren(transaction) + 1);
        }

        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        //If a LockContext is readonly, acquire/release/promote/escalate should
        // throw an UnsupportedOperationException.
        if (this.readonly) {
            throw new UnsupportedOperationException("trying to acquire on a readonly context");
        }

        //check if request is invalid by having children.
        if (this.getNumChildren(transaction) != 0) {
            throw  new InvalidLockException("resource has child locks and cant be released");
        }

        //release the lock aditionally checks if no lock is held by the resource.
        lockman.release(transaction, name);
        //update internal structures
        if (this.parent != null) {
            //decreaase number of children for the given transaction
            this.parent.numChildLocks.put(transaction.getTransNum(),
                    parent.getNumChildren(transaction) - 1 );
        }
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        //If a LockContext is readonly, acquire/release/promote/escalate should
        // throw an UnsupportedOperationException.
        if (this.readonly) {
            throw new UnsupportedOperationException("trying to acquire on a readonly context");
        }
        //get current type on the resource
        LockType currType = lockman.getLockType(transaction, this.name);


//        //check if we can substiture
//        if(!LockType.substitutable(newLockType, currType)){
//            throw new InvalidLockException("trying to substitute an bad lock");
//        }

        //You should also disallow promotion to a SIX lock if an ancestor has SIX, because this would be redundant.
        if(hasSIXAncestor(transaction) && newLockType == LockType.SIX){
            throw new InvalidLockException("promoting to a redundant SIX lock");
        }

        //check that can be a child lock
        if(parent != null) {
            //get parent lock type
            LockType parentType = lockman.getLockType(transaction, parentContext().getResourceName());
            //see if compatible with new lock context
            if (!LockType.canBeParentLock(parentType, newLockType)) {
                throw new InvalidLockException("lock promotion violates parent lock");
            }
        }

        // For promotion to SIX from
        //     * IS/IX, all S and IS locks on descendants must be simultaneously
        //     * released.

        //list of locks to remove
        List<ResourceName> removalList;
        if(newLockType == LockType.SIX) {
            //get all s and IS descendants
             removalList = sisDescendants(transaction);
             //check if we also need to remove the current lock.
            if(currType != LockType.NL){
                removalList.add(this.name);
            }
            //acquire the new lock and release all others atomically.
            lockman.acquireAndRelease(transaction, this.name, newLockType, removalList);
        } else{
            lockman.promote(transaction, this.name, newLockType);
        }

        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        //If a LockContext is readonly, acquire/release/promote/escalate should
        // throw an UnsupportedOperationException.
        if (this.readonly) {
            throw new UnsupportedOperationException("trying to acquire on a readonly context");
        }
        //check simple escilation
        if (numChildLocks.getOrDefault(transaction.getTransNum(), 0) == 0 && (getExplicitLockType(transaction) == LockType.X)|| getExplicitLockType(transaction) == LockType.S) {
            return;
        }
        LockType escalateType = LockType.S;
        //calculate new lock type which we will escilate
        List<Lock> transLocks = this.lockman.getLocks(transaction);
            //iterate over the locks in teh transaction
            for(Lock currLock : transLocks){
                //check if the current lock is on a descendant of the resource we want to escilate lock on
                if(currLock.name.isDescendantOf(name) || currLock.name == name){
                    //check if child or current holds an exclusive lock
                    if(currLock.lockType == LockType.IX || currLock.lockType == LockType.X){
                        escalateType = LockType.X;
                    }
                }
            }


        //check if the current lock on the resource is the needed escalated type
        if(lockman.getLockType(transaction, this.name) == escalateType){
            return;
        }


        //generate resources which needs to be released
        List<ResourceName> removalList = new ArrayList<>();
        for(Lock currLock : transLocks){
            //if a descendant must be removed
            if(currLock.name.isDescendantOf(this.name)){
                removalList.add(currLock.name);
            }
        }
        //must add own name for release aquire
        removalList.add(this.name);


        lockman.acquireAndRelease(transaction, this.name, escalateType, removalList);
        //clear the numchild locks for each resource in the removalList
        for (ResourceName currResource : removalList) {
            if( currResource !=  this.name) {
                if ( currResource.parent() != null){
                    LockContext currContext = fromResourceName(lockman, currResource.parent());
                    currContext.numChildLocks.put(transaction.getTransNum(),
                            currContext.getNumChildren(transaction) -1);
                }
            }
        }

        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        //get lock type of the transaction
        LockType lockType = lockman.getLockType(transaction, this.name);
        //case SIX lock have implicit S lock
        if(lockType == LockType.SIX){
            return LockType.S;
        }
        //if is intent if is intent no explicit lock held
        if (lockType.isIntent()){
            return LockType.NL;
        }
        //otherwise lock held and return it.
        return lockType;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        //get explicit value of the transaction, will be baseline lock
        LockType currEffectiveLock = getExplicitLockType(transaction);
        //check if a parent resource exists
        if(parent!=null){
            //check that our current lock is not explicit
            if(currEffectiveLock == LockType.NL){
                //recursivly get the explicit type of the parent
                currEffectiveLock = parent.getEffectiveLockType(transaction);
                //check if result is intent
                if(currEffectiveLock.isIntent()){
                    return LockType.NL;
                }
            }
        }
        return currEffectiveLock;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        //get all locks of this transaction
        for(Lock currLock : this.lockman.getLocks(transaction)){
            //check if SIX lock
            if(currLock.lockType == LockType.SIX){
                //check if ancestor
                if(this.name.isDescendantOf(currLock.name)){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        //create new list
        List<ResourceName> returnList = new ArrayList<>();
        //get all locks of this transaction
        for(Lock currLock : this.lockman.getLocks(transaction)){
            //check if currlock is sis
            if(currLock.lockType== LockType.S || currLock.lockType == LockType.IS) {
                //if lock is decendent of current context
                if (currLock.name.isDescendantOf(this.name)){
                    //add to list.
                    returnList.add(currLock.name);
                }
            }
        }
        return returnList;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

